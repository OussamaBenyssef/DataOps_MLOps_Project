"""
Module de Nettoyage des Données ETL (Data Cleaning)
=====================================================
Transformations de nettoyage des données crypto brutes (OHLCV) avant
le calcul des indicateurs techniques.

Pipeline: normalize → nulls → validation → dedup → outliers → spikes

Usage:
    df_clean, metrics = clean_ohlcv_data(df_raw, CleaningConfig())
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType

try:
    from .config import CleaningConfig
except ImportError:
    from config import CleaningConfig

logger = logging.getLogger(__name__)


# Schéma des données OHLCV brutes reçues depuis Kafka
RAW_OHLCV_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), True),
        StructField("interval", StringType(), True),
        StructField("timestamp", LongType(), True),  # epoch ms
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", DoubleType(), True),
        StructField("trades", IntegerType(), True),
    ]
)


# ─────────────────────── NULL HANDLING ─────────────────────────────────────


def handle_null_values(df: DataFrame, config: CleaningConfig):
    """
    Supprime les lignes avec des valeurs nulles dans les colonnes critiques.

    Args:
        df: DataFrame Spark d'entrée
        config: Configuration de nettoyage

    Returns:
        Tuple (DataFrame nettoyé, nombre de lignes supprimées)
    """
    initial_count = df.count()

    # Construire le filtre: toutes les colonnes critiques doivent être non-null
    filter_condition = None
    for col_name in config.required_columns:
        if col_name in df.columns:
            cond = F.col(col_name).isNotNull()
            filter_condition = cond if filter_condition is None else (filter_condition & cond)

    if filter_condition is not None:
        df_clean = df.filter(filter_condition)
    else:
        df_clean = df

    dropped = initial_count - df_clean.count()
    if dropped > 0:
        logger.warning(f"⚠️ {dropped} lignes supprimées (valeurs nulles)")
    return df_clean, dropped


# ─────────────────────── VALIDATION MÉTIER ─────────────────────────────────


def validate_ohlcv_constraints(df: DataFrame, config: CleaningConfig):
    """
    Valide les contraintes métier des données OHLCV:
        - Prix > 0
        - High >= Low
        - High >= Open et High >= Close
        - Low <= Open et Low <= Close
        - Volume >= 0

    Returns:
        Tuple (DataFrame valide, DataFrame des rejets avec raison)
    """
    valid_condition = (
        (F.col("open") > 0)
        & (F.col("high") > 0)
        & (F.col("low") > 0)
        & (F.col("close") > 0)
        & (F.col("high") >= F.col("low"))
        & (F.col("high") >= F.col("open"))
        & (F.col("high") >= F.col("close"))
        & (F.col("low") <= F.col("open"))
        & (F.col("low") <= F.col("close"))
        & (F.col("volume") >= 0)
    )

    df_valid = df.filter(valid_condition)
    df_rejected = df.filter(~valid_condition)

    rejected_count = df_rejected.count()
    if rejected_count > 0:
        logger.warning(f"⚠️ {rejected_count} lignes OHLCV rejetées (contraintes invalides)")

    return df_valid, df_rejected


def validate_trades_constraints(df: DataFrame, config: CleaningConfig):
    """
    Valide les contraintes métier des données de trades:
        - Prix > 0
        - Quantité > 0

    Returns:
        Tuple (DataFrame valide, DataFrame des rejets)
    """
    valid_condition = (F.col("price") > 0) & (F.col("quantity") > 0)

    df_valid = df.filter(valid_condition)
    df_rejected = df.filter(~valid_condition)

    return df_valid, df_rejected


# ─────────────────────── OUTLIER DETECTION ─────────────────────────────────


def detect_outliers_zscore(df: DataFrame, config: CleaningConfig):
    """
    Détecte les outliers sur le prix de clôture via Z-score.
    Ajoute une colonne `is_outlier` (boolean) et `zscore_close`.

    Z-score = (valeur - moyenne) / écart-type
    Outlier si |Z-score| > zscore_threshold (default: 3.0)
    """
    # Calculer moyenne et écart-type globaux
    stats = df.agg(F.avg("close").alias("mean_close"), F.stddev("close").alias("std_close")).first()

    mean_close = stats["mean_close"]
    std_close = stats["std_close"]

    if std_close is None or std_close == 0:
        logger.warning("⚠️ Écart-type nul — impossible de détecter les outliers")
        return df.withColumn("zscore_close", F.lit(0.0)).withColumn("is_outlier", F.lit(False))

    df_flagged = df.withColumn(
        "zscore_close", F.round(F.abs((F.col("close") - F.lit(mean_close)) / F.lit(std_close)), 4)
    ).withColumn("is_outlier", F.col("zscore_close") > F.lit(config.zscore_threshold))

    outlier_count = df_flagged.filter(F.col("is_outlier")).count()
    if outlier_count > 0:
        logger.warning(f"⚠️ {outlier_count} outliers détectés (Z-score > {config.zscore_threshold})")

    return df_flagged


def detect_price_spikes(df: DataFrame, config: CleaningConfig):
    """
    Détecte les price spikes: variation anormale entre deux bougies consécutives.

    spike = |close_t - close_{t-1}| / close_{t-1} > spike_threshold
    """
    w = Window.partitionBy("symbol", "interval").orderBy("timestamp")

    df_spikes = (
        df.withColumn("prev_close", F.lag("close", 1).over(w))
        .withColumn(
            "price_change_pct",
            F.when(
                F.col("prev_close").isNotNull() & (F.col("prev_close") > 0),
                F.round(F.abs(F.col("close") - F.col("prev_close")) / F.col("prev_close"), 6),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn("is_price_spike", F.col("price_change_pct") > F.lit(config.price_spike_threshold))
        .drop("prev_close")
    )

    return df_spikes


# ─────────────────────── DEDUPLICATION ─────────────────────────────────────


def deduplicate_ohlcv(df: DataFrame):
    """
    Supprime les doublons OHLCV basés sur (symbol, interval, timestamp).
    En cas de doublon, garde la dernière version reçue.
    """
    initial_count = df.count()

    # Window pour prendre la dernière version
    w = Window.partitionBy("symbol", "interval", "timestamp").orderBy(F.desc("timestamp"))

    df_dedup = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

    removed = initial_count - df_dedup.count()
    if removed > 0:
        logger.info(f"🗑️ {removed} doublons supprimés")

    return df_dedup


# ─────────────────────── NORMALISATION ─────────────────────────────────────


def normalize_timestamp(df: DataFrame):
    """
    Convertit le timestamp epoch (ms) en colonne TimestampType standard.
    Si le timestamp est déjà un TimestampType, on le copie directement.
    """
    from pyspark.sql.types import LongType as _LongType, IntegerType as _IntegerType

    ts_type = df.schema["timestamp"].dataType
    if isinstance(ts_type, (_LongType, _IntegerType)):
        # Epoch ms → diviser par 1000 puis cast en timestamp
        return df.withColumn("event_time", (F.col("timestamp") / 1000).cast("timestamp"))
    else:
        # Déjà un TimestampType — utiliser tel quel
        return df.withColumn("event_time", F.col("timestamp"))


def normalize_symbol(df: DataFrame):
    """
    Normalise le nom du symbole: MAJUSCULES + suppression espaces.
    """
    return df.withColumn("symbol", F.upper(F.trim(F.col("symbol"))))


# ─────────────────────── PIPELINE COMPLET ──────────────────────────────────


def clean_ohlcv_data(df: DataFrame, config: CleaningConfig):
    """
    Pipeline complet de nettoyage des données OHLCV:
        1. Normalisation (symbol, timestamp)
        2. Suppression des nulls
        3. Validation des contraintes métier
        4. Déduplication
        5. Détection outliers + spikes

    Returns:
        Tuple (DataFrame nettoyé, dict avec métriques de qualité)
    """
    initial_count = df.count()
    logger.info(f"🚀 Nettoyage OHLCV — {initial_count} lignes en entrée")

    metrics = {
        "initial_count": initial_count,
        "null_dropped": 0,
        "invalid_ohlcv": 0,
        "duplicates_removed": 0,
        "outliers_detected": 0,
        "final_count": 0,
        "quality_rate": 0.0,
    }

    # 1. Normalisation
    df_clean = normalize_symbol(df)
    df_clean = normalize_timestamp(df_clean)

    # 2. Suppression des nulls
    df_clean, null_dropped = handle_null_values(df_clean, config)
    metrics["null_dropped"] = null_dropped

    # 3. Validation OHLCV
    df_clean, df_rejected = validate_ohlcv_constraints(df_clean, config)
    metrics["invalid_ohlcv"] = df_rejected.count()

    # 4. Déduplication
    count_before_dedup = df_clean.count()
    df_clean = deduplicate_ohlcv(df_clean)
    metrics["duplicates_removed"] = count_before_dedup - df_clean.count()

    # 5. Détection outliers
    df_clean = detect_outliers_zscore(df_clean, config)
    metrics["outliers_detected"] = df_clean.filter(F.col("is_outlier")).count()

    # Métriques finales
    metrics["final_count"] = df_clean.count()
    if initial_count > 0:
        metrics["quality_rate"] = round((metrics["final_count"] / initial_count) * 100, 2)

    logger.info(
        f"✅ Nettoyage terminé — {metrics['final_count']}/{initial_count} lignes "
        f"({metrics['quality_rate']}% qualité)"
    )

    return df_clean, metrics

"""
Module de Nettoyage des Données ETL (Data Cleaning)
=======================================================
Ce module implémente les transformations de nettoyage des données
crypto brutes (OHLCV) avant le calcul des indicateurs techniques.

Fonctionnalités:
    - Suppression/remplacement des valeurs nulles
    - Validation des contraintes métier (prix > 0, high >= low, etc.)
    - Détection et traitement des outliers via Z-score
    - Détection des price spikes (variation anormale)
    - Déduplication des données
    - Normalisation des types (timestamp, symbol)
"""
import logging
from typing import Tuple, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    LongType, TimestampType, BooleanType
)
from pyspark.sql.window import Window

from config import CleaningConfig

logger = logging.getLogger(__name__)

# ─────────────────────── SCHEMAS ───────────────────────────────────────────

# Schéma des données OHLCV brutes reçues depuis Kafka
RAW_OHLCV_SCHEMA = StructType([
    StructField("symbol",    StringType(),    True),
    StructField("interval",  StringType(),    True),
    StructField("timestamp", LongType(),      True),   # epoch ms
    StructField("open",      DoubleType(),    True),
    StructField("high",      DoubleType(),    True),
    StructField("low",       DoubleType(),    True),
    StructField("close",     DoubleType(),    True),
    StructField("volume",    DoubleType(),    True),
    StructField("trades",    LongType(),      True),
])

# Schéma des données de trades bruts
RAW_TRADES_SCHEMA = StructType([
    StructField("symbol",    StringType(),  True),
    StructField("timestamp", LongType(),    True),   # epoch ms
    StructField("price",     DoubleType(),  True),
    StructField("quantity",  DoubleType(),  True),
    StructField("is_buyer",  BooleanType(), True),
    StructField("trade_id",  LongType(),    True),
])


# ─────────────────────── NULL HANDLING ─────────────────────────────────────

def handle_null_values(df: DataFrame, config: CleaningConfig) -> Tuple[DataFrame, int]:
    """
    Supprime les lignes avec des valeurs nulles dans les colonnes critiques.

    Args:
        df: DataFrame Spark d'entrée
        config: Configuration de nettoyage

    Returns:
        Tuple (DataFrame nettoyé, nombre de lignes supprimées)
    """
    initial_count = df.count()

    # Identifier les colonnes critiques présentes dans le DataFrame
    critical_cols = [
        col for col in config.required_ohlcv_columns
        if col in df.columns
    ]

    # Supprimer les lignes avec des nulls dans les colonnes critiques
    df_cleaned = df.dropna(subset=critical_cols)

    # Pour les colonnes non-critiques, remplacer par des valeurs par défaut
    if "trades" in df.columns:
        df_cleaned = df_cleaned.fillna({"trades": 0})
    if "interval" in df.columns:
        df_cleaned = df_cleaned.fillna({"interval": "1m"})
    if "is_buyer" in df.columns:
        df_cleaned = df_cleaned.fillna({"is_buyer": False})

    dropped = initial_count - df_cleaned.count()
    if dropped > 0:
        logger.warning(f"[Nettoyage] {dropped} lignes supprimées (valeurs nulles)")

    return df_cleaned, dropped


# ─────────────────────── VALIDATION MÉTIER ─────────────────────────────────

def validate_ohlcv_constraints(df: DataFrame, config: CleaningConfig) -> Tuple[DataFrame, DataFrame]:
    """
    Valide les contraintes métier des données OHLCV:
        - Prix > 0
        - High >= Low
        - High >= Open et High >= Close
        - Low <= Open et Low <= Close
        - Volume >= 0

    Args:
        df: DataFrame OHLCV
        config: Configuration de nettoyage

    Returns:
        Tuple (DataFrame valide, DataFrame des rejets avec raison)
    """
    # Conditions de validité
    valid_condition = (
        (F.col("open")  > config.min_price) &
        (F.col("high")  > config.min_price) &
        (F.col("low")   > config.min_price) &
        (F.col("close") > config.min_price) &
        (F.col("volume") >= config.min_volume) &
        (F.col("open")  <= config.max_price) &
        (F.col("high")  <= config.max_price) &
        (F.col("low")   <= config.max_price) &
        (F.col("close") <= config.max_price) &
        # Relations OHLC
        (F.col("high") >= F.col("low")) &
        (F.col("high") >= F.col("open")) &
        (F.col("high") >= F.col("close")) &
        (F.col("low")  <= F.col("open")) &
        (F.col("low")  <= F.col("close"))
    )

    # Raison du rejet
    rejection_reason = F.when(
        F.col("open") <= config.min_price, F.lit("invalid_open_price")
    ).when(
        F.col("high") < F.col("low"), F.lit("high_less_than_low")
    ).when(
        F.col("high") < F.col("open"), F.lit("high_less_than_open")
    ).when(
        F.col("high") < F.col("close"), F.lit("high_less_than_close")
    ).when(
        F.col("low") > F.col("open"), F.lit("low_greater_than_open")
    ).when(
        F.col("volume") < config.min_volume, F.lit("invalid_volume")
    ).otherwise(F.lit("unknown"))

    df_valid = df.filter(valid_condition)
    df_rejected = (
        df.filter(~valid_condition)
          .withColumn("rejection_reason", rejection_reason)
    )

    rejected_count = df_rejected.count()
    if rejected_count > 0:
        logger.warning(f"[Validation] {rejected_count} lignes rejetées (contraintes OHLCV)")
        df_rejected.select("symbol", "timestamp", "rejection_reason").show(5, truncate=False)

    return df_valid, df_rejected


def validate_trades_constraints(df: DataFrame, config: CleaningConfig) -> Tuple[DataFrame, DataFrame]:
    """
    Valide les contraintes métier des données de trades:
        - Prix > 0
        - Quantité > 0
        - Trade ID unique (optionnel)

    Args:
        df: DataFrame des trades bruts
        config: Configuration de nettoyage

    Returns:
        Tuple (DataFrame valide, DataFrame des rejets)
    """
    valid_condition = (
        (F.col("price") > config.min_price) &
        (F.col("price") <= config.max_price) &
        (F.col("quantity") > config.min_volume) &
        F.col("symbol").isNotNull() &
        F.col("timestamp").isNotNull()
    )

    df_valid = df.filter(valid_condition)
    df_rejected = df.filter(~valid_condition).withColumn(
        "rejection_reason",
        F.when(F.col("price") <= 0, F.lit("invalid_price"))
         .when(F.col("quantity") <= 0, F.lit("invalid_quantity"))
         .otherwise(F.lit("missing_required_field"))
    )

    return df_valid, df_rejected


# ─────────────────────── OUTLIER DETECTION ─────────────────────────────────

def detect_outliers_zscore(df: DataFrame, config: CleaningConfig) -> DataFrame:
    """
    Détecte les outliers sur le prix de clôture via Z-score.
    Ajoute une colonne `is_outlier` (boolean) et `zscore_close`.

    Z-score = (valeur - moyenne) / écart-type
    Outlier si |Z-score| > zscore_threshold (default: 3.0)

    Args:
        df: DataFrame OHLCV par (symbol, interval)
        config: Configuration avec zscore_threshold

    Returns:
        DataFrame avec colonnes `zscore_close` et `is_outlier`
    """
    # Fenêtre par symbol + interval, ordonnée par timestamp
    window_spec = Window.partitionBy("symbol", "interval").orderBy("timestamp")

    # Fenêtre pour calculer mean et stddev sur toute la partition
    window_agg = Window.partitionBy("symbol", "interval")

    df_with_stats = (
        df
        .withColumn("mean_close",   F.avg("close").over(window_agg))
        .withColumn("stddev_close", F.stddev("close").over(window_agg))
    )

    df_with_zscore = df_with_stats.withColumn(
        "zscore_close",
        F.when(
            F.col("stddev_close") > 0,
            F.abs((F.col("close") - F.col("mean_close")) / F.col("stddev_close"))
        ).otherwise(F.lit(0.0))
    )

    df_flagged = df_with_zscore.withColumn(
        "is_outlier",
        F.col("zscore_close") > config.zscore_threshold
    )

    outlier_count = df_flagged.filter(F.col("is_outlier")).count()
    if outlier_count > 0:
        logger.info(f"[Outliers] {outlier_count} outliers détectés (Z-score > {config.zscore_threshold})")

    return df_flagged.drop("mean_close", "stddev_close")


def detect_price_spikes(df: DataFrame, config: CleaningConfig) -> DataFrame:
    """
    Détecte les price spikes: variation anormale du prix entre deux bougies consécutives.

    spike = |close_t - close_{t-1}| / close_{t-1} > spike_threshold

    Args:
        df: DataFrame OHLCV trié par timestamp
        config: Configuration avec price_spike_threshold

    Returns:
        DataFrame avec colonnes `price_change_pct` et `is_price_spike`
    """
    window_spec = Window.partitionBy("symbol", "interval").orderBy("timestamp")

    df_with_prev = df.withColumn(
        "prev_close",
        F.lag("close", 1).over(window_spec)
    )

    df_with_change = df_with_prev.withColumn(
        "price_change_pct",
        F.when(
            F.col("prev_close").isNotNull() & (F.col("prev_close") > 0),
            F.abs((F.col("close") - F.col("prev_close")) / F.col("prev_close"))
        ).otherwise(F.lit(0.0))
    )

    df_with_spike = df_with_change.withColumn(
        "is_price_spike",
        F.col("price_change_pct") > config.price_spike_threshold
    )

    spike_count = df_with_spike.filter(F.col("is_price_spike")).count()
    if spike_count > 0:
        logger.info(
            f"[Price Spikes] {spike_count} spikes détectés "
            f"(seuil: {config.price_spike_threshold * 100:.1f}%)"
        )

    return df_with_spike.drop("prev_close")


# ─────────────────────── DEDUPLICATION ─────────────────────────────────────

def deduplicate_ohlcv(df: DataFrame) -> DataFrame:
    """
    Supprime les doublons OHLCV basés sur (symbol, interval, timestamp).
    En cas de doublon, garde la dernière version reçue.

    Args:
        df: DataFrame OHLCV

    Returns:
        DataFrame dédupliqué
    """
    # Fenêtre pour dé-dupliquer: garder la dernière ligne par clé unique
    window_dedup = Window.partitionBy("symbol", "interval", "timestamp").orderBy(
        F.col("timestamp").desc()
    )

    df_deduped = (
        df
        .withColumn("row_num", F.row_number().over(window_dedup))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    initial = df.count()
    final = df_deduped.count()
    if initial != final:
        logger.info(f"[Déduplication] {initial - final} doublons supprimés")

    return df_deduped


def deduplicate_trades(df: DataFrame) -> DataFrame:
    """
    Supprime les doublons de trades basés sur trade_id (si présent) ou
    sur (symbol, timestamp, price, quantity).

    Args:
        df: DataFrame des trades

    Returns:
        DataFrame dédupliqué
    """
    if "trade_id" in df.columns:
        dedup_cols = ["trade_id"]
    else:
        dedup_cols = ["symbol", "timestamp", "price", "quantity"]

    return df.dropDuplicates(dedup_cols)


# ─────────────────────── NORMALISATION ─────────────────────────────────────

def normalize_timestamp(df: DataFrame) -> DataFrame:
    """
    Convertit le timestamp epoch (ms) en colonne TimestampType standard.

    Args:
        df: DataFrame avec colonne `timestamp` en epoch milliseconds

    Returns:
        DataFrame avec colonne `event_time` en TimestampType
    """
    return df.withColumn(
        "event_time",
        F.to_timestamp(F.col("timestamp") / 1000)  # ms → seconds
    )


def normalize_symbol(df: DataFrame) -> DataFrame:
    """
    Normalise le nom du symbole: MAJUSCULES + suppression espaces.

    Args:
        df: DataFrame avec colonne `symbol`

    Returns:
        DataFrame avec `symbol` normalisé
    """
    return df.withColumn(
        "symbol",
        F.upper(F.trim(F.col("symbol")))
    )


# ─────────────────────── PIPELINE COMPLET ──────────────────────────────────

def clean_ohlcv_data(df: DataFrame, config: CleaningConfig) -> Tuple[DataFrame, dict]:
    """
    Pipeline complet de nettoyage des données OHLCV:
        1. Normalisation (symbol, timestamp)
        2. Suppression des nulls
        3. Validation des contraintes métier
        4. Déduplication
        5. Détection outliers (Z-score)
        6. Détection price spikes

    Args:
        df: DataFrame OHLCV brut
        config: Configuration de nettoyage

    Returns:
        Tuple (DataFrame nettoyé, dict avec métriques de qualité)
    """
    logger.info("[ETL] Démarrage du nettoyage des données OHLCV")
    metrics = {"initial_count": df.count()}

    # 1. Normalisation
    df = normalize_symbol(df)
    df = normalize_timestamp(df)

    # 2. Gestion des nulls
    df, nulls_dropped = handle_null_values(df, config)
    metrics["nulls_dropped"] = nulls_dropped

    # 3. Validation des contraintes
    df, df_rejected = validate_ohlcv_constraints(df, config)
    metrics["invalid_ohlcv"] = df_rejected.count()

    # 4. Déduplication
    df = deduplicate_ohlcv(df)

    # 5. Détection outliers
    df = detect_outliers_zscore(df, config)

    # 6. Détection price spikes
    df = detect_price_spikes(df, config)

    metrics["final_count"] = df.count()
    metrics["quality_rate"] = (
        round(metrics["final_count"] / metrics["initial_count"] * 100, 2)
        if metrics["initial_count"] > 0 else 0.0
    )

    logger.info(
        f"[ETL] Nettoyage terminé: {metrics['initial_count']} → {metrics['final_count']} lignes "
        f"(qualité: {metrics['quality_rate']}%)"
    )
    return df, metrics


def clean_trades_data(df: DataFrame, config: CleaningConfig) -> Tuple[DataFrame, dict]:
    """
    Pipeline complet de nettoyage des données de trades bruts:
        1. Normalisation
        2. Suppression des nulls
        3. Validation des contraintes
        4. Déduplication

    Args:
        df: DataFrame des trades bruts
        config: Configuration de nettoyage

    Returns:
        Tuple (DataFrame nettoyé, dict avec métriques de qualité)
    """
    logger.info("[ETL] Démarrage du nettoyage des trades")
    metrics = {"initial_count": df.count()}

    df = normalize_symbol(df)
    df = normalize_timestamp(df)

    # Remplacer les nulls
    required = [c for c in config.required_trade_columns if c in df.columns]
    df = df.dropna(subset=required)
    metrics["nulls_dropped"] = metrics["initial_count"] - df.count()

    # Validation
    df, df_rejected = validate_trades_constraints(df, config)
    metrics["invalid_trades"] = df_rejected.count()

    # Déduplication
    df = deduplicate_trades(df)

    metrics["final_count"] = df.count()
    metrics["quality_rate"] = (
        round(metrics["final_count"] / metrics["initial_count"] * 100, 2)
        if metrics["initial_count"] > 0 else 0.0
    )

    logger.info(
        f"[ETL] Trades nettoyés: {metrics['initial_count']} → {metrics['final_count']} "
        f"(qualité: {metrics['quality_rate']}%)"
    )
    return df, metrics

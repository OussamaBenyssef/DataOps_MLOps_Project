"""
Calcul des Indicateurs Techniques avec Apache Spark
=====================================================
Module de calcul des indicateurs techniques sur données OHLCV.
Utilise les Window functions de Spark pour le calcul sur séries temporelles.

Indicateurs implémentés:
    - SMA  : Simple Moving Average (20, 50, 200 périodes)
    - EMA  : Exponential Moving Average (12, 26 périodes)
    - RSI  : Relative Strength Index (14 périodes)
    - MACD : Moving Average Convergence Divergence (12, 26, 9)
    - BB   : Bollinger Bands (20 périodes, 2 écarts-types)
"""
import logging
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config import TechnicalIndicatorsConfig

logger = logging.getLogger(__name__)

# ─────────────────────── MOVING AVERAGES ───────────────────────────────────

def calculate_sma(df: DataFrame, periods: List[int], price_col: str = "close") -> DataFrame:
    """
    Calcule la Simple Moving Average (SMA) pour plusieurs périodes.

    SMA_n = mean(close_{t}, close_{t-1}, ..., close_{t-n+1})

    Args:
        df: DataFrame OHLCV avec colonnes (symbol, interval, timestamp, close)
        periods: Liste des périodes ex: [20, 50, 200]
        price_col: Colonne de prix (default: "close")

    Returns:
        DataFrame avec colonnes `sma_{n}` pour chaque période n
    """
    window_spec = Window.partitionBy("symbol", "interval").orderBy("timestamp")

    for n in periods:
        window_n = window_spec.rowsBetween(-(n - 1), 0)
        df = df.withColumn(
            f"sma_{n}",
            F.round(F.avg(F.col(price_col)).over(window_n), 8)
        )
        logger.debug(f"SMA_{n} calculée")

    return df


def calculate_ema(df: DataFrame, periods: List[int], price_col: str = "close") -> DataFrame:
    """
    Calcule l'Exponential Moving Average (EMA).

    EMA_t = price_t * k + EMA_{t-1} * (1 - k)
    où k = 2 / (period + 1)

    Note: Approximée via weighted moving average pour Spark (pas de boucle native).
    Pour une EMA exacte, on utilise une approximation avec lag suffisamment grand.

    Args:
        df: DataFrame OHLCV
        periods: Liste des périodes ex: [12, 26]
        price_col: Colonne de prix

    Returns:
        DataFrame avec colonnes `ema_{n}`
    """
    window_spec = Window.partitionBy("symbol", "interval").orderBy("timestamp")

    for n in periods:
        k = 2.0 / (n + 1)
        # Générer les poids exponentiels pour les n dernières valeurs
        # w_i = k * (1-k)^i pour i = 0, 1, ..., n-1
        weights = [k * ((1 - k) ** i) for i in range(n)]
        total_weight = sum(weights)

        # Calcul via lag: sum(close_{t-i} * w_i) / sum(w_i)
        numerator_expr = F.lit(0.0)
        for i, w in enumerate(weights):
            lagged = F.lag(F.col(price_col), i).over(window_spec)
            numerator_expr = numerator_expr + F.coalesce(lagged, F.lit(0.0)) * F.lit(w)

        df = df.withColumn(
            f"ema_{n}",
            F.round(numerator_expr / F.lit(total_weight), 8)
        )
        logger.debug(f"EMA_{n} calculée")

    return df


# ─────────────────────── RSI ───────────────────────────────────────────────

def calculate_rsi(df: DataFrame, period: int = 14, price_col: str = "close") -> DataFrame:
    """
    Calcule le Relative Strength Index (RSI) sur `period` périodes.

    Algorithme:
        1. Calculer les variations: delta = close_t - close_{t-1}
        2. Séparer gains (delta > 0) et pertes (delta < 0)
        3. Calculer moyennes glissantes des gains et pertes
        4. RS = avg_gain / avg_loss
        5. RSI = 100 - (100 / (1 + RS))

    Interprétation:
        - RSI > 70 → Surachat (potentiel retournement baissier)
        - RSI < 30 → Survente (potentiel retournement haussier)
        - RSI 40-60 → Zone neutre

    Args:
        df: DataFrame OHLCV
        period: Nombre de périodes (standard: 14)
        price_col: Colonne de prix

    Returns:
        DataFrame avec colonnes:
            - `rsi_{period}`: Valeur RSI (0-100)
            - `rsi_signal`: Signal ("overbought" | "oversold" | "neutral")
    """
    logger.info(f"Calcul RSI({period})")

    window_spec = Window.partitionBy("symbol", "interval").orderBy("timestamp")
    window_n    = window_spec.rowsBetween(-(period - 1), 0)

    # Étape 1: Variation de prix
    df = df.withColumn(
        "price_delta",
        F.col(price_col) - F.lag(F.col(price_col), 1).over(window_spec)
    )

    # Étape 2: Gain / Perte
    df = df.withColumn(
        "gain",
        F.when(F.col("price_delta") > 0, F.col("price_delta")).otherwise(F.lit(0.0))
    ).withColumn(
        "loss",
        F.when(F.col("price_delta") < 0, F.abs(F.col("price_delta"))).otherwise(F.lit(0.0))
    )

    # Étape 3: Moyennes glissantes
    df = df.withColumn(
        "avg_gain",
        F.avg(F.col("gain")).over(window_n)
    ).withColumn(
        "avg_loss",
        F.avg(F.col("loss")).over(window_n)
    )

    # Étape 4 & 5: RS → RSI
    df = df.withColumn(
        f"rsi_{period}",
        F.round(
            F.when(
                F.col("avg_loss") == 0,
                F.lit(100.0)   # Pas de pertes = RSI max
            ).otherwise(
                100.0 - (100.0 / (1.0 + (F.col("avg_gain") / F.col("avg_loss"))))
            ),
            2
        )
    )

    # Signal RSI
    df = df.withColumn(
        "rsi_signal",
        F.when(F.col(f"rsi_{period}") >= 70, F.lit("overbought"))
         .when(F.col(f"rsi_{period}") <= 30, F.lit("oversold"))
         .otherwise(F.lit("neutral"))
    )

    # Nettoyage des colonnes intermédiaires
    df = df.drop("price_delta", "gain", "loss", "avg_gain", "avg_loss")

    return df


# ─────────────────────── MACD ──────────────────────────────────────────────

def calculate_macd(
    df: DataFrame,
    fast_period: int = 12,
    slow_period: int = 26,
    signal_period: int = 9,
    price_col: str = "close"
) -> DataFrame:
    """
    Calcule le MACD (Moving Average Convergence Divergence).

    Algorithme:
        1. MACD Line   = EMA(fast) - EMA(slow)
        2. Signal Line = EMA(MACD Line, signal_period)
        3. Histogram   = MACD Line - Signal Line

    Interprétation:
        - MACD croise signal vers le haut → Signal d'achat
        - MACD croise signal vers le bas  → Signal de vente
        - Histogramme positif = momentum haussier
        - Histogramme négatif = momentum baissier

    Args:
        df: DataFrame OHLCV (doit déjà contenir ema_12 et ema_26 si calculées)
        fast_period: Période EMA rapide (default: 12)
        slow_period: Période EMA lente (default: 26)
        signal_period: Période EMA du signal (default: 9)
        price_col: Colonne de prix

    Returns:
        DataFrame avec colonnes:
            - `macd_line`: Ligne MACD
            - `macd_signal`: Ligne Signal
            - `macd_histogram`: Histogramme
            - `macd_cross`: Signal de croisement ("bullish_cross" | "bearish_cross" | "none")
    """
    logger.info(f"Calcul MACD({fast_period},{slow_period},{signal_period})")

    window_spec = Window.partitionBy("symbol", "interval").orderBy("timestamp")

    # Calculer EMA fast et slow si pas déjà calculées
    if f"ema_{fast_period}" not in df.columns:
        df = calculate_ema(df, [fast_period], price_col)
    if f"ema_{slow_period}" not in df.columns:
        df = calculate_ema(df, [slow_period], price_col)

    # 1. Ligne MACD
    df = df.withColumn(
        "macd_line",
        F.round(F.col(f"ema_{fast_period}") - F.col(f"ema_{slow_period}"), 8)
    )

    # 2. Signal Line = EMA(macd_line, signal_period)
    k_signal = 2.0 / (signal_period + 1)
    weights_signal = [k_signal * ((1 - k_signal) ** i) for i in range(signal_period)]
    total_weight_signal = sum(weights_signal)

    signal_numerator = F.lit(0.0)
    for i, w in enumerate(weights_signal):
        lagged_macd = F.lag(F.col("macd_line"), i).over(window_spec)
        signal_numerator = signal_numerator + F.coalesce(lagged_macd, F.lit(0.0)) * F.lit(w)

    df = df.withColumn(
        "macd_signal",
        F.round(signal_numerator / F.lit(total_weight_signal), 8)
    )

    # 3. Histogramme
    df = df.withColumn(
        "macd_histogram",
        F.round(F.col("macd_line") - F.col("macd_signal"), 8)
    )

    # 4. Détection croisements
    df = df.withColumn(
        "prev_histogram",
        F.lag("macd_histogram", 1).over(window_spec)
    ).withColumn(
        "macd_cross",
        F.when(
            (F.col("prev_histogram") < 0) & (F.col("macd_histogram") > 0),
            F.lit("bullish_cross")
        ).when(
            (F.col("prev_histogram") > 0) & (F.col("macd_histogram") < 0),
            F.lit("bearish_cross")
        ).otherwise(F.lit("none"))
    ).drop("prev_histogram")

    return df


# ─────────────────────── BOLLINGER BANDS ───────────────────────────────────

def calculate_bollinger_bands(
    df: DataFrame,
    period: int = 20,
    std_dev: float = 2.0,
    price_col: str = "close"
) -> DataFrame:
    """
    Calcule les Bandes de Bollinger.

    Algorithme:
        1. Middle Band = SMA(period)
        2. Écart-type = stddev(close, period)
        3. Upper Band  = Middle + (std_dev × écart-type)
        4. Lower Band  = Middle - (std_dev × écart-type)
        5. Bandwidth   = (Upper - Lower) / Middle × 100
        6. %B          = (Close - Lower) / (Upper - Lower)

    Interprétation:
        - Prix au-dessus de Upper Band → Potentiellement suracheté
        - Prix en-dessous de Lower Band → Potentiellement survendu
        - Bandes serrées → Faible volatilité (potentiel breakout)
        - Bandes larges → Forte volatilité

    Args:
        df: DataFrame OHLCV
        period: Période (default: 20)
        std_dev: Nombre d'écarts-types (default: 2.0)
        price_col: Colonne de prix

    Returns:
        DataFrame avec colonnes:
            - `bb_middle`: Bande centrale (SMA)
            - `bb_upper`: Bande supérieure
            - `bb_lower`: Bande inférieure
            - `bb_bandwidth`: Largeur des bandes (%)
            - `bb_pct_b`: Position du prix dans la bande (%B)
            - `bb_signal`: Signal ("above_upper" | "below_lower" | "inside")
    """
    logger.info(f"Calcul Bollinger Bands({period}, {std_dev}σ)")

    window_spec = Window.partitionBy("symbol", "interval").orderBy("timestamp")
    window_n    = window_spec.rowsBetween(-(period - 1), 0)

    # 1. Bande centrale (SMA)
    df = df.withColumn(
        "bb_middle",
        F.round(F.avg(F.col(price_col)).over(window_n), 8)
    )

    # 2. Écart-type (population, non biaised)
    df = df.withColumn(
        "bb_std",
        F.stddev(F.col(price_col)).over(window_n)
    )

    # 3. Bandes supérieure et inférieure
    df = df.withColumn(
        "bb_upper",
        F.round(F.col("bb_middle") + (F.lit(std_dev) * F.col("bb_std")), 8)
    ).withColumn(
        "bb_lower",
        F.round(F.col("bb_middle") - (F.lit(std_dev) * F.col("bb_std")), 8)
    )

    # 4. Bandwidth (%)
    df = df.withColumn(
        "bb_bandwidth",
        F.round(
            F.when(
                F.col("bb_middle") > 0,
                ((F.col("bb_upper") - F.col("bb_lower")) / F.col("bb_middle")) * 100.0
            ).otherwise(F.lit(0.0)),
            4
        )
    )

    # 5. %B: position du prix dans la bande (0 = lower, 1 = upper, >1 = above)
    df = df.withColumn(
        "bb_pct_b",
        F.round(
            F.when(
                (F.col("bb_upper") - F.col("bb_lower")) > 0,
                (F.col(price_col) - F.col("bb_lower")) / (F.col("bb_upper") - F.col("bb_lower"))
            ).otherwise(F.lit(0.5)),
            4
        )
    )

    # 6. Signal
    df = df.withColumn(
        "bb_signal",
        F.when(F.col(price_col) > F.col("bb_upper"), F.lit("above_upper"))
         .when(F.col(price_col) < F.col("bb_lower"), F.lit("below_lower"))
         .otherwise(F.lit("inside"))
    )

    # Supprimer colonne intermédiaire
    df = df.drop("bb_std")

    return df


# ─────────────────────── PIPELINE COMPLET ──────────────────────────────────

def calculate_all_indicators(df: DataFrame, config: TechnicalIndicatorsConfig) -> DataFrame:
    """
    Pipeline complet de calcul de tous les indicateurs techniques.

    Ordre de calcul:
        1. SMA (20, 50, 200)
        2. EMA (12, 26) — nécessaires pour MACD
        3. RSI (14)
        4. MACD (12, 26, 9)
        5. Bollinger Bands (20, 2σ)

    Args:
        df: DataFrame OHLCV nettoyé, avec colonnes (symbol, interval, timestamp, open, high, low, close, volume)
        config: Configuration des indicateurs

    Returns:
        DataFrame enrichi avec tous les indicateurs techniques
    """
    logger.info("[Indicateurs] Début du calcul de tous les indicateurs techniques")

    # 1. SMA
    logger.info(f"[Indicateurs] Calcul SMA pour périodes: {config.sma_periods}")
    df = calculate_sma(df, config.sma_periods)

    # 2. EMA (requis avant MACD)
    logger.info(f"[Indicateurs] Calcul EMA pour périodes: {config.ema_periods}")
    df = calculate_ema(df, config.ema_periods)

    # 3. RSI
    logger.info(f"[Indicateurs] Calcul RSI({config.rsi_period})")
    df = calculate_rsi(df, period=config.rsi_period)

    # 4. MACD
    logger.info(
        f"[Indicateurs] Calcul MACD({config.macd_fast_period},"
        f"{config.macd_slow_period},{config.macd_signal_period})"
    )
    df = calculate_macd(
        df,
        fast_period=config.macd_fast_period,
        slow_period=config.macd_slow_period,
        signal_period=config.macd_signal_period,
    )

    # 5. Bollinger Bands
    logger.info(
        f"[Indicateurs] Calcul Bollinger Bands({config.bollinger_period}, {config.bollinger_std_dev}σ)"
    )
    df = calculate_bollinger_bands(
        df,
        period=config.bollinger_period,
        std_dev=config.bollinger_std_dev,
    )

    # Ajouter métadonnées de calcul
    df = df.withColumn("indicators_computed_at", F.current_timestamp())

    logger.info("[Indicateurs] Calcul terminé avec succès")
    return df


def get_indicator_columns() -> List[str]:
    """
    Retourne la liste de toutes les colonnes d'indicateurs générées.

    Returns:
        Liste des noms de colonnes d'indicateurs
    """
    return [
        # SMA
        "sma_20", "sma_50", "sma_200",
        # EMA
        "ema_12", "ema_26",
        # RSI
        "rsi_14", "rsi_signal",
        # MACD
        "macd_line", "macd_signal", "macd_histogram", "macd_cross",
        # Bollinger Bands
        "bb_middle", "bb_upper", "bb_lower", "bb_bandwidth", "bb_pct_b", "bb_signal",
        # Métadonnées
        "indicators_computed_at",
    ]

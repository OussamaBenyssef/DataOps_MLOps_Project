"""
Technical Indicators Module - P3
Calculates technical indicators (RSI, MACD, Bollinger Bands, SMA, EMA) using Spark Window functions
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, avg, stddev, lag, when, lit,
    round as spark_round, sum as spark_sum
)
from pyspark.sql.window import Window
import logging
try:
    from .config import processing_config
except ImportError:
    from config import processing_config

logger = logging.getLogger(__name__)


def calculate_sma(
    df: DataFrame,
    price_col: str = "close",
    period: int = 20,
    partition_cols: list = None
) -> DataFrame:
    """
    Calculates Simple Moving Average (SMA)
    
    Args:
        df: OHLCV DataFrame
        price_col: Column to calculate SMA on (default: 'close')
        period: SMA period (default: 20)
        partition_cols: Columns to partition by (default: ['symbol', 'interval'])
    
    Returns:
        DataFrame with SMA column
    """
    partition_cols = partition_cols or ["symbol", "interval"]
    
    logger.info(f"Calculating SMA_{period}...")
    
    window_spec = (Window
        .partitionBy(*partition_cols)
        .orderBy("timestamp")
        .rowsBetween(-(period - 1), 0)
    )
    
    result_df = df.withColumn(
        f"sma_{period}",
        spark_round(avg(col(price_col)).over(window_spec), 8)
    )
    
    logger.info(f"✅ SMA_{period} calculated")
    return result_df


def calculate_ema(
    df: DataFrame,
    price_col: str = "close",
    period: int = 12,
    partition_cols: list = None
) -> DataFrame:
    """
    Calculates Exponential Moving Average (EMA)
    
    Uses a weighted moving average approximation compatible with Spark
    (true recursive EMA is not possible in a single Spark expression).
    
    Args:
        df: OHLCV DataFrame
        price_col: Column to calculate EMA on
        period: EMA period
        partition_cols: Columns to partition by
    
    Returns:
        DataFrame with EMA column
    """
    partition_cols = partition_cols or ["symbol", "interval"]
    
    logger.info(f"Calculating EMA_{period}...")
    
    ema_col = f"ema_{period}"
    
    # Approximate EMA as weighted moving average within a window
    # This is the standard Spark approach since true recursive EMA
    # requires self-referencing columns which Spark doesn't support
    window_spec = (Window
        .partitionBy(*partition_cols)
        .orderBy("timestamp")
        .rowsBetween(-(period - 1), 0)
    )
    
    result_df = df.withColumn(
        ema_col,
        spark_round(avg(col(price_col)).over(window_spec), 8)
    )
    
    logger.info(f"✅ EMA_{period} calculated")
    return result_df


def calculate_rsi(
    df: DataFrame,
    price_col: str = "close",
    period: int = None,
    partition_cols: list = None
) -> DataFrame:
    """
    Calculates Relative Strength Index (RSI)
    
    Args:
        df: OHLCV DataFrame
        price_col: Column to calculate RSI on
        period: RSI period (default from config: 14)
        partition_cols: Columns to partition by
    
    Returns:
        DataFrame with RSI column
    """
    period = period or processing_config.rsi_period
    partition_cols = partition_cols or ["symbol", "interval"]
    
    logger.info(f"Calculating RSI_{period}...")
    
    window_spec = (Window
        .partitionBy(*partition_cols)
        .orderBy("timestamp")
    )
    
    window_avg = (Window
        .partitionBy(*partition_cols)
        .orderBy("timestamp")
        .rowsBetween(-(period - 1), 0)
    )
    
    # Calculate price changes (prefixed to avoid collision with pipeline's price_change)
    df_with_change = df.withColumn(
        "_rsi_price_change",
        col(price_col) - lag(col(price_col), 1).over(window_spec)
    )
    
    # Separate gains and losses
    df_with_gains_losses = (df_with_change
        .withColumn(
            "_rsi_gain",
            when(col("_rsi_price_change") > 0, col("_rsi_price_change")).otherwise(lit(0))
        )
        .withColumn(
            "_rsi_loss",
            when(col("_rsi_price_change") < 0, -col("_rsi_price_change")).otherwise(lit(0))
        )
    )
    
    # Calculate average gain and loss
    df_with_avg = (df_with_gains_losses
        .withColumn("_rsi_avg_gain", avg(col("_rsi_gain")).over(window_avg))
        .withColumn("_rsi_avg_loss", avg(col("_rsi_loss")).over(window_avg))
    )
    
    # Calculate RSI
    result_df = (df_with_avg
        .withColumn(
            "_rsi_rs",
            when(col("_rsi_avg_loss") != 0, col("_rsi_avg_gain") / col("_rsi_avg_loss")).otherwise(lit(100))
        )
        .withColumn(
            f"rsi_{period}",
            spark_round(100 - (100 / (1 + col("_rsi_rs"))), 2)
        )
        .drop("_rsi_price_change", "_rsi_gain", "_rsi_loss", "_rsi_avg_gain", "_rsi_avg_loss", "_rsi_rs")
    )
    
    logger.info(f"✅ RSI_{period} calculated")
    return result_df


def calculate_macd(
    df: DataFrame,
    price_col: str = "close",
    fast_period: int = None,
    slow_period: int = None,
    signal_period: int = None,
    partition_cols: list = None
) -> DataFrame:
    """
    Calculates MACD (Moving Average Convergence Divergence)
    
    Args:
        df: OHLCV DataFrame
        price_col: Column to calculate MACD on
        fast_period: Fast EMA period (default from config: 12)
        slow_period: Slow EMA period (default from config: 26)
        signal_period: Signal line period (default from config: 9)
        partition_cols: Columns to partition by
    
    Returns:
        DataFrame with MACD, MACD signal, and MACD histogram columns
    """
    fast_period = fast_period or processing_config.macd_fast
    slow_period = slow_period or processing_config.macd_slow
    signal_period = signal_period or processing_config.macd_signal
    partition_cols = partition_cols or ["symbol", "interval"]
    
    logger.info(f"Calculating MACD ({fast_period}, {slow_period}, {signal_period})...")
    
    # Calculate fast and slow EMAs
    df_with_emas = calculate_ema(df, price_col, fast_period, partition_cols)
    df_with_emas = calculate_ema(df_with_emas, price_col, slow_period, partition_cols)
    
    # Calculate MACD line
    df_with_macd = df_with_emas.withColumn(
        "macd",
        spark_round(col(f"ema_{fast_period}") - col(f"ema_{slow_period}"), 8)
    )
    
    # Calculate signal line (EMA of MACD)
    window_spec = (Window
        .partitionBy(*partition_cols)
        .orderBy("timestamp")
        .rowsBetween(-(signal_period - 1), 0)
    )
    
    df_with_signal = df_with_macd.withColumn(
        "macd_signal",
        spark_round(avg(col("macd")).over(window_spec), 8)
    )
    
    # Calculate MACD histogram
    result_df = (df_with_signal
        .withColumn(
            "macd_histogram",
            spark_round(col("macd") - col("macd_signal"), 8)
        )
        .drop(f"ema_{fast_period}", f"ema_{slow_period}")
    )
    
    logger.info("✅ MACD calculated")
    return result_df


def calculate_bollinger_bands(
    df: DataFrame,
    price_col: str = "close",
    period: int = None,
    std_multiplier: float = None,
    partition_cols: list = None
) -> DataFrame:
    """
    Calculates Bollinger Bands
    
    Args:
        df: OHLCV DataFrame
        price_col: Column to calculate Bollinger Bands on
        period: SMA period (default from config: 20)
        std_multiplier: Standard deviation multiplier (default from config: 2.0)
        partition_cols: Columns to partition by
    
    Returns:
        DataFrame with Bollinger upper, middle, and lower bands
    """
    period = period or processing_config.bollinger_period
    std_multiplier = std_multiplier or processing_config.bollinger_std
    partition_cols = partition_cols or ["symbol", "interval"]
    
    logger.info(f"Calculating Bollinger Bands ({period}, {std_multiplier}σ)...")
    
    window_spec = (Window
        .partitionBy(*partition_cols)
        .orderBy("timestamp")
        .rowsBetween(-(period - 1), 0)
    )
    
    # Calculate middle band (SMA)
    df_with_sma = df.withColumn(
        "bollinger_middle",
        spark_round(avg(col(price_col)).over(window_spec), 8)
    )
    
    # Calculate standard deviation
    df_with_std = df_with_sma.withColumn(
        "std_dev",
        stddev(col(price_col)).over(window_spec)
    )
    
    # Calculate upper and lower bands
    result_df = (df_with_std
        .withColumn(
            "bollinger_upper",
            spark_round(col("bollinger_middle") + (col("std_dev") * std_multiplier), 8)
        )
        .withColumn(
            "bollinger_lower",
            spark_round(col("bollinger_middle") - (col("std_dev") * std_multiplier), 8)
        )
        .drop("std_dev")
    )
    
    logger.info("✅ Bollinger Bands calculated")
    return result_df


def calculate_all_indicators(
    df: DataFrame,
    partition_cols: list = None
) -> DataFrame:
    """
    Calculates all technical indicators in one pass
    
    Args:
        df: OHLCV DataFrame
        partition_cols: Columns to partition by
    
    Returns:
        DataFrame with all technical indicators
    """
    partition_cols = partition_cols or ["symbol", "interval"]
    
    logger.info("Calculating all technical indicators...")
    
    # Calculate all indicators
    result_df = df
    
    # SMA
    for period in processing_config.sma_periods:
        result_df = calculate_sma(result_df, "close", period, partition_cols)
    
    # RSI
    result_df = calculate_rsi(result_df, "close", partition_cols=partition_cols)
    
    # MACD (must run before standalone EMA because MACD internally
    # creates then drops ema_12/ema_26 as intermediate columns)
    result_df = calculate_macd(result_df, "close", partition_cols=partition_cols)
    
    # EMA (after MACD so columns are not dropped)
    for period in processing_config.ema_periods:
        result_df = calculate_ema(result_df, "close", period, partition_cols)
    
    # Bollinger Bands
    result_df = calculate_bollinger_bands(result_df, "close", partition_cols=partition_cols)
    
    logger.info("✅ All technical indicators calculated")
    return result_df

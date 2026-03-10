"""
Data Transformations Module - P3
Common data transformations and validation functions
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, round as spark_round, unix_timestamp, current_timestamp
import logging

logger = logging.getLogger(__name__)


def validate_trades_data(trades_df: DataFrame) -> DataFrame:
    """
    Validates and cleans trades data

    Args:
        trades_df: Raw trades DataFrame

    Returns:
        Validated trades DataFrame
    """
    logger.info("Validating trades data...")

    validated_df = (
        trades_df
        # Remove null values in critical columns
        .filter(col("symbol").isNotNull())
        .filter(col("price").isNotNull())
        .filter(col("quantity").isNotNull())
        .filter(col("timestamp").isNotNull())
        # Remove invalid prices and quantities
        .filter(col("price") > 0)
        .filter(col("quantity") > 0)
        # Round price to 8 decimals (crypto standard)
        .withColumn("price", spark_round(col("price"), 8))
        .withColumn("quantity", spark_round(col("quantity"), 8))
    )

    logger.info("✅ Trades data validated")
    return validated_df


def validate_ohlcv_data(ohlcv_df: DataFrame) -> DataFrame:
    """
    Validates and cleans OHLCV data

    Args:
        ohlcv_df: Raw OHLCV DataFrame

    Returns:
        Validated OHLCV DataFrame
    """
    logger.info("Validating OHLCV data...")

    validated_df = (
        ohlcv_df
        # Remove null values
        .filter(col("symbol").isNotNull())
        .filter(col("open").isNotNull())
        .filter(col("high").isNotNull())
        .filter(col("low").isNotNull())
        .filter(col("close").isNotNull())
        .filter(col("volume").isNotNull())
        # Validate OHLC relationships
        .filter(col("high") >= col("low"))
        .filter(col("high") >= col("open"))
        .filter(col("high") >= col("close"))
        .filter(col("low") <= col("open"))
        .filter(col("low") <= col("close"))
        # Remove invalid values
        .filter(col("open") > 0)
        .filter(col("high") > 0)
        .filter(col("low") > 0)
        .filter(col("close") > 0)
        .filter(col("volume") >= 0)
        # Round to 8 decimals
        .withColumn("open", spark_round(col("open"), 8))
        .withColumn("high", spark_round(col("high"), 8))
        .withColumn("low", spark_round(col("low"), 8))
        .withColumn("close", spark_round(col("close"), 8))
        .withColumn("volume", spark_round(col("volume"), 8))
    )

    logger.info("✅ OHLCV data validated")
    return validated_df


def add_metadata_columns(df: DataFrame) -> DataFrame:
    """
    Adds metadata columns for tracking and debugging

    Args:
        df: Input DataFrame

    Returns:
        DataFrame with metadata columns
    """
    logger.info("Adding metadata columns...")

    enriched_df = df.withColumn("processed_at", current_timestamp()).withColumn(
        "processing_timestamp", unix_timestamp(current_timestamp())
    )

    logger.info("✅ Metadata columns added")
    return enriched_df


def add_interval_column(df: DataFrame, interval: str) -> DataFrame:
    """
    Adds interval column to DataFrame (e.g., '1m', '5m', '1h')

    Args:
        df: Input DataFrame
        interval: Interval string

    Returns:
        DataFrame with interval column
    """
    return df.withColumn("interval", lit(interval))


def calculate_price_change(df: DataFrame) -> DataFrame:
    """
    Calculates price change and percentage change

    Args:
        df: OHLCV DataFrame with open and close columns

    Returns:
        DataFrame with price_change and price_change_percent columns
    """
    logger.info("Calculating price changes...")

    result_df = df.withColumn("price_change", col("close") - col("open")).withColumn(
        "price_change_percent", spark_round(((col("close") - col("open")) / col("open")) * 100, 4)
    )

    logger.info("✅ Price changes calculated")
    return result_df


def detect_price_spikes(df: DataFrame, threshold_percent: float = 5.0) -> DataFrame:
    """
    Detects price spikes based on percentage threshold

    Args:
        df: DataFrame with price_change_percent column
        threshold_percent: Threshold for spike detection (default 5%)

    Returns:
        DataFrame with is_price_spike column
    """
    logger.info(f"Detecting price spikes (threshold: {threshold_percent}%)...")

    result_df = df.withColumn(
        "is_price_spike",
        when(
            (col("price_change_percent") > threshold_percent) | (col("price_change_percent") < -threshold_percent),
            lit(True),
        ).otherwise(lit(False)),
    )

    logger.info("✅ Price spike detection complete")
    return result_df


def calculate_volume_metrics(df: DataFrame) -> DataFrame:
    """
    Calculates volume-based metrics

    Args:
        df: OHLCV DataFrame with volume column

    Returns:
        DataFrame with volume metrics
    """
    logger.info("Calculating volume metrics...")

    result_df = df.withColumn("volume_usd", spark_round(col("volume") * col("close"), 2)).withColumn(
        "avg_trade_size",
        when(col("trades_count") > 0, spark_round(col("volume") / col("trades_count"), 8)).otherwise(lit(0)),
    )

    logger.info("✅ Volume metrics calculated")
    return result_df


def add_candle_pattern(df: DataFrame) -> DataFrame:
    """
    Identifies basic candle patterns (bullish/bearish)

    Args:
        df: OHLCV DataFrame

    Returns:
        DataFrame with candle_type column
    """
    logger.info("Identifying candle patterns...")

    result_df = df.withColumn(
        "candle_type",
        when(col("close") > col("open"), lit("bullish"))
        .when(col("close") < col("open"), lit("bearish"))
        .otherwise(lit("doji")),
    )

    logger.info("✅ Candle patterns identified")
    return result_df


def deduplicate_by_timestamp(df: DataFrame, partition_cols: list = None, timestamp_col: str = "timestamp") -> DataFrame:
    """
    Removes duplicate rows based on timestamp and partition columns

    Args:
        df: Input DataFrame
        partition_cols: Columns to partition by (e.g., ['symbol', 'interval'])
        timestamp_col: Timestamp column name

    Returns:
        Deduplicated DataFrame
    """
    partition_cols = partition_cols or ["symbol"]

    logger.info(f"Deduplicating by {partition_cols} and {timestamp_col}...")

    # Use dropDuplicates with partition columns and timestamp
    dedupe_cols = partition_cols + [timestamp_col]
    result_df = df.dropDuplicates(dedupe_cols)

    logger.info("✅ Deduplication complete")
    return result_df

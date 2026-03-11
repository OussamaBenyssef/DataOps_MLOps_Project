"""
Aggregated Metrics Module - P3
Calculates aggregated metrics (daily, hourly) from OHLCV data using Spark
groupBy/window aggregations and writes results to MongoDB.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    avg,
    min as spark_min,
    max as spark_max,
    sum as spark_sum,
    count,
    stddev,
    first,
    last,
    when,
    lit,
    current_timestamp,
    round as spark_round,
    window,
)
import logging

logger = logging.getLogger(__name__)


def calculate_aggregated_metrics(
    df: DataFrame, window_duration: str = "1 day", slide_duration: str = None, partition_cols: list = None
) -> DataFrame:
    """
    Calculates aggregated metrics over a time window from OHLCV data.

    Computes: avg/min/max price, volatility, total/avg volume, VWAP,
    price range %, period return %, open/close price, num candles, total trades.

    Args:
        df: OHLCV DataFrame with columns: symbol, interval, timestamp,
            open, high, low, close, volume (and optionally trades_count/num_trades)
        window_duration: Window size for aggregation (e.g. '1 hour', '1 day')
        slide_duration: Optional slide duration for sliding windows
        partition_cols: Columns to group by (default: ['symbol'])

    Returns:
        DataFrame with aggregated metrics
    """
    partition_cols = partition_cols or ["symbol"]

    logger.info(f"Calculating aggregated metrics (window: {window_duration})...")

    # Determine the interval label based on window duration
    interval_label = _window_to_interval_label(window_duration)

    # Determine trades column name (handle both conventions)
    trades_col = _get_trades_column(df)

    # Build window spec for time-based grouping
    if slide_duration:
        time_window = window(col("timestamp"), window_duration, slide_duration)
    else:
        time_window = window(col("timestamp"), window_duration)

    # Build groupBy columns
    group_cols = partition_cols + [time_window]

    # Compute volume * close for VWAP calculation
    df_with_vwap_component = df.withColumn("_volume_price", col("volume") * col("close"))

    # Build aggregations
    agg_exprs = [
        # Period boundaries
        spark_min(col("timestamp")).alias("period_start"),
        spark_max(col("timestamp")).alias("period_end"),
        # Price metrics
        spark_round(avg(col("close")), 8).alias("avg_price"),
        spark_round(spark_min(col("low")), 8).alias("min_price"),
        spark_round(spark_max(col("high")), 8).alias("max_price"),
        spark_round(first(col("close")), 8).alias("open_price"),
        spark_round(last(col("close")), 8).alias("close_price"),
        spark_round(stddev(col("close")), 8).alias("price_volatility"),
        # Volume metrics
        spark_round(spark_sum(col("volume")), 8).alias("total_volume"),
        spark_round(avg(col("volume")), 8).alias("avg_volume"),
        # VWAP: sum(volume * close) / sum(volume)
        spark_round(spark_sum(col("_volume_price")) / spark_sum(col("volume")), 8).alias("vwap"),
        # Count
        count("*").cast("int").alias("num_candles"),
    ]

    # Add trades aggregation if column exists
    if trades_col:
        agg_exprs.append(spark_sum(col(trades_col)).cast("int").alias("total_trades"))

    # Execute aggregation
    result_df = df_with_vwap_component.groupBy(*group_cols).agg(*agg_exprs)

    # Add derived metrics
    result_df = (
        result_df
        # Price range percentage: (max - min) / min * 100
        .withColumn(
            "price_range_pct",
            spark_round(
                when(col("min_price") > 0, ((col("max_price") - col("min_price")) / col("min_price")) * 100).otherwise(
                    lit(0.0)
                ),
                4,
            ),
        )
        # Period return percentage: (close - open) / open * 100
        .withColumn(
            "period_return_pct",
            spark_round(
                when(
                    col("open_price") > 0, ((col("close_price") - col("open_price")) / col("open_price")) * 100
                ).otherwise(lit(0.0)),
                4,
            ),
        )
        # Add interval label
        .withColumn("interval", lit(interval_label))
        # Add calculation timestamp
        .withColumn("calculated_at", current_timestamp())
    )

    # Fill nulls for total_trades if column was not present in source
    if trades_col is None:
        result_df = result_df.withColumn("total_trades", lit(0).cast("int"))

    # Handle null volatility (when only 1 candle in window)
    result_df = result_df.withColumn(
        "price_volatility", when(col("price_volatility").isNull(), lit(0.0)).otherwise(col("price_volatility"))
    )

    # Drop the window struct column and select final columns
    result_df = result_df.select(
        "symbol",
        "interval",
        "period_start",
        "period_end",
        "avg_price",
        "min_price",
        "max_price",
        "open_price",
        "close_price",
        "price_volatility",
        "price_range_pct",
        "period_return_pct",
        "total_volume",
        "avg_volume",
        "total_trades",
        "vwap",
        "num_candles",
        "calculated_at",
    )

    logger.info(f"✅ Aggregated metrics calculated (window: {window_duration})")
    return result_df


def calculate_daily_metrics(df: DataFrame, partition_cols: list = None) -> DataFrame:
    """
    Calculates daily aggregated metrics from OHLCV data.

    Args:
        df: OHLCV DataFrame
        partition_cols: Columns to group by

    Returns:
        DataFrame with daily aggregated metrics
    """
    logger.info("Calculating daily aggregated metrics...")
    return calculate_aggregated_metrics(df, window_duration="1 day", partition_cols=partition_cols)


def calculate_hourly_metrics(df: DataFrame, partition_cols: list = None) -> DataFrame:
    """
    Calculates hourly aggregated metrics from OHLCV data.

    Args:
        df: OHLCV DataFrame
        partition_cols: Columns to group by

    Returns:
        DataFrame with hourly aggregated metrics
    """
    logger.info("Calculating hourly aggregated metrics...")
    return calculate_aggregated_metrics(df, window_duration="1 hour", partition_cols=partition_cols)


def _window_to_interval_label(window_duration: str) -> str:
    """Maps window duration string to interval label for MongoDB storage."""
    mapping = {
        "1 hour": "1h",
        "4 hours": "4h",
        "1 day": "1d",
        "1 week": "1w",
    }
    return mapping.get(window_duration.lower(), window_duration)


def _get_trades_column(df: DataFrame) -> str:
    """
    Detects the trades count column name in the DataFrame.
    Different sources may use 'trades_count', 'num_trades', or 'trades'.

    Returns:
        Column name if found, None otherwise
    """
    for col_name in ["trades_count", "num_trades", "trades"]:
        if col_name in df.columns:
            return col_name
    return None

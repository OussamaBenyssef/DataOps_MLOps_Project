"""
Kafka Consumer Module - P3
Consumes data from Kafka topics using Spark Structured Streaming
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, to_timestamp, expr, window
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    LongType, BooleanType, TimestampType
)
import logging
from .config import kafka_config

logger = logging.getLogger(__name__)


# ============================================
# KAFKA SCHEMAS
# ============================================

# Schema for raw_trades topic
RAW_TRADES_SCHEMA = StructType([
    StructField("symbol", StringType(), False),
    StructField("price", DoubleType(), False),
    StructField("quantity", DoubleType(), False),
    StructField("timestamp", LongType(), False),  # Unix timestamp in ms
    StructField("trade_id", LongType(), True),
    StructField("is_buyer_maker", BooleanType(), True)
])

# Schema for raw_klines topic (OHLCV candlesticks)
RAW_KLINES_SCHEMA = StructType([
    StructField("symbol", StringType(), False),
    StructField("interval", StringType(), False),
    StructField("open_time", LongType(), False),
    StructField("close_time", LongType(), False),
    StructField("open", DoubleType(), False),
    StructField("high", DoubleType(), False),
    StructField("low", DoubleType(), False),
    StructField("close", DoubleType(), False),
    StructField("volume", DoubleType(), False),
    StructField("quote_volume", DoubleType(), True),
    StructField("trades_count", LongType(), True)
])


def create_kafka_stream(
    spark: SparkSession,
    topic: str,
    starting_offsets: str = None
) -> DataFrame:
    """
    Creates a streaming DataFrame from a Kafka topic
    
    Args:
        spark: SparkSession instance
        topic: Kafka topic name
        starting_offsets: 'earliest' or 'latest' (default from config)
    
    Returns:
        Streaming DataFrame with raw Kafka data
    """
    starting_offsets = starting_offsets or kafka_config.starting_offsets
    
    logger.info(f"Creating Kafka stream for topic: {topic}")
    logger.info(f"  - Bootstrap servers: {kafka_config.bootstrap_servers}")
    logger.info(f"  - Starting offsets: {starting_offsets}")
    
    kafka_df = (spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_config.bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("maxOffsetsPerTrigger", kafka_config.max_offsets_per_trigger)
        .option("failOnDataLoss", "false")
        .load()
    )
    
    logger.info(f"✅ Kafka stream created for topic: {topic}")
    return kafka_df


def parse_raw_trades(kafka_df: DataFrame) -> DataFrame:
    """
    Parses raw_trades Kafka messages into structured DataFrame
    
    Args:
        kafka_df: Raw Kafka DataFrame
    
    Returns:
        Parsed DataFrame with trades data
    """
    logger.info("Parsing raw_trades messages...")
    
    # Parse JSON from Kafka value
    parsed_df = (kafka_df
        .selectExpr("CAST(value AS STRING) as json_value")
        .select(from_json(col("json_value"), RAW_TRADES_SCHEMA).alias("data"))
        .select("data.*")
    )
    
    # Convert timestamp from milliseconds to timestamp type
    trades_df = parsed_df.withColumn(
        "timestamp",
        to_timestamp(col("timestamp") / 1000)
    )
    
    logger.info("✅ raw_trades parsed successfully")
    return trades_df


def parse_raw_klines(kafka_df: DataFrame) -> DataFrame:
    """
    Parses raw_klines (OHLCV) Kafka messages into structured DataFrame
    
    Args:
        kafka_df: Raw Kafka DataFrame
    
    Returns:
        Parsed DataFrame with OHLCV data
    """
    logger.info("Parsing raw_klines messages...")
    
    # Parse JSON from Kafka value
    parsed_df = (kafka_df
        .selectExpr("CAST(value AS STRING) as json_value")
        .select(from_json(col("json_value"), RAW_KLINES_SCHEMA).alias("data"))
        .select("data.*")
    )
    
    # Convert timestamps from milliseconds to timestamp type
    klines_df = (parsed_df
        .withColumn("open_time", to_timestamp(col("open_time") / 1000))
        .withColumn("close_time", to_timestamp(col("close_time") / 1000))
        .withColumnRenamed("open_time", "timestamp")
        .drop("close_time")
    )
    
    logger.info("✅ raw_klines parsed successfully")
    return klines_df


def add_watermark(df: DataFrame, timestamp_col: str = "timestamp", delay: str = "1 minute") -> DataFrame:
    """
    Adds watermark to streaming DataFrame for handling late data
    
    Args:
        df: Streaming DataFrame
        timestamp_col: Name of timestamp column
        delay: Watermark delay (e.g., '1 minute', '30 seconds')
    
    Returns:
        DataFrame with watermark
    """
    logger.info(f"Adding watermark: {delay} on column '{timestamp_col}'")
    return df.withWatermark(timestamp_col, delay)


def consume_trades_stream(spark: SparkSession, with_watermark: bool = True) -> DataFrame:
    """
    High-level function to consume and parse trades stream
    
    Args:
        spark: SparkSession instance
        with_watermark: Whether to add watermark for late data handling
    
    Returns:
        Parsed trades DataFrame ready for processing
    """
    logger.info("Starting trades stream consumption...")
    
    # Create Kafka stream
    kafka_df = create_kafka_stream(spark, kafka_config.raw_trades_topic)
    
    # Parse trades
    trades_df = parse_raw_trades(kafka_df)
    
    # Add watermark if requested
    if with_watermark:
        trades_df = add_watermark(trades_df)
    
    logger.info("✅ Trades stream ready for processing")
    return trades_df


def consume_klines_stream(spark: SparkSession, with_watermark: bool = True) -> DataFrame:
    """
    High-level function to consume and parse klines (OHLCV) stream
    
    Args:
        spark: SparkSession instance
        with_watermark: Whether to add watermark for late data handling
    
    Returns:
        Parsed klines DataFrame ready for processing
    """
    logger.info("Starting klines stream consumption...")
    
    # Create Kafka stream
    kafka_df = create_kafka_stream(spark, kafka_config.raw_klines_topic)
    
    # Parse klines
    klines_df = parse_raw_klines(kafka_df)
    
    # Add watermark if requested
    if with_watermark:
        klines_df = add_watermark(klines_df)
    
    logger.info("✅ Klines stream ready for processing")
    return klines_df


def aggregate_trades_to_ohlcv(
    trades_df: DataFrame,
    window_duration: str = "1 minute",
    slide_duration: str = None
) -> DataFrame:
    """
    Aggregates raw trades into OHLCV candlesticks using windowing
    
    Args:
        trades_df: Parsed trades DataFrame
        window_duration: Window size (e.g., '1 minute', '5 minutes')
        slide_duration: Slide interval (default: same as window_duration)
    
    Returns:
        OHLCV DataFrame
    """
    slide_duration = slide_duration or window_duration
    
    logger.info(f"Aggregating trades to OHLCV (window: {window_duration})")
    
    ohlcv_df = (trades_df
        .groupBy(
            window(col("timestamp"), window_duration, slide_duration),
            col("symbol")
        )
        .agg(
            expr("first(price)").alias("open"),
            expr("max(price)").alias("high"),
            expr("min(price)").alias("low"),
            expr("last(price)").alias("last"),
            expr("sum(quantity)").alias("volume"),
            expr("count(*)").alias("trades_count")
        )
        .select(
            col("symbol"),
            col("window.start").alias("timestamp"),
            col("open"),
            col("high"),
            col("low"),
            col("last").alias("close"),
            col("volume"),
            col("trades_count")
        )
    )
    
    logger.info("✅ OHLCV aggregation complete")
    return ohlcv_df

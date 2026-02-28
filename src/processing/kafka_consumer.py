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
try:
    from .config import kafka_config
except ImportError:
    from config import kafka_config

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


# ============================================
# WebSocket-wrapped schemas (from binance-streaming container)
# Format: {"stream": "...", "data": {"e": "aggTrade", ...}}
# ============================================

# Schema for WebSocket-wrapped trades
WS_TRADE_DATA_SCHEMA = StructType([
    StructField("e", StringType(), True),     # event type
    StructField("E", LongType(), True),       # event time
    StructField("s", StringType(), True),     # symbol
    StructField("a", LongType(), True),       # aggregate trade id
    StructField("p", StringType(), True),     # price
    StructField("q", StringType(), True),     # quantity
    StructField("T", LongType(), True),       # trade time
    StructField("m", BooleanType(), True),    # is buyer maker
])

WS_TRADE_SCHEMA = StructType([
    StructField("stream", StringType(), True),
    StructField("data", WS_TRADE_DATA_SCHEMA, True),
])

# Schema for WebSocket-wrapped klines
WS_KLINE_K_SCHEMA = StructType([
    StructField("t", LongType(), True),       # kline start time
    StructField("T", LongType(), True),       # kline close time
    StructField("s", StringType(), True),     # symbol
    StructField("i", StringType(), True),     # interval
    StructField("o", StringType(), True),     # open
    StructField("c", StringType(), True),     # close
    StructField("h", StringType(), True),     # high
    StructField("l", StringType(), True),     # low
    StructField("v", StringType(), True),     # volume
    StructField("n", LongType(), True),       # number of trades
    StructField("q", StringType(), True),     # quote volume
])

WS_KLINE_DATA_SCHEMA = StructType([
    StructField("e", StringType(), True),
    StructField("E", LongType(), True),
    StructField("s", StringType(), True),
    StructField("k", WS_KLINE_K_SCHEMA, True),
])

WS_KLINE_SCHEMA = StructType([
    StructField("stream", StringType(), True),
    StructField("data", WS_KLINE_DATA_SCHEMA, True),
])


def parse_raw_trades(kafka_df: DataFrame) -> DataFrame:
    """
    Parses raw_trades Kafka messages into structured DataFrame.
    Handles both flat format (REST API) and WebSocket-wrapped format.
    
    Args:
        kafka_df: Raw Kafka DataFrame
    
    Returns:
        Parsed DataFrame with trades data
    """
    from pyspark.sql.functions import coalesce
    
    logger.info("Parsing raw_trades messages (flat + WebSocket formats)...")
    
    raw_str = kafka_df.selectExpr("CAST(value AS STRING) as json_value")
    
    # Try flat schema first
    flat = raw_str.select(from_json(col("json_value"), RAW_TRADES_SCHEMA).alias("flat"))
    
    # Try WebSocket schema
    ws = raw_str.select(from_json(col("json_value"), WS_TRADE_SCHEMA).alias("ws"))
    
    # Combine: use flat fields, fallback to WebSocket fields
    combined = raw_str.select(
        col("json_value"),
        from_json(col("json_value"), RAW_TRADES_SCHEMA).alias("flat"),
        from_json(col("json_value"), WS_TRADE_SCHEMA).alias("ws"),
    ).select(
        coalesce(col("flat.symbol"), col("ws.data.s")).alias("symbol"),
        coalesce(col("flat.price"), col("ws.data.p").cast("double")).alias("price"),
        coalesce(col("flat.quantity"), col("ws.data.q").cast("double")).alias("quantity"),
        coalesce(col("flat.timestamp"), col("ws.data.T")).alias("timestamp"),
        coalesce(col("flat.trade_id"), col("ws.data.a")).alias("trade_id"),
        coalesce(col("flat.is_buyer_maker"), col("ws.data.m")).alias("is_buyer_maker"),
    )
    
    # Convert timestamp from milliseconds to timestamp type
    trades_df = combined.withColumn(
        "timestamp",
        to_timestamp(col("timestamp") / 1000)
    )
    
    logger.info("✅ raw_trades parsed successfully")
    return trades_df


def parse_raw_klines(kafka_df: DataFrame) -> DataFrame:
    """
    Parses raw_klines (OHLCV) Kafka messages into structured DataFrame.
    Handles both flat format (REST API) and WebSocket-wrapped format.
    
    Uses get_json_object for WebSocket fields to avoid case-insensitive
    field name collisions (e.g. 't' vs 'T' in the kline sub-object).
    
    Args:
        kafka_df: Raw Kafka DataFrame
    
    Returns:
        Parsed DataFrame with OHLCV data
    """
    from pyspark.sql.functions import coalesce, get_json_object
    
    logger.info("Parsing raw_klines messages (flat + WebSocket formats)...")
    
    raw_str = kafka_df.selectExpr("CAST(value AS STRING) as json_value")
    
    # Parse both formats from the same raw_str to avoid row-id join issues
    # Use get_json_object for WebSocket: case-sensitive JSON path extraction
    combined = raw_str.select(
        col("json_value"),
        from_json(col("json_value"), RAW_KLINES_SCHEMA).alias("flat"),
    ).select(
        # Flat format fields
        col("flat.symbol").alias("flat_symbol"),
        col("flat.interval").alias("flat_interval"),
        col("flat.open_time").alias("flat_open_time"),
        col("flat.close_time").alias("flat_close_time"),
        col("flat.open").alias("flat_open"),
        col("flat.high").alias("flat_high"),
        col("flat.low").alias("flat_low"),
        col("flat.close").alias("flat_close"),
        col("flat.volume").alias("flat_volume"),
        col("flat.quote_volume").alias("flat_quote_volume"),
        col("flat.trades_count").alias("flat_trades_count"),
        # WebSocket format fields via JSON path (case-sensitive, avoids 't'/'T' clash)
        get_json_object(col("json_value"), "$.data.k.s").alias("ws_symbol"),
        get_json_object(col("json_value"), "$.data.k.i").alias("ws_interval"),
        get_json_object(col("json_value"), "$.data.k.t").alias("ws_open_time"),
        get_json_object(col("json_value"), "$.data.k.T").alias("ws_close_time"),
        get_json_object(col("json_value"), "$.data.k.o").alias("ws_open"),
        get_json_object(col("json_value"), "$.data.k.h").alias("ws_high"),
        get_json_object(col("json_value"), "$.data.k.l").alias("ws_low"),
        get_json_object(col("json_value"), "$.data.k.c").alias("ws_close"),
        get_json_object(col("json_value"), "$.data.k.v").alias("ws_volume"),
        get_json_object(col("json_value"), "$.data.k.q").alias("ws_quote_volume"),
        get_json_object(col("json_value"), "$.data.k.n").alias("ws_trades_count"),
    )
    
    # Combine: flat wins, WebSocket fallback
    merged = combined.select(
        coalesce(col("flat_symbol"), col("ws_symbol")).alias("symbol"),
        coalesce(col("flat_interval"), col("ws_interval")).alias("interval"),
        coalesce(col("flat_open_time"), col("ws_open_time").cast("long")).alias("open_time"),
        coalesce(col("flat_close_time"), col("ws_close_time").cast("long")).alias("close_time"),
        coalesce(col("flat_open"), col("ws_open").cast("double")).alias("open"),
        coalesce(col("flat_high"), col("ws_high").cast("double")).alias("high"),
        coalesce(col("flat_low"), col("ws_low").cast("double")).alias("low"),
        coalesce(col("flat_close"), col("ws_close").cast("double")).alias("close"),
        coalesce(col("flat_volume"), col("ws_volume").cast("double")).alias("volume"),
        coalesce(col("flat_quote_volume"), col("ws_quote_volume").cast("double")).alias("quote_volume"),
        coalesce(col("flat_trades_count"), col("ws_trades_count").cast("long")).alias("trades_count"),
    )
    
    # Convert timestamps from milliseconds to timestamp type
    klines_df = (merged
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

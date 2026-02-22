"""
Kafka Consumer avec Spark Structured Streaming
"""
import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from config import KafkaConfig
from data_cleaning import RAW_OHLCV_SCHEMA, RAW_TRADES_SCHEMA

logger = logging.getLogger(__name__)


def create_kafka_stream(
    spark: SparkSession,
    config: KafkaConfig,
    topic: str,
    schema: StructType,
) -> DataFrame:
    """
    Crée un DataFrame Spark Structured Streaming depuis un topic Kafka.

    Args:
        spark: SparkSession active
        config: Configuration Kafka
        topic: Nom du topic Kafka
        schema: Schéma JSON attendu dans le message

    Returns:
        DataFrame streaming avec les données parsées
    """
    logger.info(f"[Kafka] Connexion au topic: {topic}")

    # Lecture du flux Kafka
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config.bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", config.starting_offsets)
        .option("maxOffsetsPerTrigger", config.max_offsets_per_trigger)
        .option("failOnDataLoss", str(config.failOnDataLoss).lower())
        .load()
    )

    # Parser le JSON depuis la colonne `value` (bytes → string → JSON)
    parsed_stream = (
        raw_stream
        .select(
            F.cast("string", F.col("value")).alias("json_value"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("partition"),
            F.col("offset"),
        )
        .select(
            F.from_json(F.col("json_value"), schema).alias("data"),
            F.col("kafka_timestamp"),
        )
        .select("data.*", "kafka_timestamp")
    )

    # Watermark pour la gestion du late data (streaming)
    if "timestamp" in [f.name for f in schema.fields]:
        parsed_stream = parsed_stream.withWatermark(
            "kafka_timestamp", "1 minute"
        )

    logger.info(f"[Kafka] Stream créé pour le topic: {topic}")
    return parsed_stream


def get_klines_stream(spark: SparkSession, config: KafkaConfig) -> DataFrame:
    """Stream des bougies OHLCV depuis le topic raw_klines"""
    return create_kafka_stream(spark, config, config.raw_klines_topic, RAW_OHLCV_SCHEMA)


def get_trades_stream(spark: SparkSession, config: KafkaConfig) -> DataFrame:
    """Stream des trades bruts depuis le topic raw_trades"""
    return create_kafka_stream(spark, config, config.raw_trades_topic, RAW_TRADES_SCHEMA)


def aggregate_trades_to_ohlcv(df: DataFrame, window_duration: str = "1 minute") -> DataFrame:
    """
    Agrège les trades bruts en bougies OHLCV via window functions streaming.

    Args:
        df: DataFrame des trades (symbol, timestamp, price, quantity)
        window_duration: Durée de la fenêtre (default: 1 minute)

    Returns:
        DataFrame OHLCV agrégé
    """
    return (
        df
        .withColumn("event_time", F.to_timestamp(F.col("timestamp") / 1000))
        .withWatermark("event_time", "30 seconds")
        .groupBy(
            F.col("symbol"),
            F.window(F.col("event_time"), window_duration).alias("time_window"),
        )
        .agg(
            F.first("price").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.last("price").alias("close"),
            F.sum("quantity").alias("volume"),
            F.count("*").alias("trades"),
        )
        .select(
            F.col("symbol"),
            F.lit("1m").alias("interval"),
            F.unix_timestamp(F.col("time_window.start")).cast("long").alias("timestamp") * 1000,
            F.col("open"),
            F.col("high"),
            F.col("low"),
            F.col("close"),
            F.col("volume"),
            F.col("trades"),
        )
    )

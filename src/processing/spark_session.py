"""
Gestion de la SparkSession pour le pipeline ETL
"""
import logging
from pyspark.sql import SparkSession
from config import SparkConfig

logger = logging.getLogger(__name__)


def create_spark_session(config: SparkConfig) -> SparkSession:
    """
    Crée et configure une SparkSession optimisée pour le streaming Kafka → MongoDB.

    Args:
        config: Configuration Spark

    Returns:
        SparkSession configurée
    """
    logger.info(f"Création de la SparkSession: {config.app_name}")

    builder = (
        SparkSession.builder
        .appName(config.app_name)
        .master(config.master)
        # Mémoire
        .config("spark.driver.memory", config.driver_memory)
        .config("spark.executor.memory", config.executor_memory)
        # Packages Maven (Kafka + MongoDB)
        .config("spark.jars.packages", config.packages)
        # Optimisations SQL
        .config("spark.sql.shuffle.partitions", str(config.shuffle_partitions))
        .config("spark.sql.adaptive.enabled", str(config.adaptive_enabled).lower())
        .config("spark.sql.adaptive.coalescePartitions.enabled", str(config.adaptive_coalesce).lower())
        # Streaming
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    logger.info(f"SparkSession créée - version: {spark.version}")
    return spark


def stop_spark_session(spark: SparkSession) -> None:
    """Arrête proprement la SparkSession"""
    if spark and not spark._jvm.SparkContext.getOrCreate().isStopped():
        logger.info("Arrêt de la SparkSession...")
        spark.stop()
        logger.info("SparkSession arrêtée.")

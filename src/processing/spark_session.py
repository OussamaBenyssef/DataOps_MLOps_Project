"""
Spark Session Module - P3
Creates and configures SparkSession with Kafka and MongoDB support
"""

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import logging

try:
    from .config import spark_config
except ImportError:
    from config import spark_config

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def create_spark_session(app_name: str = None, master: str = None) -> SparkSession:
    """
    Creates and configures a SparkSession with optimized settings for streaming

    Args:
        app_name: Application name (default from config)
        master: Spark master URL (default from config)

    Returns:
        Configured SparkSession instance
    """
    app_name = app_name or spark_config.app_name
    master = master or spark_config.master

    logger.info(f"Creating SparkSession: {app_name} on {master}")

    # Create Spark configuration
    conf = SparkConf()
    conf.setAppName(app_name)
    conf.setMaster(master)

    # Memory configuration
    conf.set("spark.driver.memory", spark_config.driver_memory)
    conf.set("spark.executor.memory", spark_config.executor_memory)
    conf.set("spark.executor.cores", str(spark_config.executor_cores))

    # Streaming configuration
    conf.set("spark.sql.streaming.checkpointLocation", spark_config.checkpoint_location)
    conf.set("spark.sql.streaming.schemaInference", "true")

    # Shuffle partitions (optimized for streaming)
    conf.set("spark.sql.shuffle.partitions", "10")

    # Adaptive query execution
    conf.set("spark.sql.adaptive.enabled", "true")
    conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

    # Serialization
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    # UI configuration
    conf.set("spark.ui.enabled", "true")
    conf.set("spark.ui.port", "4040")

    # Build SparkSession with packages
    spark = (
        SparkSession.builder.config(conf=conf).config("spark.jars.packages", spark_config.packages_str).getOrCreate()
    )

    # Set log level
    spark.sparkContext.setLogLevel("WARN")

    logger.info("✅ SparkSession created successfully")
    logger.info(f"   - App Name: {app_name}")
    logger.info(f"   - Master: {master}")
    logger.info(f"   - Driver Memory: {spark_config.driver_memory}")
    logger.info(f"   - Executor Memory: {spark_config.executor_memory}")
    logger.info(f"   - Checkpoint Location: {spark_config.checkpoint_location}")
    logger.info("   - Spark UI: http://localhost:4040")

    return spark


def stop_spark_session(spark: SparkSession):
    """
    Gracefully stops the SparkSession

    Args:
        spark: SparkSession to stop
    """
    if spark:
        logger.info("Stopping SparkSession...")
        spark.stop()
        logger.info("✅ SparkSession stopped")


def get_spark_context(spark: SparkSession):
    """
    Returns the SparkContext from a SparkSession

    Args:
        spark: SparkSession instance

    Returns:
        SparkContext instance
    """
    return spark.sparkContext


def configure_checkpoint_location(location: str):
    """
    Updates the checkpoint location configuration

    Args:
        location: New checkpoint directory path
    """
    spark_config.checkpoint_location = location
    logger.info(f"Checkpoint location updated to: {location}")

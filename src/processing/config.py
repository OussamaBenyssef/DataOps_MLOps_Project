"""
Configuration Module - Spark Processing P3
Centralizes all configuration parameters for Kafka, MongoDB, and Spark
"""

import os
from dataclasses import dataclass
from typing import List


@dataclass
class KafkaConfig:
    """Kafka connection configuration"""

    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    topics: List[str] = None
    consumer_group_id: str = "spark-crypto-consumer"

    # Topics
    raw_trades_topic: str = "raw_trades"
    raw_klines_topic: str = "raw_klines"
    processed_data_topic: str = "processed_data"
    anomalies_topic: str = "anomalies"

    # Kafka options
    starting_offsets: str = "latest"  # 'earliest' or 'latest'
    max_offsets_per_trigger: int = 10000

    def __post_init__(self):
        if self.topics is None:
            self.topics = [self.raw_trades_topic, self.raw_klines_topic]


@dataclass
class MongoDBConfig:
    """MongoDB connection configuration"""

    uri: str = os.getenv(
        "MONGODB_URI", "mongodb://datamlops:datamlops123@localhost:27017/cryptomarket?authSource=admin"
    )
    database: str = "cryptomarket"

    # Collections
    raw_trades_collection: str = "raw_trades"
    ohlcv_collection: str = "ohlcv"
    indicators_collection: str = "indicators"
    anomalies_collection: str = "anomalies"
    predictions_collection: str = "predictions"
    daily_metrics_collection: str = "aggregated_metrics"

    # Write options
    batch_size: int = 1000
    write_mode: str = "append"  # 'append' or 'overwrite'


@dataclass
class SparkConfig:
    """Spark session configuration"""

    app_name: str = os.getenv("SPARK_APP_NAME", "CryptoETL_P3")
    master: str = os.getenv("SPARK_MASTER", "local[*]")

    # Memory configuration
    driver_memory: str = "2g"
    executor_memory: str = "2g"
    executor_cores: int = 2

    # Checkpoint configuration
    checkpoint_location: str = os.getenv("SPARK_CHECKPOINT_DIR", "/tmp/spark-checkpoints/crypto-etl")

    # Streaming configuration
    trigger_interval: str = "10 seconds"  # Processing trigger interval
    watermark_delay: str = "1 minute"  # Late data tolerance

    # Packages (Maven coordinates)
    packages: List[str] = None

    def __post_init__(self):
        if self.packages is None:
            self.packages = [
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
                "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0",
            ]

    @property
    def packages_str(self) -> str:
        """Returns packages as comma-separated string"""
        return ",".join(self.packages)


@dataclass
class CleaningConfig:
    """Configuration pour le nettoyage des données"""

    # Colonnes critiques (lignes supprimées si null)
    required_columns: List[str] = None

    # Seuil Z-score pour détection d'outliers
    zscore_threshold: float = 3.0

    # Seuil de variation de prix pour détecter les spikes (5%)
    price_spike_threshold: float = 0.05

    def __post_init__(self):
        if self.required_columns is None:
            self.required_columns = ["symbol", "interval", "timestamp", "open", "high", "low", "close", "volume"]


@dataclass
class TechnicalIndicatorsConfig:
    """Configuration pour les indicateurs techniques"""

    rsi_period: int = 14
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9
    bollinger_period: int = 20
    bollinger_std: float = 2.0
    sma_periods: List[int] = None
    ema_periods: List[int] = None

    def __post_init__(self):
        if self.sma_periods is None:
            self.sma_periods = [20, 50, 200]
        if self.ema_periods is None:
            self.ema_periods = [12, 26]


@dataclass
class ProcessingConfig:
    """Data processing configuration"""

    # Symbols to process
    symbols: List[str] = None

    # OHLCV intervals
    intervals: List[str] = None

    # Technical indicators parameters
    rsi_period: int = 14
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9
    bollinger_period: int = 20
    bollinger_std: float = 2.0
    sma_periods: List[int] = None
    ema_periods: List[int] = None

    # Anomaly detection thresholds
    price_spike_threshold: float = 0.05  # 5% price change
    volume_spike_threshold: float = 3.0  # 3x average volume

    def __post_init__(self):
        if self.symbols is None:
            self.symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]

        if self.intervals is None:
            self.intervals = ["1m", "5m", "15m", "1h", "4h", "1d"]

        if self.sma_periods is None:
            self.sma_periods = [20, 50, 200]

        if self.ema_periods is None:
            self.ema_periods = [12, 26]


# Global configuration instances
kafka_config = KafkaConfig()
mongodb_config = MongoDBConfig()
spark_config = SparkConfig()
processing_config = ProcessingConfig()
cleaning_config = CleaningConfig()
indicators_config = TechnicalIndicatorsConfig()


def get_all_config():
    """Returns all configuration objects as a dictionary"""
    return {"kafka": kafka_config, "mongodb": mongodb_config, "spark": spark_config, "processing": processing_config}

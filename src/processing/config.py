"""
Configuration centralisée pour le pipeline ETL Spark
"""
from dataclasses import dataclass, field
from typing import List, Dict, Optional
import os


@dataclass
class KafkaConfig:
    """Configuration Kafka pour le consumer Spark Structured Streaming"""
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    raw_trades_topic: str = "raw_trades"
    raw_klines_topic: str = "raw_klines"
    consumer_group: str = "spark-etl-consumer"
    starting_offsets: str = "latest"
    max_offsets_per_trigger: int = 10000
    failOnDataLoss: bool = False


@dataclass
class MongoDBConfig:
    """Configuration MongoDB pour la persistance des données traitées"""
    uri: str = os.getenv("MONGODB_URI", "mongodb://admin:password@localhost:27017/crypto_data?authSource=admin")
    database: str = "crypto_data"
    # Collections
    raw_trades_collection: str = "raw_trades"
    ohlcv_collection: str = "ohlcv"
    indicators_collection: str = "indicators"
    anomalies_collection: str = "anomalies"
    predictions_collection: str = "predictions"
    # Write options
    write_concern: str = "majority"
    ordered: bool = False


@dataclass
class SparkConfig:
    """Configuration Apache Spark"""
    app_name: str = "CryptoETL-P3"
    master: str = "local[*]"
    driver_memory: str = "2g"
    executor_memory: str = "2g"
    shuffle_partitions: int = 10
    checkpoint_location: str = "/tmp/spark_checkpoints"
    # Packages Maven pour Kafka + MongoDB
    packages: str = (
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,"
        "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0"
    )
    # Adaptive Query Execution
    adaptive_enabled: bool = True
    adaptive_coalesce: bool = True


@dataclass
class CleaningConfig:
    """Configuration pour le nettoyage des données"""
    # Seuils de validation des prix
    min_price: float = 0.0
    max_price: float = 1_000_000.0
    # Seuils de validation des volumes
    min_volume: float = 0.0
    max_volume: float = 1_000_000_000.0
    # Détection outliers via Z-score
    zscore_threshold: float = 3.0
    # Détection price spikes (% de variation)
    price_spike_threshold: float = 0.05  # 5%
    # Colonnes requises pour les données OHLCV
    required_ohlcv_columns: List[str] = field(default_factory=lambda: [
        "symbol", "timestamp", "open", "high", "low", "close", "volume"
    ])
    # Colonnes requises pour les trades raw
    required_trade_columns: List[str] = field(default_factory=lambda: [
        "symbol", "timestamp", "price", "quantity"
    ])


@dataclass
class TechnicalIndicatorsConfig:
    """Configuration pour le calcul des indicateurs techniques"""
    # SMA periods
    sma_periods: List[int] = field(default_factory=lambda: [20, 50, 200])
    # EMA periods
    ema_periods: List[int] = field(default_factory=lambda: [12, 26])
    # RSI
    rsi_period: int = 14
    rsi_overbought: float = 70.0
    rsi_oversold: float = 30.0
    # MACD
    macd_fast_period: int = 12
    macd_slow_period: int = 26
    macd_signal_period: int = 9
    # Bollinger Bands
    bollinger_period: int = 20
    bollinger_std_dev: float = 2.0
    # Symboles à traiter
    symbols: List[str] = field(default_factory=lambda: [
        "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "ADAUSDT"
    ])
    # Intervalles OHLCV
    intervals: List[str] = field(default_factory=lambda: ["1m", "5m", "15m", "1h", "4h", "1d"])


@dataclass
class ProcessingConfig:
    """Configuration globale du pipeline de traitement"""
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    mongodb: MongoDBConfig = field(default_factory=MongoDBConfig)
    spark: SparkConfig = field(default_factory=SparkConfig)
    cleaning: CleaningConfig = field(default_factory=CleaningConfig)
    indicators: TechnicalIndicatorsConfig = field(default_factory=TechnicalIndicatorsConfig)

    @classmethod
    def from_env(cls) -> "ProcessingConfig":
        """Crée la configuration depuis les variables d'environnement"""
        return cls(
            kafka=KafkaConfig(),
            mongodb=MongoDBConfig(),
            spark=SparkConfig(),
            cleaning=CleaningConfig(),
            indicators=TechnicalIndicatorsConfig(),
        )

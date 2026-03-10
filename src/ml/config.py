"""
ML Configuration Module - P4 (Abdessamad)
Centralized configuration for ML pipeline: MongoDB, feature engineering, model parameters
"""

import os
from dataclasses import dataclass, field
from typing import List


@dataclass
class MongoDBConfig:
    """MongoDB connection configuration for ML module"""

    uri: str = os.getenv(
        "MONGODB_URI", "mongodb://datamlops:datamlops123@localhost:27017/cryptomarket?authSource=admin"
    )
    database: str = "cryptomarket"

    # Source collections (read from P3 output)
    ohlcv_collection: str = "ohlcv"
    indicators_collection: str = "indicators"

    # Target collections (written by P4)
    predictions_collection: str = "predictions"
    anomalies_collection: str = "anomalies"


@dataclass
class FeatureConfig:
    """Feature engineering parameters"""

    # Lag periods for autoregressive features
    lag_periods: List[int] = field(default_factory=lambda: [1, 3, 5, 10])

    # Rolling window sizes for statistical features
    rolling_windows: List[int] = field(default_factory=lambda: [5, 10, 20])

    # Return periods for momentum features
    return_periods: List[int] = field(default_factory=lambda: [1, 3, 5])

    # RSI zone thresholds
    rsi_oversold: float = 30.0
    rsi_overbought: float = 70.0

    # Target variable config
    prediction_horizon: int = 5  # Predict N candles ahead
    anomaly_return_threshold: float = 0.03  # 3% return → anomaly

    # Columns that are metadata (excluded from feature matrix)
    metadata_columns: List[str] = field(default_factory=lambda: ["symbol", "interval", "timestamp", "_id"])

    # Target column names
    target_columns: List[str] = field(
        default_factory=lambda: ["target_direction", "target_return_pct", "anomaly_label"]
    )


@dataclass
class AnomalyConfig:
    """Anomaly detection model parameters"""

    # Isolation Forest
    contamination: float = 0.05  # Expected anomaly ratio
    n_estimators: int = 100  # Number of trees

    # Autoencoder
    autoencoder_epochs: int = 50
    autoencoder_batch_size: int = 32
    autoencoder_latent_dim: int = 8  # Bottleneck dimension
    reconstruction_threshold_percentile: float = 95.0  # Percentile cutoff

    # MLflow
    experiment_name: str = "crypto-anomaly-detection"


@dataclass
class ModelConfig:
    """Model training and serving configuration"""

    mlflow_tracking_uri: str = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5001")
    experiment_name: str = "crypto-price-prediction"
    model_name: str = "crypto-predictor"

    # Default training symbols
    symbols: List[str] = field(default_factory=lambda: ["BTCUSDT", "ETHUSDT", "BNBUSDT"])
    default_interval: str = "1m"
    default_data_limit: int = 5000


# Global configuration instances
mongodb_config = MongoDBConfig()
feature_config = FeatureConfig()
anomaly_config = AnomalyConfig()
model_config = ModelConfig()

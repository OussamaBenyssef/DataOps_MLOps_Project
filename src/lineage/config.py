"""
Lineage Configuration - P5
Centralized URN definitions and DataHub connection settings
for the Crypto MLOps data lineage graph.
"""

import os
from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class DataHubConfig:
    """DataHub GMS connection settings."""

    gms_server: str = os.getenv("DATAHUB_GMS_URL", "http://localhost:8082")
    environment: str = "PROD"


@dataclass
class PlatformConfig:
    """Platform identifiers used in DataHub URNs."""

    kafka: str = "kafka"
    spark: str = "spark"
    mongodb: str = "mongodb"
    ml: str = "mlflow"  # ML models platform


@dataclass
class LineageConfig:
    """
    Complete lineage configuration.

    URN format: urn:li:dataset:(urn:li:dataPlatform:<platform>,<name>,<env>)

    Pipeline flow:
        Kafka topics → Spark ETL jobs → MongoDB collections → ML models
    """

    datahub: DataHubConfig = field(default_factory=DataHubConfig)
    platforms: PlatformConfig = field(default_factory=PlatformConfig)

    # ── Kafka Topics ──────────────────────────────────────────────
    kafka_topics: List[str] = field(
        default_factory=lambda: [
            "raw_trades",
            "raw_klines",
            "processed_data",
            "anomalies",
        ]
    )

    # ── Spark Jobs (logical datasets) ─────────────────────────────
    spark_jobs: Dict[str, str] = field(
        default_factory=lambda: {
            "trades_cleaning": "crypto_pipeline.trades_cleaning",
            "ohlcv_processing": "crypto_pipeline.ohlcv_processing",
            "indicators_calc": "crypto_pipeline.indicators_calculation",
            "metrics_aggregation": "crypto_pipeline.metrics_aggregation",
        }
    )

    # ── MongoDB Collections ───────────────────────────────────────
    mongodb_collections: List[str] = field(
        default_factory=lambda: [
            "cryptomarket.raw_trades",
            "cryptomarket.ohlcv",
            "cryptomarket.indicators",
            "cryptomarket.anomalies",
            "cryptomarket.predictions",
        ]
    )

    # ── ML Datasets (logical) ─────────────────────────────────────
    ml_datasets: Dict[str, str] = field(
        default_factory=lambda: {
            "feature_engineering": "crypto_ml.feature_engineering",
            "xgboost_predictor": "crypto_ml.xgboost_predictor",
            "anomaly_detector": "crypto_ml.anomaly_detector",
        }
    )


# Global instance
lineage_config = LineageConfig()

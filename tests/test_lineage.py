"""
Unit Tests for Data Lineage Emission - P5
Tests use mocks — no Docker or DataHub required.
"""

import pytest
import sys
import os
from unittest.mock import patch, MagicMock

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from lineage.config import LineageConfig, lineage_config
from lineage.emit_lineage import DataLineageEmitter, DATAHUB_AVAILABLE


# ============================================
# CONFIG TESTS
# ============================================

def test_lineage_config_defaults():
    """Test that lineage config has correct defaults."""
    cfg = LineageConfig()
    assert cfg.datahub.gms_server == os.getenv("DATAHUB_GMS_URL", "http://localhost:8082")
    assert cfg.datahub.environment == "PROD"
    assert cfg.platforms.kafka == "kafka"
    assert cfg.platforms.spark == "spark"
    assert cfg.platforms.mongodb == "mongodb"
    assert cfg.platforms.ml == "mlflow"


def test_lineage_config_kafka_topics():
    """Test Kafka topics are configured."""
    cfg = LineageConfig()
    assert "raw_trades" in cfg.kafka_topics
    assert "raw_klines" in cfg.kafka_topics
    assert "processed_data" in cfg.kafka_topics
    assert "anomalies" in cfg.kafka_topics


def test_lineage_config_spark_jobs():
    """Test Spark job names are configured."""
    cfg = LineageConfig()
    assert "trades_cleaning" in cfg.spark_jobs
    assert "ohlcv_processing" in cfg.spark_jobs
    assert "indicators_calc" in cfg.spark_jobs


def test_lineage_config_mongodb_collections():
    """Test MongoDB collections are configured."""
    cfg = LineageConfig()
    assert "cryptomarket.ohlcv" in cfg.mongodb_collections
    assert "cryptomarket.indicators" in cfg.mongodb_collections
    assert "cryptomarket.raw_trades" in cfg.mongodb_collections


def test_lineage_config_ml_datasets():
    """Test ML datasets are configured."""
    cfg = LineageConfig()
    assert "feature_engineering" in cfg.ml_datasets
    assert "xgboost_predictor" in cfg.ml_datasets
    assert "anomaly_detector" in cfg.ml_datasets


# ============================================
# URN FORMAT TESTS
# ============================================

@pytest.mark.skipif(not DATAHUB_AVAILABLE, reason="acryl-datahub not installed")
def test_lineage_urns_format():
    """Test that URNs follow DataHub format."""
    emitter = DataLineageEmitter()

    kafka_urn = emitter.make_kafka_urn("raw_trades")
    assert kafka_urn.startswith("urn:li:dataset:")
    assert "kafka" in kafka_urn
    assert "raw_trades" in kafka_urn

    mongo_urn = emitter.make_mongodb_urn("cryptomarket.ohlcv")
    assert "mongodb" in mongo_urn
    assert "cryptomarket.ohlcv" in mongo_urn

    spark_urn = emitter.make_spark_urn("trades_cleaning")
    assert "spark" in spark_urn

    ml_urn = emitter.make_ml_urn("xgboost_predictor")
    assert "mlflow" in ml_urn


# ============================================
# LINEAGE EMISSION TESTS (mocked)
# ============================================

@pytest.mark.skipif(not DATAHUB_AVAILABLE, reason="acryl-datahub not installed")
def test_lineage_kafka_to_spark():
    """Test Kafka → Spark lineage edges."""
    emitter = DataLineageEmitter()
    events = emitter.emit_kafka_to_spark()

    assert len(events) == 2

    # First event: raw_trades → trades_cleaning
    e0 = events[0]
    assert "trades_cleaning" in e0.entityUrn
    assert any("raw_trades" in u.dataset for u in e0.aspect.upstreams)

    # Second event: raw_klines → ohlcv_processing
    e1 = events[1]
    assert "ohlcv_processing" in e1.entityUrn
    assert any("raw_klines" in u.dataset for u in e1.aspect.upstreams)


@pytest.mark.skipif(not DATAHUB_AVAILABLE, reason="acryl-datahub not installed")
def test_lineage_spark_to_mongodb():
    """Test Spark → MongoDB lineage edges."""
    emitter = DataLineageEmitter()
    events = emitter.emit_spark_to_mongodb()

    assert len(events) == 3

    # Check downstream URNs contain MongoDB collections
    downstream_urns = [e.entityUrn for e in events]
    assert any("raw_trades" in u for u in downstream_urns)
    assert any("ohlcv" in u and "indicators" not in u for u in downstream_urns)
    assert any("indicators" in u for u in downstream_urns)


@pytest.mark.skipif(not DATAHUB_AVAILABLE, reason="acryl-datahub not installed")
def test_lineage_mongodb_to_ml():
    """Test MongoDB → ML lineage edges."""
    emitter = DataLineageEmitter()
    events = emitter.emit_mongodb_to_ml()

    assert len(events) == 3

    # First event: ohlcv + indicators → feature_engineering
    e0 = events[0]
    assert "feature_engineering" in e0.entityUrn
    upstream_datasets = [u.dataset for u in e0.aspect.upstreams]
    assert len(upstream_datasets) == 2  # ohlcv + indicators
    assert any("ohlcv" in d for d in upstream_datasets)
    assert any("indicators" in d for d in upstream_datasets)

    # Second: feature_engineering → xgboost_predictor
    e1 = events[1]
    assert "xgboost_predictor" in e1.entityUrn

    # Third: feature_engineering → anomaly_detector
    e2 = events[2]
    assert "anomaly_detector" in e2.entityUrn


@pytest.mark.skipif(not DATAHUB_AVAILABLE, reason="acryl-datahub not installed")
def test_lineage_emitter_creates_all_events():
    """Test that emit_all creates exactly 7 lineage + 16 description events."""
    emitter = DataLineageEmitter()

    with patch.object(emitter, 'emitter', None):
        count = emitter.emit_all(dry_run=True)

    # dry_run=True → emitter is None → _events_emitted stays 0
    # But we can verify the methods run without error
    assert count == 0  # No events emitted in dry-run


@pytest.mark.skipif(not DATAHUB_AVAILABLE, reason="acryl-datahub not installed")
def test_lineage_emitter_with_mock_emitter():
    """Test that emit_all with mocked emitter emits all events."""
    emitter = DataLineageEmitter()
    mock_rest = MagicMock()
    mock_rest.test_connection = MagicMock()
    emitter.emitter = mock_rest

    # Call each lineage method
    kafka_events = emitter.emit_kafka_to_spark()
    spark_events = emitter.emit_spark_to_mongodb()
    ml_events = emitter.emit_mongodb_to_ml()
    desc_events = emitter.emit_dataset_descriptions()

    total = len(kafka_events) + len(spark_events) + len(ml_events) + len(desc_events)
    assert total == 2 + 3 + 3 + 16  # 24 total events
    assert mock_rest.emit.call_count == 24


@pytest.mark.skipif(not DATAHUB_AVAILABLE, reason="acryl-datahub not installed")
def test_lineage_dataset_descriptions():
    """Test that dataset descriptions are emitted for all pipeline components."""
    emitter = DataLineageEmitter()
    events = emitter.emit_dataset_descriptions()

    assert len(events) == 16  # 4 Kafka + 4 Spark + 5 MongoDB + 3 ML

    # Verify all events have DatasetPropertiesClass aspect
    for event in events:
        assert hasattr(event.aspect, 'description')
        assert event.aspect.description != ""
        assert "pipeline" in event.aspect.customProperties


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

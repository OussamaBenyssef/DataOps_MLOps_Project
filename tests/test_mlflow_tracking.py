"""
Unit Tests for MLflow Tracking Module - P4 (Abdessamad)
Tests use synthetic data + a local file-based MLflow tracking URI (temp directory).
No Docker or remote MLflow server required.
"""

import pytest
import numpy as np
import pandas as pd
import sys
import os
import tempfile
import shutil

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import mlflow
from ml.mlflow_tracking import MLflowExperimentTracker
from ml.feature_engineering import CryptoFeatureEngineer
from ml.config import model_config, anomaly_config


# ============================================
# FIXTURES
# ============================================

@pytest.fixture(scope="module")
def mlflow_tmpdir():
    """Temporary directory for MLflow file-based tracking."""
    tmpdir = tempfile.mkdtemp(prefix="mlflow_test_")
    yield tmpdir
    shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture(scope="module")
def tracker(mlflow_tmpdir):
    """MLflowExperimentTracker using local file store."""
    tracking_uri = f"file:///{mlflow_tmpdir.replace(os.sep, '/')}"
    return MLflowExperimentTracker(tracking_uri=tracking_uri)


@pytest.fixture(scope="module")
def sample_feature_df():
    """Synthetic feature DataFrame with 200 rows."""
    np.random.seed(42)
    n = 200
    timestamps = pd.date_range("2024-02-01", periods=n, freq="1min")

    base_price = 45000.0
    returns = np.random.normal(0.0002, 0.005, n)

    # Inject anomalies
    for idx in [50, 100, 150]:
        returns[idx] = np.random.choice([-0.06, 0.06])

    close = base_price * np.cumprod(1 + returns)
    noise = np.random.uniform(0.001, 0.005, n)

    df = pd.DataFrame({
        "symbol": "BTCUSDT",
        "interval": "1m",
        "timestamp": timestamps,
        "open": close * (1 - noise),
        "high": close * (1 + np.random.uniform(0, 0.003, n)),
        "low": close * (1 - np.random.uniform(0, 0.003, n)),
        "close": close,
        "volume": np.random.uniform(50, 500, n),
    })

    fe = CryptoFeatureEngineer()
    df = fe.build_feature_matrix_from_dataframe(df, dropna=True)
    return df


@pytest.fixture(scope="module")
def feature_names(sample_feature_df):
    """Feature column names."""
    fe = CryptoFeatureEngineer()
    return fe.get_feature_names(sample_feature_df)


# ============================================
# PREDICTION EXPERIMENT TESTS
# ============================================

@pytest.fixture(scope="module")
def prediction_results(tracker, sample_feature_df, feature_names):
    """Run prediction experiment once, reuse across tests."""
    return tracker.run_prediction_experiment(
        sample_feature_df,
        feature_names,
        experiment_name="test-price-prediction",
        xgboost_params={"n_estimators": 5, "max_depth": 3},
        lstm_epochs=2,
        lstm_batch_size=16,
        symbol="BTCUSDT",
    )


def test_run_prediction_experiment(prediction_results):
    """Test that prediction experiment produces valid results."""
    assert "xgboost_run_id" in prediction_results
    assert "lstm_run_id" in prediction_results
    assert "xgboost_metrics" in prediction_results
    assert "lstm_metrics" in prediction_results
    assert "best_model" in prediction_results
    assert prediction_results["best_model"] in ("xgboost", "lstm")
    assert prediction_results["xgboost_run_id"] is not None
    assert prediction_results["lstm_run_id"] is not None


def test_prediction_metrics_valid(prediction_results):
    """Test that prediction metrics are in expected range."""
    for model in ["xgboost", "lstm"]:
        metrics = prediction_results[f"{model}_metrics"]
        assert 0 <= metrics["f1"] <= 1
        assert 0 <= metrics["accuracy"] <= 1
        assert 0 <= metrics["precision"] <= 1
        assert 0 <= metrics["recall"] <= 1


# ============================================
# ANOMALY EXPERIMENT TESTS
# ============================================

@pytest.fixture(scope="module")
def anomaly_results(tracker, sample_feature_df, feature_names):
    """Run anomaly experiment once, reuse across tests."""
    return tracker.run_anomaly_experiment(
        sample_feature_df,
        feature_names,
        experiment_name="test-anomaly-detection",
        ae_epochs=2,
        ae_batch_size=16,
        symbol="BTCUSDT",
    )


def test_run_anomaly_experiment(anomaly_results):
    """Test that anomaly experiment produces valid results."""
    assert "if_run_id" in anomaly_results
    assert "ae_run_id" in anomaly_results
    assert "if_metrics" in anomaly_results
    assert "ae_metrics" in anomaly_results
    assert "ae_threshold" in anomaly_results
    assert "best_model" in anomaly_results
    assert anomaly_results["best_model"] in ("isolation_forest", "autoencoder")


def test_anomaly_metrics_valid(anomaly_results):
    """Test that anomaly metrics are in expected range."""
    for key in ["if_metrics", "ae_metrics"]:
        metrics = anomaly_results[key]
        assert 0 <= metrics["f1"] <= 1
        assert 0 <= metrics["precision"] <= 1
        assert metrics["anomalies_detected"] >= 0


# ============================================
# QUERY & COMPARISON TESTS
# ============================================

def test_get_best_run(tracker, prediction_results):
    """Test querying the best run by metric."""
    best = tracker.get_best_run("test-price-prediction", metric="f1")
    assert best is not None
    assert best.info.run_id in (
        prediction_results["xgboost_run_id"],
        prediction_results["lstm_run_id"],
    )


def test_compare_runs(tracker, prediction_results):
    """Test run comparison DataFrame."""
    df = tracker.compare_runs("test-price-prediction", top_n=5)
    assert len(df) >= 2
    assert "run_id" in df.columns
    assert "model_type" in df.columns
    assert "f1" in df.columns


def test_compare_runs_nonexistent(tracker):
    """Test comparison for a nonexistent experiment."""
    df = tracker.compare_runs("nonexistent-experiment")
    assert len(df) == 0


def test_list_experiments(tracker, prediction_results, anomaly_results):
    """Test experiment listing."""
    df = tracker.list_experiments()
    assert len(df) >= 2  # at least prediction + anomaly
    assert "name" in df.columns
    assert "run_count" in df.columns


# ============================================
# MODEL REGISTRY TESTS
# ============================================

def test_register_best_model(tracker, prediction_results):
    """Test model registration from best run."""
    version = tracker.register_best_model(
        "test-price-prediction",
        metric="f1",
        model_name="test-predictor",
    )
    assert version is not None


def test_promote_model(tracker, prediction_results):
    """Test model stage transition."""
    # Register first
    version = tracker.register_best_model(
        "test-price-prediction",
        metric="f1",
        model_name="test-predictor-promote",
    )
    assert version is not None

    # Promote to Staging
    success = tracker.promote_model("test-predictor-promote", version, "Staging")
    assert success is True

    # Promote to Production
    success = tracker.promote_model("test-predictor-promote", version, "Production")
    assert success is True


def test_load_production_model(tracker, prediction_results):
    """Test loading a production model."""
    # Register + promote
    version = tracker.register_best_model(
        "test-price-prediction",
        metric="f1",
        model_name="test-predictor-load",
    )
    tracker.promote_model("test-predictor-load", version, "Production")

    model = tracker.load_production_model("test-predictor-load")
    assert model is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

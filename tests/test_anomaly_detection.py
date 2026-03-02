"""
Unit Tests for Anomaly Detection Module - P4 (Abdessamad)
Tests use synthetic data with injected anomalies — no MongoDB, Docker, or MLflow required.
TensorFlow tests use small models with minimal epochs for speed.
"""

import pytest
import numpy as np
import pandas as pd
import sys
import os
import tempfile

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from ml.anomaly_detection import CryptoAnomalyDetector
from ml.feature_engineering import CryptoFeatureEngineer
from ml.config import feature_config


# ============================================
# FIXTURES
# ============================================

@pytest.fixture
def detector():
    """CryptoAnomalyDetector instance."""
    return CryptoAnomalyDetector(contamination=0.1)


@pytest.fixture
def sample_feature_df():
    """
    Generates a synthetic feature DataFrame with 300 rows,
    including injected anomalies (large price spikes).
    """
    np.random.seed(42)
    n = 300
    timestamps = pd.date_range("2024-02-01", periods=n, freq="1min")

    base_price = 45000.0
    returns = np.random.normal(0.0002, 0.005, n)

    # Inject obvious anomalies at known positions
    anomaly_indices = [50, 100, 150, 200, 250]
    for idx in anomaly_indices:
        returns[idx] = np.random.choice([-0.08, 0.08])  # 8% spike

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

    # Run through feature engineering
    fe = CryptoFeatureEngineer()
    df = fe.build_feature_matrix_from_dataframe(df, dropna=True)
    return df


@pytest.fixture
def feature_names(sample_feature_df):
    """Feature column names."""
    fe = CryptoFeatureEngineer()
    return fe.get_feature_names(sample_feature_df)


@pytest.fixture
def prepared_data(detector, sample_feature_df, feature_names):
    """Pre-split and scaled data."""
    return detector.prepare_data(sample_feature_df, feature_names)


# ============================================
# DATA PREPARATION TESTS
# ============================================

def test_prepare_data_shapes(detector, sample_feature_df, feature_names):
    """Test train/test split produces correct proportions."""
    X_train, X_test, y_train, y_test = detector.prepare_data(
        sample_feature_df, feature_names
    )
    total = len(sample_feature_df)
    expected_test = int(total * detector.test_size)

    assert X_test.shape[0] == expected_test
    assert X_train.shape[0] == total - expected_test
    assert y_train.shape[0] == X_train.shape[0]
    assert y_test.shape[0] == X_test.shape[0]


def test_prepare_data_scaling(detector, sample_feature_df, feature_names):
    """Test that features are scaled (mean near 0, std near 1)."""
    X_train, _, _, _ = detector.prepare_data(sample_feature_df, feature_names)
    means = np.abs(X_train.mean(axis=0))
    stds = X_train.std(axis=0)

    assert np.all(means < 1.0), "Scaled means should be near 0"
    assert np.median(stds) < 2.0, "Scaled stds should be near 1"


def test_prepare_data_labels(detector, sample_feature_df, feature_names):
    """Test that anomaly labels are binary."""
    _, _, y_train, y_test = detector.prepare_data(sample_feature_df, feature_names)
    assert set(np.unique(y_train)).issubset({0, 1})
    assert set(np.unique(y_test)).issubset({0, 1})


# ============================================
# ISOLATION FOREST TESTS
# ============================================

def test_train_isolation_forest(detector, prepared_data):
    """Test Isolation Forest training and metric output."""
    X_train, X_test, _, y_test = prepared_data

    model, metrics = detector.train_isolation_forest(X_train, X_test, y_test)

    assert model is not None
    assert "precision" in metrics
    assert "recall" in metrics
    assert "f1" in metrics
    assert "anomalies_detected" in metrics
    assert 0 <= metrics["precision"] <= 1
    assert 0 <= metrics["f1"] <= 1
    assert metrics["anomalies_detected"] >= 0


def test_predict_isolation_forest(detector, prepared_data):
    """Test Isolation Forest prediction output."""
    X_train, X_test, _, y_test = prepared_data

    model, _ = detector.train_isolation_forest(X_train, X_test, y_test)
    preds = detector.predict_isolation_forest(model, X_test)

    assert preds.shape == (X_test.shape[0],)
    assert set(np.unique(preds)).issubset({0, 1})


def test_save_load_isolation_forest(detector, prepared_data):
    """Test Isolation Forest model save and reload."""
    X_train, X_test, _, y_test = prepared_data
    model, _ = detector.train_isolation_forest(X_train, X_test, y_test)

    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "test_if")
        saved_path = detector.save_model(model, path, "isolation_forest")
        loaded = detector.load_model(saved_path, "isolation_forest")

        preds_original = model.predict(X_test)
        preds_loaded = loaded.predict(X_test)
        np.testing.assert_array_equal(preds_original, preds_loaded)


# ============================================
# AUTOENCODER TESTS
# ============================================

def test_train_autoencoder(detector, prepared_data):
    """Test Autoencoder training, threshold, and metric output."""
    X_train, X_test, _, y_test = prepared_data

    model, threshold, metrics = detector.train_autoencoder(
        X_train, X_test, y_test, epochs=3, batch_size=16
    )

    assert model is not None
    assert threshold > 0
    assert "precision" in metrics
    assert "recall" in metrics
    assert "f1" in metrics
    assert "anomalies_detected" in metrics
    assert "threshold" in metrics
    assert 0 <= metrics["precision"] <= 1


def test_predict_autoencoder(detector, prepared_data):
    """Test Autoencoder prediction output."""
    X_train, X_test, _, y_test = prepared_data

    model, threshold, _ = detector.train_autoencoder(
        X_train, X_test, y_test, epochs=3, batch_size=16
    )

    preds = detector.predict_autoencoder(model, X_test, threshold)
    assert preds.shape == (X_test.shape[0],)
    assert set(np.unique(preds)).issubset({0, 1})


def test_save_load_autoencoder(detector, prepared_data):
    """Test Autoencoder model save and reload."""
    X_train, X_test, _, y_test = prepared_data
    model, threshold, _ = detector.train_autoencoder(
        X_train, X_test, y_test, epochs=3, batch_size=16
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "test_ae")
        saved_path = detector.save_model(model, path, "autoencoder")
        loaded = detector.load_model(saved_path, "autoencoder")

        preds_original = model.predict(X_test, verbose=0)
        preds_loaded = loaded.predict(X_test, verbose=0)
        np.testing.assert_array_almost_equal(preds_original, preds_loaded, decimal=5)


def test_save_load_threshold(detector):
    """Test threshold save and reload."""
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "test")
        detector.save_threshold(0.0123, path)
        loaded = detector.load_threshold(f"{path}_threshold.json")
        assert abs(loaded - 0.0123) < 1e-6


# ============================================
# DETECT & COMPARE TEST
# ============================================

def test_detect_and_compare(detector, sample_feature_df, feature_names):
    """Test full comparison pipeline."""
    results = detector.detect_and_compare(
        sample_feature_df,
        feature_names,
        ae_epochs=3,
        ae_batch_size=16,
        log_mlflow=False,
    )

    assert results["best_model_name"] in ("isolation_forest", "autoencoder")
    assert "if_metrics" in results
    assert "ae_metrics" in results
    assert "if_model" in results
    assert "ae_model" in results
    assert "ae_threshold" in results
    assert results["if_model"] is not None
    assert results["ae_model"] is not None
    assert results["ae_threshold"] > 0


# ============================================
# SCALER TESTS
# ============================================

def test_save_load_scaler(detector, sample_feature_df, feature_names):
    """Test scaler save and reload."""
    detector.prepare_data(sample_feature_df, feature_names)

    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "test")
        detector.save_scaler(path)

        new_detector = CryptoAnomalyDetector()
        new_detector.load_scaler(f"{path}_scaler.joblib")

        np.testing.assert_array_almost_equal(
            detector.scaler.mean_, new_detector.scaler.mean_
        )


# ============================================
# ANOMALY DETECTION QUALITY TEST
# ============================================

def test_isolation_forest_detects_anomalies(detector, prepared_data):
    """Test that Isolation Forest detects at least some anomalies."""
    X_train, X_test, _, y_test = prepared_data

    model, metrics = detector.train_isolation_forest(X_train, X_test, y_test)

    # Should detect at least 1 anomaly in the test set
    assert metrics["anomalies_detected"] > 0, "Isolation Forest should detect anomalies"


def test_autoencoder_detects_anomalies(detector, prepared_data):
    """Test that Autoencoder detects at least some anomalies."""
    X_train, X_test, _, y_test = prepared_data

    _, _, metrics = detector.train_autoencoder(
        X_train, X_test, y_test, epochs=5, batch_size=16
    )

    assert metrics["anomalies_detected"] > 0, "Autoencoder should detect anomalies"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

"""
Unit Tests for Model Training Module - P4 (Abdessamad)
Tests use synthetic data — no MongoDB, Docker, or MLflow server required.
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

from ml.model_training import CryptoPricePredictor
from ml.feature_engineering import CryptoFeatureEngineer
from ml.config import feature_config


# ============================================
# FIXTURES
# ============================================

@pytest.fixture
def predictor():
    """CryptoPricePredictor instance."""
    return CryptoPricePredictor(sequence_length=5)


@pytest.fixture
def sample_feature_df():
    """
    Generates a synthetic feature DataFrame with 200 rows,
    mimicking the output of CryptoFeatureEngineer.
    """
    np.random.seed(42)
    n = 200
    timestamps = pd.date_range("2024-02-01", periods=n, freq="1min")

    base_price = 45000.0
    returns = np.random.normal(0.0002, 0.005, n)
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
    """Feature column names (from the feature engineer)."""
    fe = CryptoFeatureEngineer()
    return fe.get_feature_names(sample_feature_df)


@pytest.fixture
def prepared_data(predictor, sample_feature_df, feature_names):
    """Pre-split and scaled data."""
    return predictor.prepare_data(sample_feature_df, feature_names)


# ============================================
# DATA PREPARATION TESTS
# ============================================

def test_prepare_data_shapes(predictor, sample_feature_df, feature_names):
    """Test train/test split produces correct proportions."""
    X_train, X_test, y_train, y_test = predictor.prepare_data(
        sample_feature_df, feature_names
    )
    total = len(sample_feature_df)
    expected_test = int(total * predictor.test_size)

    assert X_test.shape[0] == expected_test
    assert X_train.shape[0] == total - expected_test
    assert y_train.shape[0] == X_train.shape[0]
    assert y_test.shape[0] == X_test.shape[0]


def test_prepare_data_scaling(predictor, sample_feature_df, feature_names):
    """Test that features are scaled (mean ≈ 0, std ≈ 1 for training set)."""
    X_train, _, _, _ = predictor.prepare_data(sample_feature_df, feature_names)
    means = np.abs(X_train.mean(axis=0))
    stds = X_train.std(axis=0)

    # Means should be close to 0 (within tolerance)
    assert np.all(means < 1.0), "Scaled means should be near 0"
    # Most stds should be close to 1
    assert np.median(stds) < 2.0, "Scaled stds should be near 1"


# ============================================
# SEQUENCE CREATION TESTS
# ============================================

def test_create_sequences(predictor, prepared_data):
    """Test LSTM sequence creation produces correct shapes."""
    X_train, _, y_train, _ = prepared_data
    X_seq, y_seq = predictor.create_sequences(
        X_train, y_train, seq_length=5
    )

    expected_samples = X_train.shape[0] - 5
    assert X_seq.shape == (expected_samples, 5, X_train.shape[1])
    assert y_seq.shape == (expected_samples,)


def test_create_sequences_values(predictor, prepared_data):
    """Test that sequence values are correctly aligned."""
    X_train, _, y_train, _ = prepared_data
    X_seq, y_seq = predictor.create_sequences(X_train, y_train, seq_length=3)

    # The first sequence should contain rows 0, 1, 2
    np.testing.assert_array_equal(X_seq[0], X_train[0:3])
    # Its target should be row 3's target
    assert y_seq[0] == y_train[3]


# ============================================
# XGBOOST TESTS
# ============================================

def test_train_xgboost(predictor, prepared_data):
    """Test XGBoost training and metric output."""
    X_train, X_test, y_train, y_test = prepared_data

    model, metrics = predictor.train_xgboost(
        X_train, y_train, X_test, y_test,
        params={"n_estimators": 10, "max_depth": 3}  # small for speed
    )

    assert model is not None
    assert set(metrics.keys()) == {"accuracy", "precision", "recall", "f1"}
    assert 0 <= metrics["accuracy"] <= 1
    assert 0 <= metrics["f1"] <= 1


def test_predict_xgboost(predictor, prepared_data):
    """Test XGBoost prediction output."""
    X_train, X_test, y_train, y_test = prepared_data

    model, _ = predictor.train_xgboost(
        X_train, y_train, X_test, y_test,
        params={"n_estimators": 10}
    )

    preds = predictor.predict(model, X_test, model_type="xgboost")
    assert preds.shape == (X_test.shape[0],)
    assert set(np.unique(preds)).issubset({0, 1})


def test_save_load_xgboost(predictor, prepared_data):
    """Test XGBoost model save and reload."""
    X_train, X_test, y_train, y_test = prepared_data
    model, _ = predictor.train_xgboost(
        X_train, y_train, X_test, y_test,
        params={"n_estimators": 5}
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "test_xgb")
        saved_path = predictor.save_model(model, path, "xgboost")
        loaded = predictor.load_model(saved_path, "xgboost")

        preds_original = model.predict(X_test)
        preds_loaded = loaded.predict(X_test)
        np.testing.assert_array_equal(preds_original, preds_loaded)


# ============================================
# LSTM TESTS
# ============================================

def test_train_lstm(predictor, prepared_data):
    """Test LSTM training and metric output (minimal epochs)."""
    X_train, X_test, y_train, y_test = prepared_data

    model, metrics = predictor.train_lstm(
        X_train, y_train, X_test, y_test,
        epochs=2, batch_size=16  # minimal for speed
    )

    assert model is not None
    assert set(metrics.keys()) == {"accuracy", "precision", "recall", "f1"}
    assert 0 <= metrics["accuracy"] <= 1


def test_predict_lstm(predictor, prepared_data):
    """Test LSTM prediction output."""
    X_train, X_test, y_train, y_test = prepared_data

    model, _ = predictor.train_lstm(
        X_train, y_train, X_test, y_test,
        epochs=2, batch_size=16
    )

    preds = predictor.predict(model, X_test, model_type="lstm")
    assert len(preds) > 0
    assert set(np.unique(preds)).issubset({0, 1})


def test_save_load_lstm(predictor, prepared_data):
    """Test LSTM model save and reload."""
    X_train, X_test, y_train, y_test = prepared_data
    model, _ = predictor.train_lstm(
        X_train, y_train, X_test, y_test,
        epochs=2, batch_size=16
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "test_lstm")
        saved_path = predictor.save_model(model, path, "lstm")
        loaded = predictor.load_model(saved_path, "lstm")

        X_seq, _ = predictor.create_sequences(
            X_test, y_test, predictor.sequence_length
        )
        if len(X_seq) > 0:
            preds_original = model.predict(X_seq, verbose=0)
            preds_loaded = loaded.predict(X_seq, verbose=0)
            np.testing.assert_array_almost_equal(preds_original, preds_loaded, decimal=5)


# ============================================
# TRAIN & COMPARE TEST
# ============================================

def test_train_and_compare(predictor, sample_feature_df, feature_names):
    """Test full comparison pipeline."""
    results = predictor.train_and_compare(
        sample_feature_df,
        feature_names,
        xgboost_params={"n_estimators": 5, "max_depth": 3},
        lstm_epochs=2,
        lstm_batch_size=16,
        log_mlflow=False
    )

    assert results["best_model_name"] in ("xgboost", "lstm")
    assert "xgboost_metrics" in results
    assert "lstm_metrics" in results
    assert "xgboost_model" in results
    assert "lstm_model" in results
    assert results["xgboost_model"] is not None
    assert results["lstm_model"] is not None


# ============================================
# SCALER TESTS
# ============================================

def test_save_load_scaler(predictor, sample_feature_df, feature_names):
    """Test scaler save and reload."""
    predictor.prepare_data(sample_feature_df, feature_names)

    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "test")
        predictor.save_scaler(path)

        new_predictor = CryptoPricePredictor()
        new_predictor.load_scaler(f"{path}_scaler.joblib")

        np.testing.assert_array_almost_equal(
            predictor.scaler.mean_, new_predictor.scaler.mean_
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

"""
Unit Tests for MLflow Model Registry Module - P4 (Abdessamad)
Tests use synthetic data + a local SQLite-based MLflow tracking URI.
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
from ml.mlflow_registry import MLflowModelRegistry
from ml.mlflow_tracking import MLflowExperimentTracker
from ml.feature_engineering import CryptoFeatureEngineer


# ============================================
# FIXTURES
# ============================================

@pytest.fixture(scope="module")
def mlflow_tmpdir():
    """Temporary directory for MLflow SQLite-based tracking."""
    tmpdir = tempfile.mkdtemp(prefix="mlflow_reg_test_")
    yield tmpdir
    shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture(scope="module")
def tracking_uri(mlflow_tmpdir):
    """SQLite tracking URI (avoids file-store deprecation)."""
    db_path = os.path.join(mlflow_tmpdir, "mlflow.db").replace(os.sep, "/")
    return f"sqlite:///{db_path}"


@pytest.fixture(scope="module")
def registry(tracking_uri):
    """MLflowModelRegistry using local SQLite store."""
    return MLflowModelRegistry(tracking_uri=tracking_uri)


@pytest.fixture(scope="module")
def tracker(tracking_uri):
    """MLflowExperimentTracker (same store) to create runs for testing."""
    return MLflowExperimentTracker(tracking_uri=tracking_uri)


@pytest.fixture(scope="module")
def sample_feature_df():
    """Synthetic feature DataFrame with 200 rows."""
    np.random.seed(42)
    n = 200
    timestamps = pd.date_range("2024-02-01", periods=n, freq="1min")

    base_price = 45000.0
    returns = np.random.normal(0.0002, 0.005, n)
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


@pytest.fixture(scope="module")
def experiment_results(tracker, sample_feature_df, feature_names):
    """Run a prediction experiment to create models we can register."""
    return tracker.run_prediction_experiment(
        sample_feature_df,
        feature_names,
        experiment_name="registry-test-prediction",
        xgboost_params={"n_estimators": 5, "max_depth": 3},
        lstm_epochs=2,
        lstm_batch_size=16,
    )


MODEL_NAME = "test-registry-model"


# ============================================
# REGISTRATION TESTS
# ============================================

def test_register_model(registry, experiment_results):
    """Test registering a model from a run."""
    run_id = experiment_results["xgboost_run_id"]
    version = registry.register_model(run_id, MODEL_NAME)

    assert version is not None
    assert int(version) >= 1


def test_register_second_version(registry, experiment_results):
    """Test registering a second version of the same model."""
    run_id = experiment_results["lstm_run_id"]
    version = registry.register_model(run_id, MODEL_NAME)

    assert version is not None
    assert int(version) >= 2


# ============================================
# PROMOTION TESTS
# ============================================

def test_promote_to_staging(registry):
    """Test promoting a model version to staging."""
    success = registry.promote_to_staging(MODEL_NAME, "1")
    assert success is True


def test_validate_before_promotion_pass(registry):
    """Test validation passes with low thresholds."""
    result = registry.validate_before_promotion(
        MODEL_NAME, "1", min_f1=0.0, min_accuracy=0.0
    )
    assert result is True


def test_validate_before_promotion_fail(registry):
    """Test validation fails with impossibly high thresholds."""
    result = registry.validate_before_promotion(
        MODEL_NAME, "1", min_f1=0.999999
    )
    assert result is False


def test_promote_to_production(registry):
    """Test promoting model to production (no validation)."""
    success = registry.promote_to_production(MODEL_NAME, "1")
    assert success is True


def test_promote_to_production_with_validation(registry):
    """Test promotion to production with validation thresholds."""
    # Should fail with impossible threshold
    success = registry.promote_to_production(
        MODEL_NAME, "2", min_f1=0.999999
    )
    assert success is False

    # Should pass with low threshold
    success = registry.promote_to_production(
        MODEL_NAME, "2", min_f1=0.0
    )
    assert success is True


# ============================================
# MODEL LOADING TESTS
# ============================================

def test_load_model_by_alias_staging(registry):
    """Test loading a model by staging alias."""
    model = registry.load_model_by_alias(MODEL_NAME, "staging")
    assert model is not None


def test_load_model_by_alias_production(registry):
    """Test loading a model by production alias."""
    model = registry.load_model_by_alias(MODEL_NAME, "production")
    assert model is not None


def test_load_latest_model(registry):
    """Test loading the latest model version."""
    model = registry.load_latest_model(MODEL_NAME)
    assert model is not None


# ============================================
# QUERYING & COMPARISON TESTS
# ============================================

def test_list_model_versions(registry):
    """Test listing all versions of a model."""
    df = registry.list_model_versions(MODEL_NAME)
    assert len(df) >= 2
    assert "version" in df.columns
    assert "run_id" in df.columns
    assert "aliases" in df.columns


def test_get_model_version_info(registry):
    """Test getting detailed info for a version."""
    info = registry.get_model_version_info(MODEL_NAME, "1")
    assert info is not None
    assert info["model_name"] == MODEL_NAME
    assert str(info["version"]) == "1"
    assert "metrics" in info
    assert "params" in info
    assert "tags" in info


def test_compare_model_versions(registry):
    """Test comparing all versions by metrics."""
    df = registry.compare_model_versions(MODEL_NAME)
    assert len(df) >= 2
    assert "version" in df.columns
    assert "run_id" in df.columns


def test_get_production_version(registry):
    """Test getting current production version info."""
    info = registry.get_production_version(MODEL_NAME)
    assert info is not None
    assert info["model_name"] == MODEL_NAME
    assert "metrics" in info


# ============================================
# LIFECYCLE TESTS
# ============================================

def test_archive_model(registry):
    """Test archiving a model version."""
    success = registry.archive_model(MODEL_NAME, "1")
    assert success is True


def test_rollback_production(registry):
    """Test rolling back production to previous version."""
    # Currently v2 is production, rollback should go to v1
    success = registry.rollback_production(MODEL_NAME)
    assert success is True

    # Verify production now points to v1
    info = registry.get_production_version(MODEL_NAME)
    assert info is not None
    assert str(info["version"]) == "1"


def test_list_model_versions_nonexistent(registry):
    """Test listing versions of a nonexistent model."""
    df = registry.list_model_versions("nonexistent-model-xyz")
    assert len(df) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

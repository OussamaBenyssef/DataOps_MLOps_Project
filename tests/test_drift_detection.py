"""
Unit Tests for Data Drift Detection Module - P4 (Abdessamad)
Tests use synthetic data with injected drift — no Docker or external services required.
"""

import pytest
import numpy as np
import pandas as pd
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from ml.drift_detection import DataDriftDetector


# ============================================
# FIXTURES
# ============================================

@pytest.fixture
def detector():
    """DataDriftDetector instance."""
    return DataDriftDetector(psi_bins=10, psi_threshold=0.2, ks_pvalue_threshold=0.05)


@pytest.fixture
def reference_df():
    """Reference DataFrame (training data) — normal distribution."""
    np.random.seed(42)
    n = 500
    return pd.DataFrame({
        "feature_a": np.random.normal(0, 1, n),
        "feature_b": np.random.normal(5, 2, n),
        "feature_c": np.random.uniform(0, 10, n),
        "feature_d": np.random.exponential(2, n),
    })


@pytest.fixture
def no_drift_df():
    """Current DataFrame with NO drift (same distribution)."""
    np.random.seed(123)
    n = 500
    return pd.DataFrame({
        "feature_a": np.random.normal(0, 1, n),
        "feature_b": np.random.normal(5, 2, n),
        "feature_c": np.random.uniform(0, 10, n),
        "feature_d": np.random.exponential(2, n),
    })


@pytest.fixture
def drifted_df():
    """Current DataFrame with INJECTED drift (shifted distributions)."""
    np.random.seed(456)
    n = 500
    return pd.DataFrame({
        "feature_a": np.random.normal(3, 1, n),     # mean shifted 0 -> 3
        "feature_b": np.random.normal(5, 6, n),     # std widened 2 -> 6
        "feature_c": np.random.uniform(5, 15, n),   # range shifted
        "feature_d": np.random.exponential(2, n),    # NO drift (same)
    })


# ============================================
# PSI TESTS
# ============================================

def test_psi_no_drift(detector):
    """PSI should be near 0 for identical distributions."""
    np.random.seed(42)
    ref = np.random.normal(0, 1, 1000)
    curr = np.random.normal(0, 1, 1000)
    psi = detector.compute_psi(ref, curr)
    assert psi < 0.1, f"PSI {psi} should be < 0.1 for same distribution"


def test_psi_drift(detector):
    """PSI should be high for shifted distributions."""
    np.random.seed(42)
    ref = np.random.normal(0, 1, 1000)
    curr = np.random.normal(3, 1, 1000)  # mean shifted by 3σ
    psi = detector.compute_psi(ref, curr)
    assert psi > 0.2, f"PSI {psi} should be > 0.2 for shifted distribution"


def test_psi_non_negative(detector):
    """PSI should always be >= 0."""
    np.random.seed(42)
    ref = np.random.normal(0, 1, 500)
    curr = np.random.normal(0, 1, 500)
    psi = detector.compute_psi(ref, curr)
    assert psi >= 0


# ============================================
# KS TEST
# ============================================

def test_ks_test_no_drift(detector):
    """KS test should not flag drift for same distribution."""
    np.random.seed(42)
    ref = np.random.normal(0, 1, 1000)
    curr = np.random.normal(0, 1, 1000)
    result = detector.compute_ks_test(ref, curr)
    assert "statistic" in result
    assert "p_value" in result
    assert "is_drifted" in result
    assert result["p_value"] > 0.05  # Should not reject H0


def test_ks_test_drift(detector):
    """KS test should flag drift for shifted distribution."""
    np.random.seed(42)
    ref = np.random.normal(0, 1, 1000)
    curr = np.random.normal(2, 1, 1000)
    result = detector.compute_ks_test(ref, curr)
    assert result["is_drifted"] is True
    assert result["p_value"] < 0.05


# ============================================
# FEATURE DRIFT TESTS
# ============================================

def test_feature_drift_no_drift(detector, reference_df, no_drift_df):
    """No significant drift expected between same-distribution data."""
    features = list(reference_df.columns)
    results = detector.detect_feature_drift(reference_df, no_drift_df, features)

    assert len(results) == len(features)
    for feat, details in results.items():
        assert "psi" in details
        assert "ks_statistic" in details
        assert "ks_pvalue" in details
        assert "is_drifted" in details
        assert "drift_level" in details


def test_feature_drift_detected(detector, reference_df, drifted_df):
    """Drift should be detected for shifted features."""
    features = list(reference_df.columns)
    results = detector.detect_feature_drift(reference_df, drifted_df, features)

    # feature_a, feature_b, feature_c are drifted
    drifted_count = sum(1 for v in results.values() if v["is_drifted"])
    assert drifted_count >= 2, f"Expected >= 2 drifted features, got {drifted_count}"

    # feature_d should NOT be drifted (same distribution)
    assert results["feature_d"]["drift_level"] == "none"


# ============================================
# PREDICTION DRIFT TESTS
# ============================================

def test_prediction_drift_no_drift(detector):
    """No prediction drift for same distribution."""
    np.random.seed(42)
    ref = np.random.binomial(1, 0.5, 500).astype(float)
    curr = np.random.binomial(1, 0.5, 500).astype(float)
    result = detector.detect_prediction_drift(ref, curr)
    assert "psi" in result
    assert "drift_level" in result


def test_prediction_drift_detected(detector):
    """Prediction drift should be detected for shifted predictions."""
    np.random.seed(42)
    ref = np.random.binomial(1, 0.3, 500).astype(float)   # 30% positive
    curr = np.random.binomial(1, 0.8, 500).astype(float)  # 80% positive
    result = detector.detect_prediction_drift(ref, curr)
    assert result["is_drifted"] is True


# ============================================
# DRIFT REPORT TESTS
# ============================================

def test_drift_report_no_drift(detector, reference_df, no_drift_df):
    """Report should show no significant drift."""
    features = list(reference_df.columns)
    report = detector.generate_drift_report(reference_df, no_drift_df, features)

    assert "timestamp" in report
    assert "total_features" in report
    assert "drifted_features_count" in report
    assert "drift_ratio" in report
    assert "overall_psi" in report
    assert "overall_drift_level" in report
    assert "feature_details" in report
    assert report["total_features"] == len(features)
    assert report["overall_drift_level"] in ("none", "moderate")


def test_drift_report_with_drift(detector, reference_df, drifted_df):
    """Report should detect significant drift."""
    features = list(reference_df.columns)
    report = detector.generate_drift_report(reference_df, drifted_df, features)

    assert report["drifted_features_count"] >= 2
    assert len(report["drifted_features"]) >= 2
    assert report["drift_ratio"] > 0


def test_drift_report_with_predictions(detector, reference_df, no_drift_df):
    """Report should include prediction drift when provided."""
    np.random.seed(42)
    features = list(reference_df.columns)
    ref_preds = np.random.binomial(1, 0.5, 100).astype(float)
    curr_preds = np.random.binomial(1, 0.5, 100).astype(float)

    report = detector.generate_drift_report(
        reference_df, no_drift_df, features,
        ref_preds=ref_preds, curr_preds=curr_preds,
    )
    assert "prediction_drift" in report
    assert "psi" in report["prediction_drift"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

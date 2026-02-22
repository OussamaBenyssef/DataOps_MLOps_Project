"""
Unit Tests for Feature Engineering Module - P4 (Abdessamad)
Tests use synthetic pandas DataFrames — no MongoDB or Spark required.
"""

import pytest
import numpy as np
import pandas as pd
import sys
import os

# Add src to path for package imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from ml.feature_engineering import CryptoFeatureEngineer
from ml.config import feature_config


# ============================================
# FIXTURES
# ============================================

@pytest.fixture
def engineer():
    """CryptoFeatureEngineer instance (no MongoDB interaction in tests)."""
    return CryptoFeatureEngineer()


@pytest.fixture
def sample_ohlcv_df():
    """
    Generates a realistic synthetic OHLCV DataFrame with 100 rows.
    Simulates an upward-trending market with some noise.
    """
    np.random.seed(42)
    n = 100
    timestamps = pd.date_range("2024-02-01", periods=n, freq="1min")

    # Random walk for price
    base_price = 45000.0
    returns = np.random.normal(0.0002, 0.005, n)
    close_prices = base_price * np.cumprod(1 + returns)

    # Build OHLCV from close
    noise = np.random.uniform(0.001, 0.005, n)
    open_prices = close_prices * (1 - noise * np.random.choice([-1, 1], n))
    high_prices = np.maximum(open_prices, close_prices) * (1 + np.random.uniform(0, 0.003, n))
    low_prices = np.minimum(open_prices, close_prices) * (1 - np.random.uniform(0, 0.003, n))
    volumes = np.random.uniform(50, 500, n)

    return pd.DataFrame({
        "symbol": "BTCUSDT",
        "interval": "1m",
        "timestamp": timestamps,
        "open": open_prices,
        "high": high_prices,
        "low": low_prices,
        "close": close_prices,
        "volume": volumes,
        "num_trades": np.random.randint(100, 5000, n),
    })


@pytest.fixture
def sample_indicators_df(sample_ohlcv_df):
    """
    Generates matching indicator columns as if computed by P3.
    Uses simplified calculations — the exact values are not critical,
    only that the columns exist and have plausible ranges.
    """
    df = sample_ohlcv_df[["symbol", "interval", "timestamp", "close"]].copy()

    # SMA
    df["sma_20"] = df["close"].rolling(20).mean()
    df["sma_50"] = df["close"].rolling(50).mean()

    # EMA
    df["ema_12"] = df["close"].ewm(span=12).mean()
    df["ema_26"] = df["close"].ewm(span=26).mean()

    # RSI (simplified)
    delta = df["close"].diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    rs = gain / loss.replace(0, np.nan)
    df["rsi_14"] = 100 - (100 / (1 + rs))

    # MACD
    df["macd"] = df["ema_12"] - df["ema_26"]
    df["macd_signal"] = df["macd"].rolling(9).mean()
    df["macd_histogram"] = df["macd"] - df["macd_signal"]

    # Bollinger
    df["bollinger_middle"] = df["close"].rolling(20).mean()
    std = df["close"].rolling(20).std()
    df["bollinger_upper"] = df["bollinger_middle"] + 2 * std
    df["bollinger_lower"] = df["bollinger_middle"] - 2 * std

    # Drop the close column before merge (it will come from OHLCV)
    df = df.drop(columns=["close"])

    return df


@pytest.fixture
def merged_df(sample_ohlcv_df, sample_indicators_df):
    """Merged OHLCV + indicators DataFrame."""
    return CryptoFeatureEngineer.merge_ohlcv_indicators(
        sample_ohlcv_df, sample_indicators_df
    )


# ============================================
# MERGE TESTS
# ============================================

def test_merge_ohlcv_indicators(sample_ohlcv_df, sample_indicators_df):
    """Test merging OHLCV and indicators DataFrames."""
    merged = CryptoFeatureEngineer.merge_ohlcv_indicators(
        sample_ohlcv_df, sample_indicators_df
    )
    assert len(merged) == len(sample_ohlcv_df)
    # Should have indicator columns
    assert "rsi_14" in merged.columns
    assert "macd" in merged.columns
    assert "bollinger_upper" in merged.columns
    # Should NOT have duplicate 'close' column
    assert list(merged.columns).count("close") == 1


def test_merge_with_empty_indicators(sample_ohlcv_df):
    """Test merge when indicators are empty returns OHLCV unchanged."""
    empty = pd.DataFrame()
    result = CryptoFeatureEngineer.merge_ohlcv_indicators(
        sample_ohlcv_df, empty
    )
    assert len(result) == len(sample_ohlcv_df)


# ============================================
# PRICE FEATURE TESTS
# ============================================

def test_add_price_features(merged_df, engineer):
    """Test price feature column creation and value sanity."""
    result = engineer.add_price_features(merged_df.copy())

    # Return columns
    for p in feature_config.return_periods:
        col_name = f"return_{p}"
        assert col_name in result.columns, f"Missing column: {col_name}"

    # Log return
    assert "log_return_1" in result.columns

    # Volatility columns
    for w in feature_config.rolling_windows:
        col_name = f"volatility_{w}"
        assert col_name in result.columns, f"Missing column: {col_name}"

    # ATR
    assert "atr" in result.columns

    # Volatility values should be non-negative (where not NaN)
    vol5 = result["volatility_5"].dropna()
    assert (vol5 >= 0).all(), "Volatility should be non-negative"


# ============================================
# VOLUME FEATURE TESTS
# ============================================

def test_add_volume_features(merged_df, engineer):
    """Test volume feature column creation."""
    result = engineer.add_volume_features(merged_df.copy())

    for w in feature_config.rolling_windows:
        col_name = f"volume_ratio_{w}"
        assert col_name in result.columns, f"Missing column: {col_name}"

    assert "volume_change_1" in result.columns

    # Volume ratio should be positive where defined
    vr5 = result["volume_ratio_5"].dropna()
    assert (vr5 > 0).all(), "Volume ratio should be positive"


# ============================================
# CANDLE FEATURE TESTS
# ============================================

def test_add_candle_features(merged_df, engineer):
    """Test candle structure feature creation and value ranges."""
    result = engineer.add_candle_features(merged_df.copy())

    assert "body_ratio" in result.columns
    assert "upper_shadow" in result.columns
    assert "lower_shadow" in result.columns
    assert "hl_range" in result.columns

    # Ratios should be between 0 and 1 (where not NaN)
    body = result["body_ratio"].dropna()
    assert (body >= 0).all() and (body <= 1.01).all(), "body_ratio should be in [0, 1]"

    upper = result["upper_shadow"].dropna()
    assert (upper >= -0.01).all() and (upper <= 1.01).all(), "upper_shadow should be in [0, 1]"


# ============================================
# INDICATOR FEATURE TESTS
# ============================================

def test_add_indicator_features(merged_df, engineer):
    """Test indicator-derived feature creation."""
    result = engineer.add_indicator_features(merged_df.copy())

    # RSI zone
    assert "rsi_zone" in result.columns
    rsi_zones = result["rsi_zone"].dropna().unique()
    assert set(rsi_zones).issubset({0, 1, 2}), f"Unexpected RSI zones: {rsi_zones}"

    # MACD cross
    assert "macd_cross_signal" in result.columns
    macd_vals = result["macd_cross_signal"].dropna().unique()
    assert set(macd_vals).issubset({-1, 0, 1}), f"Unexpected MACD signals: {macd_vals}"

    # Bollinger
    assert "bollinger_pband" in result.columns
    assert "bollinger_width" in result.columns

    # SMA cross
    assert "sma_cross_20_50" in result.columns
    sma_vals = result["sma_cross_20_50"].dropna().unique()
    assert set(sma_vals).issubset({0, 1}), f"Unexpected SMA cross values: {sma_vals}"


def test_indicator_features_without_indicators(sample_ohlcv_df, engineer):
    """Test that indicator features are skipped gracefully when columns are missing."""
    result = engineer.add_indicator_features(sample_ohlcv_df.copy())
    # Should not crash, and should not add indicator columns
    assert "rsi_zone" not in result.columns
    assert "macd_cross_signal" not in result.columns


# ============================================
# LAG FEATURE TESTS
# ============================================

def test_add_lag_features(merged_df, engineer):
    """Test lag feature creation and correct shift values."""
    result = engineer.add_lag_features(merged_df.copy())

    for lag in feature_config.lag_periods:
        assert f"close_lag_{lag}" in result.columns
        assert f"volume_lag_{lag}" in result.columns

    # RSI lags
    for lag in feature_config.lag_periods[:3]:
        assert f"rsi_14_lag_{lag}" in result.columns

    # Verify lag correctness: close_lag_1[i] == close[i-1]
    assert result["close_lag_1"].iloc[5] == result["close"].iloc[4]


# ============================================
# ROLLING FEATURE TESTS
# ============================================

def test_add_rolling_features(merged_df, engineer):
    """Test rolling statistical features."""
    result = engineer.add_rolling_features(merged_df.copy())

    for w in feature_config.rolling_windows:
        assert f"rolling_mean_{w}" in result.columns
        assert f"rolling_std_{w}" in result.columns

    max_w = max(feature_config.rolling_windows)
    assert f"rolling_min_{max_w}" in result.columns
    assert f"rolling_max_{max_w}" in result.columns

    # Rolling std should be non-negative
    rstd = result["rolling_std_5"].dropna()
    assert (rstd >= 0).all()


# ============================================
# TARGET VARIABLE TESTS
# ============================================

def test_add_target_variables(merged_df, engineer):
    """Test target variable creation."""
    df = merged_df.copy()
    # Ensure return_1 exists
    df["return_1"] = df["close"].pct_change(periods=1)

    result = engineer.add_target_variables(df)

    assert "target_direction" in result.columns
    assert "target_return_pct" in result.columns
    assert "anomaly_label" in result.columns

    # Direction should be binary
    direction_vals = result["target_direction"].dropna().unique()
    assert set(direction_vals).issubset({0, 1})

    # Anomaly label should be binary
    anomaly_vals = result["anomaly_label"].dropna().unique()
    assert set(anomaly_vals).issubset({0, 1})


# ============================================
# FULL PIPELINE TESTS
# ============================================

def test_build_feature_matrix_from_dataframe(merged_df, engineer):
    """Test the full pipeline produces a clean, NaN-free DataFrame."""
    result = engineer.build_feature_matrix_from_dataframe(merged_df.copy())

    # Should have no NaN
    assert result.isna().sum().sum() == 0, "Feature matrix should have no NaN values"

    # Should still have rows
    assert len(result) > 0, "Feature matrix should not be empty"

    # Should have target columns
    assert "target_direction" in result.columns
    assert "anomaly_label" in result.columns


def test_get_feature_names(merged_df, engineer):
    """Test feature name extraction excludes metadata and targets."""
    result = engineer.build_feature_matrix_from_dataframe(merged_df.copy())
    feature_names = engineer.get_feature_names(result)

    # Should be a non-empty list
    assert len(feature_names) > 0

    # Should NOT include metadata or targets
    for col in feature_config.metadata_columns + feature_config.target_columns:
        assert col not in feature_names, f"{col} should be excluded from features"

    # Should NOT include raw OHLCV
    assert "open" not in feature_names
    assert "close" not in feature_names


def test_feature_matrix_column_count(merged_df, engineer):
    """Verify the feature matrix has a reasonable number of features."""
    result = engineer.build_feature_matrix_from_dataframe(merged_df.copy())
    features = engineer.get_feature_names(result)

    # We expect approximately 40-60 features
    assert len(features) >= 30, f"Too few features: {len(features)}"
    assert len(features) <= 80, f"Too many features: {len(features)}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

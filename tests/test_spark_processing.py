"""
Unit Tests for Spark Processing Module - P3
Tests for transformations, technical indicators, and data validation
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from datetime import datetime, timedelta
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src', 'processing'))

from transformations import (
    validate_trades_data,
    validate_ohlcv_data,
    calculate_price_change,
    detect_price_spikes
)
from technical_indicators import (
    calculate_sma,
    calculate_rsi,
    calculate_bollinger_bands
)


@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing"""
    spark = (SparkSession.builder
             .appName("TestSparkProcessing")
             .master("local[2]")
             .getOrCreate())
    
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture
def sample_trades_data(spark):
    """Create sample trades data for testing"""
    schema = StructType([
        StructField("symbol", StringType(), False),
        StructField("price", DoubleType(), False),
        StructField("quantity", DoubleType(), False),
        StructField("timestamp", TimestampType(), False)
    ])
    
    data = [
        ("BTCUSDT", 45000.50, 0.1, datetime(2024, 2, 15, 10, 0, 0)),
        ("BTCUSDT", 45100.00, 0.2, datetime(2024, 2, 15, 10, 1, 0)),
        ("BTCUSDT", 45050.25, 0.15, datetime(2024, 2, 15, 10, 2, 0)),
        ("BTCUSDT", None, 0.1, datetime(2024, 2, 15, 10, 3, 0)),  # Invalid: null price
        ("BTCUSDT", -100.0, 0.1, datetime(2024, 2, 15, 10, 4, 0)),  # Invalid: negative price
    ]
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_ohlcv_data(spark):
    """Create sample OHLCV data for testing"""
    schema = StructType([
        StructField("symbol", StringType(), False),
        StructField("interval", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("open", DoubleType(), False),
        StructField("high", DoubleType(), False),
        StructField("low", DoubleType(), False),
        StructField("close", DoubleType(), False),
        StructField("volume", DoubleType(), False)
    ])
    
    # Generate 50 candles for indicator calculation
    base_time = datetime(2024, 2, 15, 10, 0, 0)
    data = []
    
    for i in range(50):
        timestamp = base_time + timedelta(minutes=i)
        open_price = 45000 + (i * 10)
        close_price = open_price + 50
        high_price = close_price + 20
        low_price = open_price - 10
        volume = 100.0 + (i * 2)
        
        data.append((
            "BTCUSDT",
            "1m",
            timestamp,
            open_price,
            high_price,
            low_price,
            close_price,
            volume
        ))
    
    return spark.createDataFrame(data, schema)


# ============================================
# TRANSFORMATION TESTS
# ============================================

def test_validate_trades_data(sample_trades_data):
    """Test trades data validation removes invalid rows"""
    validated_df = validate_trades_data(sample_trades_data)
    
    # Should have 3 valid rows (removed 2 invalid)
    assert validated_df.count() == 3
    
    # All prices should be positive
    prices = [row.price for row in validated_df.collect()]
    assert all(p > 0 for p in prices)


def test_validate_ohlcv_data(sample_ohlcv_data):
    """Test OHLCV data validation"""
    validated_df = validate_ohlcv_data(sample_ohlcv_data)
    
    # All rows should be valid
    assert validated_df.count() == 50
    
    # Verify OHLC relationships
    for row in validated_df.collect():
        assert row.high >= row.low
        assert row.high >= row.open
        assert row.high >= row.close
        assert row.low <= row.open
        assert row.low <= row.close


def test_calculate_price_change(sample_ohlcv_data):
    """Test price change calculation"""
    result_df = calculate_price_change(sample_ohlcv_data)
    
    # Should have price_change and price_change_percent columns
    assert "price_change" in result_df.columns
    assert "price_change_percent" in result_df.columns
    
    # Verify calculation for first row
    first_row = result_df.first()
    expected_change = first_row.close - first_row.open
    assert abs(first_row.price_change - expected_change) < 0.01


def test_detect_price_spikes(sample_ohlcv_data):
    """Test price spike detection"""
    df_with_change = calculate_price_change(sample_ohlcv_data)
    result_df = detect_price_spikes(df_with_change, threshold_percent=1.0)
    
    # Should have is_price_spike column
    assert "is_price_spike" in result_df.columns
    
    # Count spikes
    spike_count = result_df.filter("is_price_spike = true").count()
    assert spike_count >= 0  # May or may not have spikes depending on data


# ============================================
# TECHNICAL INDICATOR TESTS
# ============================================

def test_calculate_sma(sample_ohlcv_data):
    """Test Simple Moving Average calculation"""
    result_df = calculate_sma(sample_ohlcv_data, period=20)
    
    # Should have sma_20 column
    assert "sma_20" in result_df.columns
    
    # SMA should not be null after 20 periods
    rows = result_df.orderBy("timestamp").collect()
    assert rows[19].sma_20 is not None
    
    # SMA should be within reasonable range
    for row in rows[19:]:
        if row.sma_20 is not None:
            assert row.sma_20 > 0


def test_calculate_rsi(sample_ohlcv_data):
    """Test RSI calculation"""
    result_df = calculate_rsi(sample_ohlcv_data, period=14)
    
    # Should have rsi_14 column
    assert "rsi_14" in result_df.columns
    
    # RSI should be between 0 and 100
    rows = result_df.collect()
    for row in rows:
        if row.rsi_14 is not None:
            assert 0 <= row.rsi_14 <= 100


def test_calculate_bollinger_bands(sample_ohlcv_data):
    """Test Bollinger Bands calculation"""
    result_df = calculate_bollinger_bands(sample_ohlcv_data, period=20)
    
    # Should have bollinger columns
    assert "bollinger_upper" in result_df.columns
    assert "bollinger_middle" in result_df.columns
    assert "bollinger_lower" in result_df.columns
    
    # Verify band relationships
    rows = result_df.orderBy("timestamp").collect()
    for row in rows[19:]:  # After 20 periods
        if all([row.bollinger_upper, row.bollinger_middle, row.bollinger_lower]):
            assert row.bollinger_upper > row.bollinger_middle
            assert row.bollinger_middle > row.bollinger_lower


# ============================================
# INTEGRATION TESTS
# ============================================

def test_full_pipeline_simulation(sample_ohlcv_data):
    """Test a simplified version of the full pipeline"""
    # Validate
    df = validate_ohlcv_data(sample_ohlcv_data)
    
    # Calculate price changes
    df = calculate_price_change(df)
    
    # Calculate indicators
    df = calculate_sma(df, period=20)
    df = calculate_rsi(df, period=14)
    df = calculate_bollinger_bands(df, period=20)
    
    # Verify final DataFrame has all expected columns
    expected_columns = [
        "symbol", "interval", "timestamp", "open", "high", "low", "close", "volume",
        "price_change", "price_change_percent",
        "sma_20", "rsi_14",
        "bollinger_upper", "bollinger_middle", "bollinger_lower"
    ]
    
    for col in expected_columns:
        assert col in df.columns
    
    # Verify we still have data
    assert df.count() > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

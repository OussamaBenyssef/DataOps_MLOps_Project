"""
Tests Unitaires — Métriques Agrégées (P3)
==========================================
Tests pour le module aggregated_metrics.py :
calcul des métriques agrégées (daily, hourly) à partir de données OHLCV.

Lancer:
    pytest tests/test_aggregated_metrics.py -v
    pytest tests/test_aggregated_metrics.py::TestAggregatedMetrics -v
    pytest tests/test_aggregated_metrics.py::TestAggregatedMetricsIntegration -v
"""

import pytest
from datetime import datetime, timedelta
from typing import List

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    TimestampType, IntegerType
)
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from processing.aggregated_metrics import (
    calculate_aggregated_metrics,
    calculate_daily_metrics,
    calculate_hourly_metrics,
    _window_to_interval_label,
    _get_trades_column,
)


# ─────────────────────── FIXTURES ──────────────────────────────────────────


@pytest.fixture(scope="session")
def spark():
    """SparkSession pour toute la session de test"""
    spark_session = (
        SparkSession.builder
        .appName("AggregatedMetrics-Tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    spark_session.sparkContext.setLogLevel("ERROR")
    yield spark_session
    spark_session.stop()


def make_ohlcv_rows_ts(
    n: int = 48,
    symbol: str = "BTCUSDT",
    interval_minutes: int = 60,
    base_time: datetime = None
) -> List[Row]:
    """
    Génère n bougies OHLCV avec timestamps réels pour tests d'agrégation.
    Par défaut: 48 bougies horaires (= 2 jours) pour tester daily aggregation.
    """
    if base_time is None:
        base_time = datetime(2024, 3, 1, 0, 0, 0)

    rows = []
    base_price = 45000.0
    for i in range(n):
        ts = base_time + timedelta(minutes=i * interval_minutes)
        close = base_price + (i % 12 - 6) * 200
        open_p = close - 50
        high = close + 150
        low = open_p - 100
        volume = 1000.0 + i * 50
        trades = 500 + i * 10
        rows.append(Row(
            symbol=symbol,
            interval="1m",
            timestamp=ts,
            open=float(open_p),
            high=float(high),
            low=float(low),
            close=float(close),
            volume=float(volume),
            trades_count=int(trades),
        ))
    return rows


@pytest.fixture
def sample_ohlcv_2days(spark):
    """48 bougies horaires = 2 jours complets de données BTCUSDT"""
    rows = make_ohlcv_rows_ts(n=48, symbol="BTCUSDT", interval_minutes=60)
    return spark.createDataFrame(rows)


@pytest.fixture
def sample_ohlcv_multi_symbol(spark):
    """Données multi-symboles: 24h BTCUSDT + 24h ETHUSDT"""
    btc_rows = make_ohlcv_rows_ts(n=24, symbol="BTCUSDT", interval_minutes=60)
    eth_rows = make_ohlcv_rows_ts(n=24, symbol="ETHUSDT", interval_minutes=60)
    return spark.createDataFrame(btc_rows + eth_rows)


@pytest.fixture
def sample_ohlcv_no_trades(spark):
    """Données OHLCV sans colonne trades_count"""
    schema = StructType([
        StructField("symbol", StringType(), False),
        StructField("interval", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("open", DoubleType(), False),
        StructField("high", DoubleType(), False),
        StructField("low", DoubleType(), False),
        StructField("close", DoubleType(), False),
        StructField("volume", DoubleType(), False),
    ])
    base_time = datetime(2024, 3, 1, 0, 0, 0)
    data = [
        ("BTCUSDT", "1m", base_time + timedelta(hours=i),
         45000.0 + i * 10, 45100.0 + i * 10,
         44900.0 + i * 10, 45050.0 + i * 10,
         float(1000 + i * 100))
        for i in range(24)
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_single_candle(spark):
    """Une seule bougie pour tester les cas limites"""
    rows = make_ohlcv_rows_ts(n=1, symbol="BTCUSDT", interval_minutes=60)
    return spark.createDataFrame(rows)


# ═══════════════════════════════════════════════════════════════════════════
# TEST 1: Helper Functions
# ═══════════════════════════════════════════════════════════════════════════


class TestHelperFunctions:
    """Tests pour les fonctions utilitaires internes"""

    def test_window_to_interval_label_day(self):
        assert _window_to_interval_label("1 day") == "1d"

    def test_window_to_interval_label_hour(self):
        assert _window_to_interval_label("1 hour") == "1h"

    def test_window_to_interval_label_4hours(self):
        assert _window_to_interval_label("4 hours") == "4h"

    def test_window_to_interval_label_week(self):
        assert _window_to_interval_label("1 week") == "1w"

    def test_window_to_interval_label_unknown(self):
        """Labels inconnus retournés tels quels"""
        assert _window_to_interval_label("30 minutes") == "30 minutes"

    def test_get_trades_column_trades_count(self, spark):
        df = spark.createDataFrame([Row(trades_count=10, price=100.0)])
        assert _get_trades_column(df) == "trades_count"

    def test_get_trades_column_num_trades(self, spark):
        df = spark.createDataFrame([Row(num_trades=10, price=100.0)])
        assert _get_trades_column(df) == "num_trades"

    def test_get_trades_column_trades(self, spark):
        df = spark.createDataFrame([Row(trades=10, price=100.0)])
        assert _get_trades_column(df) == "trades"

    def test_get_trades_column_none(self, spark):
        df = spark.createDataFrame([Row(price=100.0, volume=50.0)])
        assert _get_trades_column(df) is None


# ═══════════════════════════════════════════════════════════════════════════
# TEST 2: Aggregated Metrics Calculation
# ═══════════════════════════════════════════════════════════════════════════


class TestAggregatedMetrics:
    """Tests unitaires pour le calcul des métriques agrégées"""

    def test_daily_metrics_columns_exist(self, sample_ohlcv_2days):
        """calculate_daily_metrics produit toutes les colonnes attendues"""
        result = calculate_daily_metrics(sample_ohlcv_2days)
        expected_cols = [
            "symbol", "interval", "period_start", "period_end",
            "avg_price", "min_price", "max_price",
            "open_price", "close_price", "price_volatility",
            "price_range_pct", "period_return_pct",
            "total_volume", "avg_volume", "total_trades",
            "vwap", "num_candles", "calculated_at"
        ]
        for col_name in expected_cols:
            assert col_name in result.columns, f"Missing column: {col_name}"

    def test_daily_metrics_row_count(self, sample_ohlcv_2days):
        """48 bougies horaires sur 2 jours → 2 lignes daily"""
        result = calculate_daily_metrics(sample_ohlcv_2days)
        count = result.count()
        assert count == 2, f"Expected 2 daily rows, got {count}"

    def test_hourly_metrics_row_count(self, sample_ohlcv_2days):
        """
        48 bougies horaires → 48 lignes hourly
        (chaque bougie tombe dans sa propre fenêtre d'1h)
        """
        result = calculate_hourly_metrics(sample_ohlcv_2days)
        count = result.count()
        assert count == 48, f"Expected 48 hourly rows, got {count}"

    def test_interval_label_daily(self, sample_ohlcv_2days):
        """L'intervalle daily doit être '1d'"""
        result = calculate_daily_metrics(sample_ohlcv_2days)
        intervals = [row.interval for row in result.select("interval").distinct().collect()]
        assert intervals == ["1d"]

    def test_interval_label_hourly(self, sample_ohlcv_2days):
        """L'intervalle hourly doit être '1h'"""
        result = calculate_hourly_metrics(sample_ohlcv_2days)
        intervals = [row.interval for row in result.select("interval").distinct().collect()]
        assert intervals == ["1h"]

    def test_avg_price_within_range(self, sample_ohlcv_2days):
        """avg_price doit être entre min_price et max_price"""
        result = calculate_daily_metrics(sample_ohlcv_2days)
        for row in result.collect():
            assert row.min_price <= row.avg_price <= row.max_price, \
                f"avg_price {row.avg_price} not in [{row.min_price}, {row.max_price}]"

    def test_min_price_le_max_price(self, sample_ohlcv_2days):
        """min_price <= max_price toujours"""
        result = calculate_daily_metrics(sample_ohlcv_2days)
        for row in result.collect():
            assert row.min_price <= row.max_price

    def test_total_volume_positive(self, sample_ohlcv_2days):
        """Le volume total doit être positif"""
        result = calculate_daily_metrics(sample_ohlcv_2days)
        for row in result.collect():
            assert row.total_volume > 0

    def test_vwap_within_price_range(self, sample_ohlcv_2days):
        """VWAP doit être entre min et max price"""
        result = calculate_daily_metrics(sample_ohlcv_2days)
        for row in result.collect():
            assert row.min_price <= row.vwap <= row.max_price, \
                f"VWAP {row.vwap} not in [{row.min_price}, {row.max_price}]"

    def test_num_candles_daily(self, sample_ohlcv_2days):
        """Chaque jour doit avoir 24 bougies"""
        result = calculate_daily_metrics(sample_ohlcv_2days)
        for row in result.collect():
            assert row.num_candles == 24, f"Expected 24 candles, got {row.num_candles}"

    def test_price_range_pct_positive(self, sample_ohlcv_2days):
        """price_range_pct doit être >= 0"""
        result = calculate_daily_metrics(sample_ohlcv_2days)
        for row in result.collect():
            assert row.price_range_pct >= 0

    def test_total_trades_aggregated(self, sample_ohlcv_2days):
        """total_trades doit être la somme des trades_count"""
        result = calculate_daily_metrics(sample_ohlcv_2days)
        for row in result.collect():
            assert row.total_trades > 0

    def test_multi_symbol_aggregation(self, sample_ohlcv_multi_symbol):
        """Les métriques sont calculées séparément par symbole"""
        result = calculate_daily_metrics(sample_ohlcv_multi_symbol)
        symbols = sorted([
            row.symbol for row in result.select("symbol").distinct().collect()
        ])
        assert symbols == ["BTCUSDT", "ETHUSDT"]

    def test_multi_symbol_correct_count(self, sample_ohlcv_multi_symbol):
        """24h par symbole → 1 jour par symbole → 2 lignes total"""
        result = calculate_daily_metrics(sample_ohlcv_multi_symbol)
        assert result.count() == 2

    def test_no_trades_column_handled(self, sample_ohlcv_no_trades):
        """Données sans colonne trades → total_trades = 0"""
        result = calculate_daily_metrics(sample_ohlcv_no_trades)
        for row in result.collect():
            assert row.total_trades == 0

    def test_single_candle_no_crash(self, sample_single_candle):
        """Une seule bougie ne doit pas provoquer d'erreur"""
        result = calculate_daily_metrics(sample_single_candle)
        assert result.count() == 1

    def test_single_candle_volatility_zero(self, sample_single_candle):
        """Volatilité = 0 pour une seule bougie"""
        result = calculate_daily_metrics(sample_single_candle)
        row = result.first()
        assert row.price_volatility == 0.0

    def test_calculated_at_not_null(self, sample_ohlcv_2days):
        """calculated_at doit être renseigné"""
        result = calculate_daily_metrics(sample_ohlcv_2days)
        nulls = result.filter(F.col("calculated_at").isNull()).count()
        assert nulls == 0

    def test_period_start_before_period_end(self, sample_ohlcv_2days):
        """period_start < period_end"""
        result = calculate_daily_metrics(sample_ohlcv_2days)
        for row in result.collect():
            assert row.period_start <= row.period_end


# ═══════════════════════════════════════════════════════════════════════════
# TEST 3: Custom Window Duration
# ═══════════════════════════════════════════════════════════════════════════


class TestCustomWindowDuration:
    """Tests avec des durées de fenêtre personnalisées"""

    def test_4hour_window(self, sample_ohlcv_2days):
        """48 bougies horaires avec fenêtre de 4h → 12 groupes"""
        result = calculate_aggregated_metrics(
            sample_ohlcv_2days,
            window_duration="4 hours"
        )
        count = result.count()
        assert count == 12, f"Expected 12 groups for 4h window, got {count}"

    def test_4hour_interval_label(self, sample_ohlcv_2days):
        """La fenêtre 4h doit avoir l'intervalle '4h'"""
        result = calculate_aggregated_metrics(
            sample_ohlcv_2days,
            window_duration="4 hours"
        )
        intervals = [row.interval for row in result.select("interval").distinct().collect()]
        assert intervals == ["4h"]


# ═══════════════════════════════════════════════════════════════════════════
# TEST 4: Integration with Cleaning + Indicators Pipeline
# ═══════════════════════════════════════════════════════════════════════════


class TestAggregatedMetricsIntegration:
    """Tests d'intégration : nettoyage → indicateurs → métriques agrégées"""

    def test_full_pipeline_with_aggregation(self, spark):
        """
        Pipeline complet: OHLCV → validate → price change → aggregated metrics
        """
        from processing.transformations import (
            validate_ohlcv_data,
            calculate_price_change,
        )

        # Create 48 hours of simulated data
        rows = make_ohlcv_rows_ts(n=48, symbol="BTCUSDT", interval_minutes=60)
        df_raw = spark.createDataFrame(rows)

        # Validate
        df_valid = validate_ohlcv_data(df_raw)
        assert df_valid.count() == 48

        # Calculate price change
        df_pc = calculate_price_change(df_valid)

        # Calculate aggregated metrics
        df_agg = calculate_daily_metrics(df_pc)
        assert df_agg.count() == 2

        # Verify all columns are present
        expected = [
            "symbol", "interval", "period_start", "period_end",
            "avg_price", "min_price", "max_price",
            "total_volume", "vwap", "num_candles"
        ]
        for col_name in expected:
            assert col_name in df_agg.columns

        # Verify metrics are valid
        for row in df_agg.collect():
            assert row.avg_price > 0
            assert row.total_volume > 0
            assert row.num_candles == 24
            assert row.vwap > 0

    def test_aggregation_preserves_symbol_grouping(self, spark):
        """
        Multi-symboles: les métriques sont correctement groupées par symbole
        """
        btc_rows = make_ohlcv_rows_ts(n=24, symbol="BTCUSDT")
        eth_rows = make_ohlcv_rows_ts(n=24, symbol="ETHUSDT")
        bnb_rows = make_ohlcv_rows_ts(n=24, symbol="BNBUSDT")
        df = spark.createDataFrame(btc_rows + eth_rows + bnb_rows)

        result = calculate_daily_metrics(df)
        assert result.count() == 3

        symbols = sorted([
            row.symbol for row in result.select("symbol").distinct().collect()
        ])
        assert symbols == ["BNBUSDT", "BTCUSDT", "ETHUSDT"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

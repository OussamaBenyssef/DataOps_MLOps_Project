"""
Tests Unitaires + Intégration — Pipeline ETL Spark (P3)
=========================================================
Tests pour le nettoyage des données, la validation, les indicateurs
techniques et le pipeline complet. PySpark en mode local.

Lancer:
    pytest tests/test_spark_processing.py -v
    pytest tests/test_spark_processing.py::TestDataCleaning -v
    pytest tests/test_spark_processing.py::TestTechnicalIndicators -v
    pytest tests/test_spark_processing.py::TestIntegration -v
"""

import pytest
from datetime import datetime, timedelta
from typing import List

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from processing.config import CleaningConfig, TechnicalIndicatorsConfig
from processing.data_cleaning import (
    handle_null_values,
    validate_ohlcv_constraints,
    validate_trades_constraints,
    detect_outliers_zscore,
    detect_price_spikes,
    deduplicate_ohlcv,
    normalize_timestamp,
    normalize_symbol,
    clean_ohlcv_data,
)
from processing.transformations import (
    validate_trades_data,
    validate_ohlcv_data,
    calculate_price_change,
    detect_price_spikes as detect_price_spikes_transform,
)
from processing.technical_indicators import (
    calculate_sma,
    calculate_rsi,
    calculate_macd,
    calculate_bollinger_bands,
    calculate_all_indicators,
)


# ─────────────────────── FIXTURES ──────────────────────────────────────────

@pytest.fixture(scope="session")
def spark():
    """SparkSession pour toute la session de test"""
    spark_session = (
        SparkSession.builder
        .appName("ETL-Tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    spark_session.sparkContext.setLogLevel("ERROR")
    yield spark_session
    spark_session.stop()


@pytest.fixture
def cleaning_config():
    return CleaningConfig()


@pytest.fixture
def indicators_config():
    return TechnicalIndicatorsConfig()


def make_ohlcv_rows(n: int = 30, symbol: str = "BTCUSDT") -> List[Row]:
    """Génère n bougies OHLCV simulées pour les tests"""
    now_ms = int(datetime.utcnow().timestamp() * 1000)
    rows = []
    base = 45000.0
    for i in range(n):
        ts = now_ms + i * 60_000
        close = base + (i % 10 - 5) * 200
        open_p = close - 50
        high  = close + 150
        low   = open_p - 150
        rows.append(Row(
            symbol=symbol,
            interval="1m",
            timestamp=ts,
            open=float(open_p),
            high=float(high),
            low=float(low),
            close=float(close),
            volume=float(1000 + i * 100),
            trades=500 + i,
        ))
    return rows


@pytest.fixture
def sample_trades_data(spark):
    """Données de trades pour tests"""
    schema = StructType([
        StructField("symbol", StringType(), False),
        StructField("price", DoubleType(), True),
        StructField("quantity", DoubleType(), False),
        StructField("timestamp", TimestampType(), False),
    ])
    data = [
        ("BTCUSDT", 45000.50, 0.1, datetime(2024, 2, 15, 10, 0, 0)),
        ("BTCUSDT", 45100.00, 0.2, datetime(2024, 2, 15, 10, 1, 0)),
        ("BTCUSDT", 45050.25, 0.15, datetime(2024, 2, 15, 10, 2, 0)),
        ("BTCUSDT", None, 0.1, datetime(2024, 2, 15, 10, 3, 0)),
        ("BTCUSDT", -100.0, 0.1, datetime(2024, 2, 15, 10, 4, 0)),
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_ohlcv_data(spark):
    """Données OHLCV pour tests"""
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
    base_time = datetime(2024, 2, 15, 10, 0, 0)
    data = [
        ("BTCUSDT", "1m", base_time + timedelta(minutes=i),
         float(45000 + i * 10), float(45000 + i * 10 + 70),
         float(45000 + i * 10 - 10), float(45000 + i * 10 + 50),
         float(100.0 + i * 2))
        for i in range(50)
    ]
    return spark.createDataFrame(data, schema)


# ═══════════════════════════════════════════════════════════════════════════
# TEST 1: Data Cleaning (data_cleaning.py)
# ═══════════════════════════════════════════════════════════════════════════

class TestDataCleaning:
    """Tests unitaires pour le module data_cleaning.py"""

    def test_normalize_symbol_uppercase(self, spark):
        """Les symboles doivent être normalisés en majuscules"""
        rows = [Row(symbol="btcusdt", interval="1m", timestamp=1000, open=100.0,
                    high=110.0, low=90.0, close=105.0, volume=1000.0, trades=100)]
        df = spark.createDataFrame(rows)
        df_norm = normalize_symbol(df)
        result = df_norm.select("symbol").first()["symbol"]
        assert result == "BTCUSDT"

    def test_normalize_timestamp_creates_event_time(self, spark):
        """normalize_timestamp doit créer la colonne event_time"""
        rows = [Row(symbol="BTCUSDT", interval="1m", timestamp=1_700_000_000_000,
                    open=100.0, high=110.0, low=90.0, close=105.0, volume=1000.0, trades=10)]
        df = spark.createDataFrame(rows)
        df_norm = normalize_timestamp(df)
        assert "event_time" in df_norm.columns

    def test_handle_null_values_drops_null_required(self, spark, cleaning_config):
        """Les lignes avec valeurs nulles critiques doivent être supprimées"""
        rows = [
            Row(symbol="BTCUSDT", interval="1m", timestamp=1000,
                open=100.0, high=110.0, low=90.0, close=105.0, volume=1000.0, trades=100),
            Row(symbol=None, interval="1m", timestamp=1000,
                open=100.0, high=110.0, low=90.0, close=105.0, volume=1000.0, trades=100),
        ]
        df = spark.createDataFrame(rows)
        df_clean, dropped = handle_null_values(df, cleaning_config)
        assert dropped == 1
        assert df_clean.count() == 1

    def test_validate_ohlcv_rejects_high_less_than_low(self, spark, cleaning_config):
        """Les bougies avec high < low doivent être rejetées"""
        rows = [
            Row(symbol="BTCUSDT", interval="1m", timestamp=1000,
                open=100.0, high=90.0, low=110.0, close=105.0, volume=1000.0, trades=10),
            Row(symbol="BTCUSDT", interval="1m", timestamp=2000,
                open=100.0, high=120.0, low=80.0, close=110.0, volume=1000.0, trades=10),
        ]
        df = spark.createDataFrame(rows)
        df_valid, df_rejected = validate_ohlcv_constraints(df, cleaning_config)
        assert df_valid.count() == 1
        assert df_rejected.count() == 1

    def test_validate_ohlcv_rejects_negative_price(self, spark, cleaning_config):
        """Les prix négatifs doivent être rejetés"""
        rows = [
            Row(symbol="BTCUSDT", interval="1m", timestamp=1000,
                open=-100.0, high=110.0, low=-90.0, close=105.0, volume=1000.0, trades=10),
            Row(symbol="BTCUSDT", interval="1m", timestamp=2000,
                open=100.0, high=120.0, low=80.0, close=110.0, volume=1000.0, trades=10),
        ]
        df = spark.createDataFrame(rows)
        df_valid, df_rejected = validate_ohlcv_constraints(df, cleaning_config)
        assert df_valid.count() == 1

    def test_deduplicate_removes_duplicates(self, spark):
        """La déduplication supprime les doublons (symbol, interval, timestamp)"""
        rows = [
            Row(symbol="BTCUSDT", interval="1m", timestamp=1000,
                open=100.0, high=110.0, low=90.0, close=105.0, volume=1000.0, trades=10),
            Row(symbol="BTCUSDT", interval="1m", timestamp=1000,
                open=101.0, high=111.0, low=91.0, close=106.0, volume=1100.0, trades=11),
        ]
        df = spark.createDataFrame(rows)
        df_dedup = deduplicate_ohlcv(df)
        assert df_dedup.count() == 1

    def test_detect_outliers_with_zscore(self, spark, cleaning_config):
        """Les outliers Z-score doivent être marqués is_outlier=True"""
        rows = make_ohlcv_rows(25, "BTCUSDT")
        rows.append(Row(
            symbol="BTCUSDT", interval="1m",
            timestamp=rows[-1].timestamp + 60000,
            open=1_000_000.0, high=1_100_000.0, low=900_000.0, close=1_050_000.0,
            volume=1000.0, trades=50
        ))
        df = spark.createDataFrame(rows)
        df_flagged = detect_outliers_zscore(df, cleaning_config)
        outliers = df_flagged.filter(F.col("is_outlier") == True).count()
        assert outliers >= 1

    def test_clean_ohlcv_data_full_pipeline(self, spark, cleaning_config):
        """Le pipeline complet conserve des données valides"""
        rows = make_ohlcv_rows(30, "BTCUSDT")
        df = spark.createDataFrame(rows)
        df_clean, metrics = clean_ohlcv_data(df, cleaning_config)
        assert metrics["final_count"] > 0
        assert metrics["quality_rate"] >= 90.0


# ═══════════════════════════════════════════════════════════════════════════
# TEST 2: Transformations (transformations.py)
# ═══════════════════════════════════════════════════════════════════════════

class TestTransformations:
    """Tests pour transformations.py"""

    def test_validate_trades_data(self, sample_trades_data):
        """Supprime trades invalides (null price, negative price)"""
        validated = validate_trades_data(sample_trades_data)
        assert validated.count() == 3
        prices = [row.price for row in validated.collect()]
        assert all(p > 0 for p in prices)

    def test_validate_ohlcv_data(self, sample_ohlcv_data):
        """Valide les contraintes OHLCV"""
        validated = validate_ohlcv_data(sample_ohlcv_data)
        assert validated.count() == 50
        for row in validated.collect():
            assert row.high >= row.low


    def test_calculate_price_change(self, sample_ohlcv_data):
        """Calcule price_change et price_change_percent"""
        result = calculate_price_change(sample_ohlcv_data)
        assert "price_change" in result.columns
        assert "price_change_percent" in result.columns
        first = result.first()
        expected = first.close - first.open
        assert abs(first.price_change - expected) < 0.01

    def test_detect_price_spikes(self, sample_ohlcv_data):
        """Détecte les price spikes"""
        df = calculate_price_change(sample_ohlcv_data)
        result = detect_price_spikes_transform(df, threshold_percent=1.0)
        assert "is_price_spike" in result.columns


# ═══════════════════════════════════════════════════════════════════════════
# TEST 3: Technical Indicators (technical_indicators.py)
# ═══════════════════════════════════════════════════════════════════════════

class TestTechnicalIndicators:
    """Tests des indicateurs techniques RSI, MACD, Bollinger"""

    def test_sma_20_is_calculated(self, sample_ohlcv_data):
        """SMA_20 calculée pour lignes avec ≥ 20 données"""
        result = calculate_sma(sample_ohlcv_data, period=20)
        assert "sma_20" in result.columns
        rows = result.orderBy("timestamp").collect()
        assert rows[19].sma_20 is not None

    def test_rsi_range_0_to_100(self, sample_ohlcv_data):
        """RSI doit être entre 0 et 100"""
        result = calculate_rsi(sample_ohlcv_data, period=14)
        assert "rsi_14" in result.columns
        rsi_notnull = result.filter(F.col("rsi_14").isNotNull())
        if rsi_notnull.count() > 0:
            out_of_range = rsi_notnull.filter(
                (F.col("rsi_14") < 0) | (F.col("rsi_14") > 100)
            ).count()
            assert out_of_range == 0

    def test_bollinger_upper_ge_middle_ge_lower(self, sample_ohlcv_data):
        """Bollinger: upper >= middle >= lower"""
        result = calculate_bollinger_bands(sample_ohlcv_data, period=20)
        assert "bollinger_upper" in result.columns
        rows = result.orderBy("timestamp").collect()
        for row in rows[19:]:
            if all([row.bollinger_upper, row.bollinger_middle, row.bollinger_lower]):
                assert row.bollinger_upper >= row.bollinger_middle
                assert row.bollinger_middle >= row.bollinger_lower

    def test_macd_columns_exist(self, sample_ohlcv_data):
        """MACD calcule les 3 colonnes: macd, macd_signal, macd_histogram"""
        result = calculate_macd(sample_ohlcv_data)
        for col in ["macd", "macd_signal", "macd_histogram"]:
            assert col in result.columns

    def test_calculate_all_indicators(self, sample_ohlcv_data):
        """calculate_all_indicators ajoute toutes les colonnes"""
        result = calculate_all_indicators(sample_ohlcv_data)
        expected = ["sma_20", "rsi_14", "bollinger_upper", "bollinger_middle",
                     "bollinger_lower", "macd", "macd_signal", "macd_histogram"]
        for col in expected:
            assert col in result.columns

    def test_row_count_unchanged(self, sample_ohlcv_data):
        """Le nombre de lignes ne change pas après calcul des indicateurs"""
        count = sample_ohlcv_data.count()
        result = calculate_all_indicators(sample_ohlcv_data)
        assert result.count() == count


# ═══════════════════════════════════════════════════════════════════════════
# TEST 4: Intégration E2E
# ═══════════════════════════════════════════════════════════════════════════

class TestIntegration:
    """Test d'intégration complet du pipeline ETL"""

    def test_full_etl_pipeline(self, spark, cleaning_config):
        """
        Pipeline complet: données brutes → nettoyage → indicateurs techniques
        """
        # 1. Données brutes (simulées) avec 1 invalide
        rows = make_ohlcv_rows(30, "ETHUSDT")
        rows.append(Row(
            symbol="ETHUSDT", interval="1m", timestamp=9999999999,
            open=-1.0, high=100.0, low=50.0, close=80.0, volume=100.0, trades=10
        ))
        df_raw = spark.createDataFrame(rows)
        assert df_raw.count() == 31

        # 2. Nettoyage
        df_clean, metrics = clean_ohlcv_data(df_raw, cleaning_config)
        assert df_clean.count() == 30
        assert "event_time" in df_clean.columns
        assert metrics["invalid_ohlcv"] >= 1

        # 3. Calcul des indicateurs (drop outlier cols for compatibility)
        df_indicators = df_clean.drop("zscore_close", "is_outlier")
        df_final = calculate_all_indicators(df_indicators)
        assert df_final.count() == 30

        # 4. Vérification qualité RSI
        df_rsi = df_final.filter(F.col("rsi_14").isNotNull())
        if df_rsi.count() > 0:
            rsi_invalid = df_rsi.filter(
                (F.col("rsi_14") < 0) | (F.col("rsi_14") > 100)
            ).count()
            assert rsi_invalid == 0

    def test_full_pipeline_with_transformations(self, sample_ohlcv_data):
        """Pipeline simplifié: validation → price change → indicateurs"""
        df = validate_ohlcv_data(sample_ohlcv_data)
        df = calculate_price_change(df)
        df = calculate_sma(df, period=20)
        df = calculate_rsi(df, period=14)
        df = calculate_bollinger_bands(df, period=20)

        expected = [
            "symbol", "interval", "timestamp", "open", "high", "low", "close", "volume",
            "price_change", "price_change_percent",
            "sma_20", "rsi_14", "bollinger_upper", "bollinger_middle", "bollinger_lower"
        ]
        for col in expected:
            assert col in df.columns
        assert df.count() > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

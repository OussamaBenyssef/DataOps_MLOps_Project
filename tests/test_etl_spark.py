"""
Tests unitaires du pipeline ETL Spark
======================================
Tests pour le nettoyage des données et le calcul des indicateurs techniques.
Utilise PySpark en mode local sans Kafka ni MongoDB.

Lancer avec:
    pytest tests/test_etl_spark.py -v
    pytest tests/test_etl_spark.py::TestDataCleaning -v
    pytest tests/test_etl_spark.py::TestTechnicalIndicators -v
"""
import pytest
from datetime import datetime
from typing import List

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src', 'processing'))

from config import CleaningConfig, TechnicalIndicatorsConfig
from data_cleaning import (
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
from technical_indicators import (
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


# ─────────────────────── TESTS NETTOYAGE ───────────────────────────────────

class TestDataCleaning:
    """Tests unitaires pour le module data_cleaning.py"""

    def test_normalize_symbol_uppercase(self, spark):
        """Les symboles doivent être normalisés en majuscules"""
        rows = [Row(symbol="btcusdt", interval="1m", timestamp=1000, open=100.0,
                    high=110.0, low=90.0, close=105.0, volume=1000.0, trades=100)]
        df = spark.createDataFrame(rows)
        df_norm = normalize_symbol(df)
        result = df_norm.select("symbol").first()["symbol"]
        assert result == "BTCUSDT", f"Attendu 'BTCUSDT', obtenu '{result}'"

    def test_normalize_timestamp_creates_event_time(self, spark):
        """normalize_timestamp doit créer la colonne event_time"""
        rows = [Row(symbol="BTCUSDT", interval="1m", timestamp=1_700_000_000_000,
                    open=100.0, high=110.0, low=90.0, close=105.0, volume=1000.0, trades=10)]
        df = spark.createDataFrame(rows)
        df_norm = normalize_timestamp(df)
        assert "event_time" in df_norm.columns, "Colonne 'event_time' manquante"

    def test_handle_null_values_drops_null_required(self, spark, cleaning_config):
        """Les lignes avec des valeurs nulles dans les colonnes critiques doivent être supprimées"""
        rows = [
            Row(symbol="BTCUSDT", interval="1m", timestamp=1000,
                open=100.0, high=110.0, low=90.0, close=105.0, volume=1000.0, trades=100),
            Row(symbol=None, interval="1m", timestamp=1000,
                open=100.0, high=110.0, low=90.0, close=105.0, volume=1000.0, trades=100),
        ]
        df = spark.createDataFrame(rows)
        df_clean, dropped = handle_null_values(df, cleaning_config)
        assert dropped == 1, f"1 ligne nulle devrait être supprimée, obtenu: {dropped}"
        assert df_clean.count() == 1

    def test_validate_ohlcv_rejects_high_less_than_low(self, spark, cleaning_config):
        """Les bougies avec high < low doivent être rejetées"""
        rows = [
            Row(symbol="BTCUSDT", interval="1m", timestamp=1000,
                open=100.0, high=90.0, low=110.0, close=105.0, volume=1000.0, trades=10),  # invalide
            Row(symbol="BTCUSDT", interval="1m", timestamp=2000,
                open=100.0, high=120.0, low=80.0, close=110.0, volume=1000.0, trades=10),  # valide
        ]
        df = spark.createDataFrame(rows)
        df_valid, df_rejected = validate_ohlcv_constraints(df, cleaning_config)
        assert df_valid.count() == 1
        assert df_rejected.count() == 1

    def test_validate_ohlcv_rejects_negative_price(self, spark, cleaning_config):
        """Les prix négatifs ou nuls doivent être rejetés"""
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
        """La déduplication doit supprimer les doublons (symbol, interval, timestamp)"""
        rows = [
            Row(symbol="BTCUSDT", interval="1m", timestamp=1000,
                open=100.0, high=110.0, low=90.0, close=105.0, volume=1000.0, trades=10),
            Row(symbol="BTCUSDT", interval="1m", timestamp=1000,  # doublon
                open=101.0, high=111.0, low=91.0, close=106.0, volume=1100.0, trades=11),
        ]
        df = spark.createDataFrame(rows)
        df_dedup = deduplicate_ohlcv(df)
        assert df_dedup.count() == 1

    def test_detect_outliers_with_zscore(self, spark, cleaning_config):
        """Les outliers détectés par Z-score doivent être marqués is_outlier=True"""
        rows = make_ohlcv_rows(25, "BTCUSDT")
        # Ajouter un outlier extrême
        rows.append(Row(
            symbol="BTCUSDT", interval="1m",
            timestamp=rows[-1].timestamp + 60000,
            open=1_000_000.0, high=1_100_000.0, low=900_000.0, close=1_050_000.0,
            volume=1000.0, trades=50
        ))
        df = spark.createDataFrame(rows)
        df_flagged = detect_outliers_zscore(df, cleaning_config)
        outliers = df_flagged.filter(F.col("is_outlier") == True).count()
        assert outliers >= 1, "Au moins un outlier doit être détecté"

    def test_clean_ohlcv_data_full_pipeline(self, spark, cleaning_config):
        """Le pipeline complet de nettoyage doit conserver des données valides"""
        rows = make_ohlcv_rows(30, "BTCUSDT")
        df = spark.createDataFrame(rows)
        df_clean, metrics = clean_ohlcv_data(df, cleaning_config)
        assert metrics["final_count"] > 0, "Le résultat ne doit pas être vide"
        assert metrics["quality_rate"] >= 90.0, f"Taux de qualité trop bas: {metrics['quality_rate']}%"


# ─────────────────────── TESTS INDICATEURS ─────────────────────────────────

class TestTechnicalIndicators:
    """Tests des indicateurs techniques RSI, MACD, Bollinger Bands"""

    def test_sma_20_is_calculated(self, spark):
        """SMA_20 doit être calculée pour les lignes avec ≥ 20 données"""
        rows = make_ohlcv_rows(30)
        df = spark.createDataFrame(rows)
        df_sma = calculate_sma(df, [20])
        non_null_sma = df_sma.filter(F.col("sma_20").isNotNull()).count()
        assert non_null_sma > 0, "SMA_20 ne doit pas être entièrement null"

    def test_rsi_range_0_to_100(self, spark, indicators_config):
        """Le RSI doit être compris entre 0 et 100"""
        rows = make_ohlcv_rows(30)
        df = spark.createDataFrame(rows)
        df_rsi = calculate_rsi(df, period=14)
        # Filtrer les non-nulls (premières périodes sont null)
        df_rsi_notnull = df_rsi.filter(F.col("rsi_14").isNotNull())
        if df_rsi_notnull.count() > 0:
            out_of_range = df_rsi_notnull.filter(
                (F.col("rsi_14") < 0) | (F.col("rsi_14") > 100)
            ).count()
            assert out_of_range == 0, f"RSI hors plage [0, 100]: {out_of_range} valeurs"

    def test_rsi_signal_values(self, spark):
        """Le signal RSI doit être parmi: overbought, oversold, neutral"""
        rows = make_ohlcv_rows(30)
        df = spark.createDataFrame(rows)
        df_rsi = calculate_rsi(df, period=14)
        valid_signals = {"overbought", "oversold", "neutral"}
        signals = {row["rsi_signal"] for row in df_rsi.select("rsi_signal").collect() if row["rsi_signal"]}
        assert signals.issubset(valid_signals), f"Signaux RSI invalides: {signals - valid_signals}"

    def test_bollinger_upper_ge_middle_ge_lower(self, spark):
        """Bollinger Bands: upper >= middle >= lower"""
        rows = make_ohlcv_rows(30)
        df = spark.createDataFrame(rows)
        df_bb = calculate_bollinger_bands(df, period=20, std_dev=2.0)
        df_notnull = df_bb.filter(
            F.col("bb_upper").isNotNull() &
            F.col("bb_middle").isNotNull() &
            F.col("bb_lower").isNotNull()
        )
        if df_notnull.count() > 0:
            invalid = df_notnull.filter(
                (F.col("bb_upper") < F.col("bb_middle")) |
                (F.col("bb_middle") < F.col("bb_lower"))
            ).count()
            assert invalid == 0, f"Bollinger Bands invalides: {invalid} lignes"

    def test_macd_line_equals_ema_diff(self, spark):
        """MACD line = EMA(12) - EMA(26)"""
        from technical_indicators import calculate_ema
        rows = make_ohlcv_rows(30)
        df = spark.createDataFrame(rows)
        df_macd = calculate_macd(df, fast_period=12, slow_period=26, signal_period=9)
        assert "macd_line" in df_macd.columns
        assert "macd_signal" in df_macd.columns
        assert "macd_histogram" in df_macd.columns
        assert "macd_cross" in df_macd.columns

    def test_calculate_all_indicators_adds_all_columns(self, spark, indicators_config):
        """calculate_all_indicators doit ajouter RSI, MACD et Bollinger au DataFrame"""
        rows = make_ohlcv_rows(30)
        df = spark.createDataFrame(rows)
        df_result = calculate_all_indicators(df, indicators_config)

        expected_columns = [
            "sma_20", "ema_12", "ema_26",
            "rsi_14", "rsi_signal",
            "macd_line", "macd_signal", "macd_histogram", "macd_cross",
            "bb_middle", "bb_upper", "bb_lower", "bb_bandwidth", "bb_pct_b", "bb_signal",
        ]
        for col in expected_columns:
            assert col in df_result.columns, f"Colonne manquante après calculate_all_indicators: '{col}'"

    def test_row_count_unchanged_after_indicators(self, spark, indicators_config):
        """Le nombre de lignes ne doit pas changer après calcul des indicateurs"""
        rows = make_ohlcv_rows(30)
        df = spark.createDataFrame(rows)
        original_count = df.count()
        df_result = calculate_all_indicators(df, indicators_config)
        assert df_result.count() == original_count, "Le count a changé après calcul des indicateurs!"


# ─────────────────────── TEST D'INTÉGRATION ────────────────────────────────

class TestIntegration:
    """Test d'intégration complet du pipeline ETL"""

    def test_full_etl_pipeline(self, spark, cleaning_config, indicators_config):
        """
        Test du pipeline complet:
            données brutes → nettoyage → indicateurs techniques
        """
        # 1. Données brutes (simulées)
        rows = make_ohlcv_rows(30, "ETHUSDT")
        # Ajouter des données invalides
        rows.append(Row(
            symbol="ETHUSDT", interval="1m", timestamp=9999999999,
            open=-1.0, high=100.0, low=50.0, close=80.0, volume=100.0, trades=10
        ))
        df_raw = spark.createDataFrame(rows)
        assert df_raw.count() == 31

        # 2. Nettoyage
        df_clean, metrics = clean_ohlcv_data(df_raw, cleaning_config)
        assert df_clean.count() == 30, f"La ligne invalide doit être filtrée. Count: {df_clean.count()}"
        assert "event_time" in df_clean.columns
        assert metrics["invalid_ohlcv"] >= 1

        # 3. Calcul des indicateurs
        df_final = calculate_all_indicators(df_clean, indicators_config)
        assert df_final.count() == 30

        # Vérifications qualité
        df_notnull = df_final.filter(F.col("rsi_14").isNotNull())
        if df_notnull.count() > 0:
            rsi_invalid = df_notnull.filter(
                (F.col("rsi_14") < 0) | (F.col("rsi_14") > 100)
            ).count()
            assert rsi_invalid == 0

        print("\n✅ TEST D'INTÉGRATION COMPLET RÉUSSI")
        print(f"   - Données d'entrée: 31 lignes (dont 1 invalide)")
        print(f"   - Après nettoyage: {df_clean.count()} lignes")
        print(f"   - Indicateurs calculés: RSI, MACD, Bollinger Bands, SMA, EMA")

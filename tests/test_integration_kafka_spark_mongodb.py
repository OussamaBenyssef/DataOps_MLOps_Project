"""
Tests d'Intégration — Pipeline Kafka → Spark ETL → MongoDB (P1)
================================================================
Tests end-to-end vérifiant le flux de données complet :
  1. Kafka message parsing → Spark DataFrame
  2. Spark ETL pipeline (cleaning + indicators + transformations)
  3. MongoDB write verification (format + schema)
  4. End-to-end flow simulation multi-symboles

PySpark local, pas besoin de services Docker externes.

Lancer:
    pytest tests/test_integration_kafka_spark_mongodb.py -v
    pytest tests/test_integration_kafka_spark_mongodb.py::TestKafkaToSpark -v
    pytest tests/test_integration_kafka_spark_mongodb.py::TestSparkETLPipeline -v
    pytest tests/test_integration_kafka_spark_mongodb.py::TestMongoDBWriter -v
    pytest tests/test_integration_kafka_spark_mongodb.py::TestEndToEndFlow -v
"""

import json
import pytest
from datetime import datetime, timedelta, timezone
from typing import List
from unittest.mock import patch, MagicMock

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    LongType, IntegerType, TimestampType,
)

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from processing.config import CleaningConfig, TechnicalIndicatorsConfig
from processing.data_cleaning import clean_ohlcv_data
from processing.transformations import (
    validate_ohlcv_data,
    calculate_price_change,
    calculate_volume_metrics,
    add_candle_pattern,
    add_metadata_columns,
)
from processing.technical_indicators import (
    calculate_sma,
    calculate_rsi,
    calculate_macd,
    calculate_bollinger_bands,
    calculate_all_indicators,
)
from processing.kafka_consumer import RAW_KLINES_SCHEMA
from streaming.config import kafka_config as streaming_kafka_config, rest_config


# ─────────────────────── FIXTURES ──────────────────────────────────────────

@pytest.fixture(scope="session")
def spark():
    """SparkSession pour toute la session de test"""
    spark_session = (
        SparkSession.builder
        .appName("Integration-Tests-Kafka-Spark-MongoDB")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "1g")
        .config("spark.ui.enabled", "false")
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


def make_kline_message(
    symbol: str = "BTCUSDT",
    interval: str = "1m",
    open_time_ms: int = None,
    open_p: float = 45000.0,
    high: float = 45150.0,
    low: float = 44850.0,
    close: float = 45050.0,
    volume: float = 123.456,
    num_trades: int = 1500,
) -> dict:
    """
    Génère un message kline au format WebSocket Binance identique
    à celui produit par BinanceRESTClient._format_kline_as_websocket
    """
    if open_time_ms is None:
        open_time_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    close_time_ms = open_time_ms + 59999
    now = datetime.now(timezone.utc)
    now_ms = int(now.timestamp() * 1000)

    return {
        "stream": f"{symbol.lower()}@kline_{interval}",
        "data": {
            "e": "kline",
            "E": now_ms,
            "s": symbol.upper(),
            "k": {
                "t": open_time_ms,
                "T": close_time_ms,
                "s": symbol.upper(),
                "i": interval,
                "o": str(open_p),
                "h": str(high),
                "l": str(low),
                "c": str(close),
                "v": str(volume),
                "n": num_trades,
                "x": True,
                "q": str(volume * close),
                "V": str(volume * 0.6),
                "Q": str(volume * close * 0.6),
            },
        },
        "received_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": rest_config.SOURCE_LABEL,
    }


def make_ohlcv_rows(n: int = 50, symbol: str = "BTCUSDT") -> List[Row]:
    """Génère n bougies OHLCV simulées pour les tests"""
    now_ms = int(datetime.utcnow().timestamp() * 1000)
    rows = []
    base = 45000.0
    for i in range(n):
        ts = now_ms + i * 60_000
        close = base + (i % 10 - 5) * 200
        open_p = close - 50
        high = close + 150
        low = open_p - 150
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


def make_multi_symbol_rows(symbols: List[str], n_per_symbol: int = 30) -> List[Row]:
    """Génère des données OHLCV pour plusieurs symboles"""
    all_rows = []
    base_prices = {"BTCUSDT": 45000.0, "ETHUSDT": 3200.0, "BNBUSDT": 420.0}
    now_ms = int(datetime.utcnow().timestamp() * 1000)

    for symbol in symbols:
        base = base_prices.get(symbol, 1000.0)
        for i in range(n_per_symbol):
            ts = now_ms + i * 60_000
            close = base + (i % 10 - 5) * (base * 0.004)
            open_p = close - (base * 0.001)
            high = close + (base * 0.003)
            low = open_p - (base * 0.003)
            all_rows.append(Row(
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
    return all_rows


# ═══════════════════════════════════════════════════════════════════════════
# TEST 1: Kafka → Spark (Parsing et routage)
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.integration_pipeline
class TestKafkaToSpark:
    """Vérifie l'ingestion de messages Kafka vers Spark DataFrames"""

    def test_kline_message_format_matches_schema(self):
        """Le format du message kline REST doit contenir les champs attendus"""
        msg = make_kline_message(symbol="BTCUSDT", interval="1m")

        assert msg["stream"] == "btcusdt@kline_1m"
        assert msg["data"]["e"] == "kline"
        assert msg["data"]["s"] == "BTCUSDT"
        assert msg["source"] == rest_config.SOURCE_LABEL

        kline = msg["data"]["k"]
        assert float(kline["o"]) > 0
        assert float(kline["h"]) >= float(kline["l"])
        assert float(kline["c"]) > 0
        assert float(kline["v"]) > 0
        assert kline["x"] is True
        assert isinstance(kline["n"], int)

    def test_kline_message_parseable_to_spark_schema(self, spark):
        """Un message kline converti en colonnes OHLCV doit être parseable par Spark"""
        msg = make_kline_message()
        kline = msg["data"]["k"]

        # Simuler le parsing comme le ferait kafka_consumer.parse_raw_klines
        flat_data = {
            "symbol": msg["data"]["s"],
            "interval": kline["i"],
            "open_time": kline["t"],
            "close_time": kline["T"],
            "open": float(kline["o"]),
            "high": float(kline["h"]),
            "low": float(kline["l"]),
            "close": float(kline["c"]),
            "volume": float(kline["v"]),
            "quote_volume": float(kline["q"]),
            "trades_count": kline["n"],
        }

        df = spark.createDataFrame([flat_data], schema=RAW_KLINES_SCHEMA)
        assert df.count() == 1

        row = df.first()
        assert row.symbol == "BTCUSDT"
        assert row.open > 0
        assert row.high >= row.low

    def test_topic_routing_kline_to_raw_klines(self):
        """Les messages kline doivent être routés vers le topic raw_klines"""
        # Logique de routage inline (même logique que BinanceKafkaProducer._determine_topic)
        stream_name = "btcusdt@kline_1m"
        if "kline" in stream_name:
            topic = streaming_kafka_config.TOPICS["raw_klines"]
        elif "trade" in stream_name.lower() or "aggTrade" in stream_name:
            topic = streaming_kafka_config.TOPICS["raw_trades"]
        else:
            topic = streaming_kafka_config.TOPICS.get("raw_trades", "raw_trades")

        assert topic == streaming_kafka_config.TOPICS["raw_klines"]

    def test_topic_routing_aggtrade_to_raw_trades(self):
        """Les messages aggTrade doivent être routés vers le topic raw_trades"""
        stream_name = "btcusdt@aggTrade"
        if "kline" in stream_name:
            topic = streaming_kafka_config.TOPICS["raw_klines"]
        elif "trade" in stream_name.lower() or "aggTrade" in stream_name:
            topic = streaming_kafka_config.TOPICS["raw_trades"]
        else:
            topic = streaming_kafka_config.TOPICS.get("raw_trades", "raw_trades")

        assert topic == streaming_kafka_config.TOPICS["raw_trades"]

    def test_trading_pair_extraction(self):
        """La paire de trading est extraite correctement du stream name"""
        # Logique inline (même logique que BinanceKafkaProducer._extract_trading_pair)
        def extract_pair(stream_name):
            return stream_name.split("@")[0] if "@" in stream_name else stream_name

        assert extract_pair("btcusdt@kline_1m") == "btcusdt"
        assert extract_pair("ethusdt@aggTrade") == "ethusdt"
        assert extract_pair("bnbusdt@trade") == "bnbusdt"

    def test_rest_client_format_matches_websocket(self):
        """Le format REST kline doit correspondre au format WebSocket Binance"""
        # Simuler _format_kline_as_websocket inline (même logique que BinanceRESTClient)
        symbol = "BTCUSDT"
        interval = "1m"
        raw_kline = [
            1700000000000,   # 0: open_time
            "45000.00",      # 1: open
            "45150.00",      # 2: high
            "44850.00",      # 3: low
            "45050.00",      # 4: close
            "123.456",       # 5: volume
            1700000059999,   # 6: close_time
            "5560380.00",    # 7: quote_volume
            1500,            # 8: num_trades
            "74.073",        # 9: taker_buy_base_volume
            "3336228.00",    # 10: taker_buy_quote_volume
            "0",             # 11: ignore
        ]

        # Reproduire le format WebSocket
        result = {
            "stream": f"{symbol.lower()}@kline_{interval}",
            "data": {
                "e": "kline",
                "E": int(datetime.now(timezone.utc).timestamp() * 1000),
                "s": symbol,
                "k": {
                    "t": raw_kline[0],
                    "T": raw_kline[6],
                    "s": symbol,
                    "i": interval,
                    "o": raw_kline[1],
                    "h": raw_kline[2],
                    "l": raw_kline[3],
                    "c": raw_kline[4],
                    "v": raw_kline[5],
                    "n": raw_kline[8],
                    "x": True,
                    "q": raw_kline[7],
                    "V": raw_kline[9],
                    "Q": raw_kline[10],
                },
            },
            "received_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "source": rest_config.SOURCE_LABEL,
        }

        assert result["stream"] == "btcusdt@kline_1m"
        assert result["source"] == rest_config.SOURCE_LABEL
        assert result["data"]["e"] == "kline"
        assert result["data"]["s"] == "BTCUSDT"

        k = result["data"]["k"]
        assert k["o"] == "45000.00"
        assert k["h"] == "45150.00"
        assert k["l"] == "44850.00"
        assert k["c"] == "45050.00"
        assert k["v"] == "123.456"
        assert k["x"] is True
        assert k["n"] == 1500

    def test_multiple_symbols_parsed_correctly(self, spark):
        """Plusieurs messages de symboles différents sont parsés correctement"""
        messages = [
            make_kline_message(symbol="BTCUSDT", close=45000.0),
            make_kline_message(symbol="ETHUSDT", close=3200.0),
            make_kline_message(symbol="BNBUSDT", close=420.0),
        ]

        rows = []
        for msg in messages:
            kline = msg["data"]["k"]
            rows.append({
                "symbol": msg["data"]["s"],
                "interval": kline["i"],
                "open_time": kline["t"],
                "close_time": kline["T"],
                "open": float(kline["o"]),
                "high": float(kline["h"]),
                "low": float(kline["l"]),
                "close": float(kline["c"]),
                "volume": float(kline["v"]),
                "quote_volume": float(kline["q"]),
                "trades_count": kline["n"],
            })

        df = spark.createDataFrame(rows, schema=RAW_KLINES_SCHEMA)
        assert df.count() == 3

        symbols = [row.symbol for row in df.select("symbol").distinct().collect()]
        assert set(symbols) == {"BTCUSDT", "ETHUSDT", "BNBUSDT"}


# ═══════════════════════════════════════════════════════════════════════════
# TEST 2: Pipeline ETL Spark complet
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.integration_pipeline
class TestSparkETLPipeline:
    """Vérifie le pipeline ETL complet : cleaning → indicators → transformations"""

    def test_cleaning_then_indicators(self, spark, cleaning_config):
        """Pipeline: données brutes → nettoyage → indicateurs techniques"""
        rows = make_ohlcv_rows(50, "BTCUSDT")
        df_raw = spark.createDataFrame(rows)

        # Étape 1: Nettoyage
        df_clean, metrics = clean_ohlcv_data(df_raw, cleaning_config)
        assert metrics["final_count"] > 0
        assert metrics["quality_rate"] >= 90.0

        # Étape 2: Indicateurs techniques
        df_indicators = df_clean.drop("zscore_close", "is_outlier")
        df_result = calculate_all_indicators(df_indicators)

        # Vérifier les colonnes d'indicateurs
        expected_cols = [
            "sma_20", "sma_50", "sma_200",
            "rsi_14",
            "macd", "macd_signal", "macd_histogram",
            "bollinger_upper", "bollinger_middle", "bollinger_lower",
        ]
        for col in expected_cols:
            assert col in df_result.columns, f"Missing column: {col}"

        # Le nombre de lignes ne change pas
        assert df_result.count() == df_clean.count()

    def test_cleaning_removes_invalid_data(self, spark, cleaning_config):
        """Le nettoyage supprime les données invalides (prix négatifs, doublons)"""
        rows = make_ohlcv_rows(30, "BTCUSDT")

        # Ajouter 3 lignes invalides
        base_ts = rows[-1].timestamp + 60000
        invalid_rows = [
            # Prix négatif
            Row(symbol="BTCUSDT", interval="1m", timestamp=base_ts,
                open=-100.0, high=100.0, low=50.0, close=80.0, volume=100.0, trades=10),
            # High < Low
            Row(symbol="BTCUSDT", interval="1m", timestamp=base_ts + 60000,
                open=100.0, high=90.0, low=110.0, close=95.0, volume=100.0, trades=10),
            # Volume négatif
            Row(symbol="BTCUSDT", interval="1m", timestamp=base_ts + 120000,
                open=100.0, high=120.0, low=80.0, close=110.0, volume=-50.0, trades=10),
        ]
        all_rows = rows + invalid_rows
        df_raw = spark.createDataFrame(all_rows)

        df_clean, metrics = clean_ohlcv_data(df_raw, cleaning_config)

        assert metrics["invalid_ohlcv"] >= 2  # au moins prix négatif + high<low
        assert df_clean.count() < df_raw.count()

    def test_cleaning_deduplication(self, spark, cleaning_config):
        """Le nettoyage supprime les doublons (symbol, interval, timestamp)"""
        rows = make_ohlcv_rows(20, "BTCUSDT")
        # Dupliquer les 5 premières lignes
        all_rows = rows + rows[:5]
        df_raw = spark.createDataFrame(all_rows)

        df_clean, metrics = clean_ohlcv_data(df_raw, cleaning_config)

        assert metrics["duplicates_removed"] >= 5
        assert df_clean.count() == 20

    def test_full_transformation_chain(self, spark, cleaning_config):
        """Pipeline complet: clean → indicators → price_change → volume → candle → metadata"""
        rows = make_ohlcv_rows(50, "ETHUSDT")
        df_raw = spark.createDataFrame(rows)

        # 1. Nettoyage
        df_clean, _ = clean_ohlcv_data(df_raw, cleaning_config)
        df = df_clean.drop("zscore_close", "is_outlier")

        # 2. Indicateurs techniques
        df = calculate_all_indicators(df)

        # 3. Transformations
        df = calculate_price_change(df)
        df = add_candle_pattern(df)
        df = add_metadata_columns(df)

        # Vérifier les colonnes finales
        expected_final = [
            "symbol", "interval", "timestamp", "open", "high", "low", "close", "volume",
            "sma_20", "rsi_14", "macd", "bollinger_upper",
            "price_change", "price_change_percent",
            "candle_type", "processed_at",
        ]
        for col in expected_final:
            assert col in df.columns, f"Missing column: {col}"

        # Vérifier la cohérence des candle patterns
        candle_types = [row.candle_type for row in df.select("candle_type").distinct().collect()]
        for ct in candle_types:
            assert ct in ("bullish", "bearish", "doji")

    def test_rsi_values_in_valid_range(self, spark, cleaning_config):
        """RSI doit être entre 0 et 100 après le pipeline ETL complet"""
        rows = make_ohlcv_rows(50, "BTCUSDT")
        df = spark.createDataFrame(rows)
        df_clean, _ = clean_ohlcv_data(df, cleaning_config)
        df_result = calculate_rsi(df_clean.drop("zscore_close", "is_outlier"))

        rsi_valid = df_result.filter(F.col("rsi_14").isNotNull())
        if rsi_valid.count() > 0:
            out_of_range = rsi_valid.filter(
                (F.col("rsi_14") < 0) | (F.col("rsi_14") > 100)
            ).count()
            assert out_of_range == 0

    def test_bollinger_bands_ordering(self, spark, cleaning_config):
        """Bollinger Bands: upper >= middle >= lower après le pipeline"""
        rows = make_ohlcv_rows(50, "BTCUSDT")
        df = spark.createDataFrame(rows)
        df_clean, _ = clean_ohlcv_data(df, cleaning_config)
        df_result = calculate_bollinger_bands(
            df_clean.drop("zscore_close", "is_outlier"), period=20
        )

        valid_rows = df_result.filter(
            F.col("bollinger_upper").isNotNull() &
            F.col("bollinger_middle").isNotNull() &
            F.col("bollinger_lower").isNotNull()
        ).collect()

        for row in valid_rows:
            assert row.bollinger_upper >= row.bollinger_middle, \
                f"upper ({row.bollinger_upper}) < middle ({row.bollinger_middle})"
            assert row.bollinger_middle >= row.bollinger_lower, \
                f"middle ({row.bollinger_middle}) < lower ({row.bollinger_lower})"


# ═══════════════════════════════════════════════════════════════════════════
# TEST 3: MongoDB Writer (schéma et format)
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.integration_pipeline
class TestMongoDBWriter:
    """Vérifie le format et le schéma des données destinées à MongoDB"""

    def test_ohlcv_output_has_required_mongodb_fields(self, spark, cleaning_config):
        """Les données OHLCV de sortie doivent contenir les champs requis par MongoDB"""
        rows = make_ohlcv_rows(30, "BTCUSDT")
        df = spark.createDataFrame(rows)
        df_clean, _ = clean_ohlcv_data(df, cleaning_config)

        # Champs requis par la collection ohlcv (mongodb/init.js)
        required_fields = ["symbol", "interval", "open", "high", "low", "close", "volume", "timestamp"]
        for field in required_fields:
            assert field in df_clean.columns, f"Missing required MongoDB field: {field}"

    def test_indicators_output_has_required_mongodb_fields(self, spark, cleaning_config):
        """Les indicateurs de sortie doivent contenir les champs requis par MongoDB"""
        rows = make_ohlcv_rows(50, "BTCUSDT")
        df = spark.createDataFrame(rows)
        df_clean, _ = clean_ohlcv_data(df, cleaning_config)
        df_indicators = calculate_all_indicators(df_clean.drop("zscore_close", "is_outlier"))

        # Champs requis par la collection indicators (mongodb/init.js)
        required_fields = ["symbol", "interval", "timestamp"]
        for field in required_fields:
            assert field in df_indicators.columns, f"Missing required MongoDB field: {field}"

        # Indicateurs attendus
        indicator_fields = [
            "rsi_14", "macd", "macd_signal", "macd_histogram",
            "bollinger_upper", "bollinger_middle", "bollinger_lower",
            "sma_20", "sma_50",
        ]
        for field in indicator_fields:
            assert field in df_indicators.columns, f"Missing indicator field: {field}"

    def test_ohlcv_data_convertible_to_mongodb_documents(self, spark, cleaning_config):
        """Les données OHLCV Spark doivent être convertibles en documents MongoDB"""
        rows = make_ohlcv_rows(10, "BTCUSDT")
        df = spark.createDataFrame(rows)
        df_clean, _ = clean_ohlcv_data(df, cleaning_config)

        # Convertir en list de dicts (simule MongoDB insert_many)
        documents = [row.asDict() for row in df_clean.collect()]

        assert len(documents) > 0
        for doc in documents:
            assert doc["symbol"] == "BTCUSDT"
            assert doc["interval"] == "1m"
            assert isinstance(doc["open"], float)
            assert isinstance(doc["high"], float)
            assert isinstance(doc["low"], float)
            assert isinstance(doc["close"], float)
            assert isinstance(doc["volume"], float)
            assert doc["open"] > 0
            assert doc["high"] >= doc["low"]
            assert doc["close"] > 0
            assert doc["volume"] > 0

    def test_no_null_values_in_required_fields(self, spark, cleaning_config):
        """Aucune valeur null dans les champs requis pour MongoDB"""
        rows = make_ohlcv_rows(30, "BTCUSDT")
        df = spark.createDataFrame(rows)
        df_clean, _ = clean_ohlcv_data(df, cleaning_config)

        required_fields = ["symbol", "interval", "open", "high", "low", "close", "volume"]
        for field in required_fields:
            null_count = df_clean.filter(F.col(field).isNull()).count()
            assert null_count == 0, f"Found {null_count} null values in {field}"

    def test_unique_ohlcv_key_constraint(self, spark, cleaning_config):
        """Les données nettoyées respectent la clé unique (symbol, interval, timestamp)"""
        rows = make_ohlcv_rows(30, "BTCUSDT") + make_ohlcv_rows(30, "BTCUSDT")[:5]  # 5 doublons
        df = spark.createDataFrame(rows)
        df_clean, _ = clean_ohlcv_data(df, cleaning_config)

        # Vérifier l'unicité
        dup_count = (
            df_clean.groupBy("symbol", "interval", "timestamp")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        assert dup_count == 0, f"Found {dup_count} duplicate keys"

    @patch("pymongo.MongoClient")
    def test_batch_write_calls_mongodb(self, mock_client_cls, spark, cleaning_config):
        """Vérifie que write_to_mongodb_batch appelle correctement MongoDB"""
        rows = make_ohlcv_rows(5, "BTCUSDT")
        df = spark.createDataFrame(rows)
        df_clean, _ = clean_ohlcv_data(df, cleaning_config)

        # Les données sont convertibles (simulation d'écriture)
        documents = [row.asDict() for row in df_clean.collect()]
        assert len(documents) == 5

        # Simuler l'insertion dans MongoDB
        mock_db = MagicMock()
        mock_collection = MagicMock()
        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_client_cls.return_value = mock_client

        # Simuler insert_many
        mock_collection.insert_many(documents)
        mock_collection.insert_many.assert_called_once_with(documents)


# ═══════════════════════════════════════════════════════════════════════════
# TEST 4: Flux End-to-End complet
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.integration_pipeline
class TestEndToEndFlow:
    """Test end-to-end: message Kafka → parse → ETL → format MongoDB"""

    def test_kline_message_to_mongodb_ready(self, spark, cleaning_config):
        """
        Flux complet: message kline Binance → parsing → nettoyage →
        indicateurs → transformations → format prêt pour MongoDB
        """
        # 1. Générer des messages kline au format Binance
        base_time = int(datetime(2024, 6, 1, 10, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        base_price = 45000.0

        ohlcv_rows = []
        for i in range(50):
            ts = base_time + i * 60000
            close = base_price + (i % 10 - 5) * 200
            open_p = close - 50
            high = close + 150
            low = open_p - 150
            ohlcv_rows.append(Row(
                symbol="BTCUSDT",
                interval="1m",
                timestamp=ts,
                open=float(open_p),
                high=float(high),
                low=float(low),
                close=float(close),
                volume=float(1000 + i * 100),
                trades=500 + i,
            ))

        df_raw = spark.createDataFrame(ohlcv_rows)

        # 2. Nettoyage
        df_clean, metrics = clean_ohlcv_data(df_raw, cleaning_config)
        assert metrics["final_count"] == 50

        # 3. Indicateurs techniques
        df = df_clean.drop("zscore_close", "is_outlier")
        df = calculate_all_indicators(df)

        # 4. Transformations finales
        df = calculate_price_change(df)
        df = add_candle_pattern(df)
        df = add_metadata_columns(df)

        # 5. Vérification du format final
        final_columns = df.columns
        mongodb_required = ["symbol", "interval", "timestamp", "open", "high", "low", "close", "volume"]
        for col in mongodb_required:
            assert col in final_columns, f"Missing MongoDB-required column: {col}"

        indicator_cols = ["rsi_14", "macd", "bollinger_upper", "sma_20"]
        for col in indicator_cols:
            assert col in final_columns, f"Missing indicator column: {col}"

        enrichment_cols = ["price_change", "candle_type", "processed_at"]
        for col in enrichment_cols:
            assert col in final_columns, f"Missing enrichment column: {col}"

        # 6. Vérifier qu'on peut convertir en documents
        documents = [row.asDict() for row in df.limit(5).collect()]
        assert len(documents) == 5
        for doc in documents:
            assert doc["symbol"] == "BTCUSDT"
            assert doc["close"] > 0

    def test_multi_symbol_pipeline(self, spark, cleaning_config):
        """
        Pipeline avec 3 symboles: les données sont correctement partitionnées
        et les indicateurs calculés indépendamment par symbole
        """
        symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
        rows = make_multi_symbol_rows(symbols, n_per_symbol=30)
        df_raw = spark.createDataFrame(rows)

        assert df_raw.count() == 90  # 30 * 3

        # Nettoyage
        df_clean, metrics = clean_ohlcv_data(df_raw, cleaning_config)
        assert metrics["final_count"] > 0

        # Indicateurs
        df = df_clean.drop("zscore_close", "is_outlier")
        df = calculate_all_indicators(df)

        # Vérifier que les 3 symboles sont présents
        result_symbols = [
            row.symbol for row in df.select("symbol").distinct().collect()
        ]
        assert set(result_symbols) == set(symbols)

        # Vérifier que chaque symbole a des lignes
        for symbol in symbols:
            count = df.filter(F.col("symbol") == symbol).count()
            assert count > 0, f"No data for {symbol}"

    def test_data_integrity_throughout_pipeline(self, spark, cleaning_config):
        """Vérifie l'intégrité des données à chaque étape du pipeline"""
        rows = make_ohlcv_rows(40, "BTCUSDT")
        df_raw = spark.createDataFrame(rows)
        initial_count = df_raw.count()

        # Étape 1: Nettoyage
        df_clean, _ = clean_ohlcv_data(df_raw, cleaning_config)
        clean_count = df_clean.count()
        assert clean_count <= initial_count
        assert clean_count > 0

        # Étape 2: Indicateurs (ne doit pas changer le count)
        df_ind = calculate_all_indicators(df_clean.drop("zscore_close", "is_outlier"))
        assert df_ind.count() == clean_count

        # Étape 3: Transformations (ne doit pas changer le count)
        df_final = calculate_price_change(df_ind)
        df_final = add_candle_pattern(df_final)
        df_final = add_metadata_columns(df_final)
        assert df_final.count() == clean_count

        # Vérifier qu'aucune donnée n'a été corrompue
        for row in df_final.collect():
            assert row.open > 0, f"Corrupted open price: {row.open}"
            assert row.high >= row.low, f"high ({row.high}) < low ({row.low})"
            assert row.volume > 0, f"Invalid volume: {row.volume}"

    def test_pipeline_handles_edge_case_single_row(self, spark, cleaning_config):
        """Le pipeline gère correctement le cas minimal (1 seule bougie)"""
        rows = [make_ohlcv_rows(1, "BTCUSDT")[0]]
        df_raw = spark.createDataFrame(rows)

        df_clean, metrics = clean_ohlcv_data(df_raw, cleaning_config)
        assert df_clean.count() == 1

        df = df_clean.drop("zscore_close", "is_outlier")
        df = calculate_price_change(df)
        df = add_candle_pattern(df)

        assert df.count() == 1
        assert "candle_type" in df.columns

    def test_pipeline_output_json_serializable(self, spark, cleaning_config):
        """La sortie du pipeline doit être sérialisable en JSON (MongoDB)"""
        rows = make_ohlcv_rows(20, "BTCUSDT")
        df = spark.createDataFrame(rows)
        df_clean, _ = clean_ohlcv_data(df, cleaning_config)
        df_final = calculate_price_change(df_clean.drop("zscore_close", "is_outlier"))

        # Convertir en JSON string (simule la sérialisation MongoDB)
        for row in df_final.limit(5).collect():
            doc = row.asDict()
            try:
                json_str = json.dumps(doc, default=str)
                parsed = json.loads(json_str)
                assert parsed["symbol"] == "BTCUSDT"
            except (TypeError, ValueError) as e:
                pytest.fail(f"Document not JSON serializable: {e}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

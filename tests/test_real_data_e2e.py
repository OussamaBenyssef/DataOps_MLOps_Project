"""
Tests E2E avec données réelles Binance
=======================================
Appelle la vraie API publique Binance (pas de clé API nécessaire),
puis exécute le pipeline complet :
  1. REST API → klines réelles
  2. Spark ETL (cleaning → indicateurs techniques → transformations)
  3. Feature engineering P4
  4. Model training (XGBoost + LSTM) avec données réelles
  5. WebSocket streaming live (10 secondes)

Lancer:
    RUN_INTEGRATION=1 pytest tests/test_real_data_e2e.py -v
"""

import os
import sys
import json
import time
import pytest
import threading
import numpy as np
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

# Condition pour les tests d'intégration
RUN_INTEGRATION = os.getenv("RUN_INTEGRATION", "0") == "1"
skip_integration = pytest.mark.skipif(
    not RUN_INTEGRATION,
    reason="Tests d'intégration désactivés. Lancer avec RUN_INTEGRATION=1."
)

# ═══════════════════════════════════════════════════════════════════════════
# TEST 1: API REST Binance → Données réelles
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.integration
class TestBinanceRESTRealData:
    """Récupère des données réelles depuis l'API publique Binance"""

    @skip_integration
    @patch("streaming.binance_rest_client.BinanceKafkaProducer")
    def test_fetch_btcusdt_klines(self, mock_producer):
        """Récupère 100 klines BTCUSDT 1m depuis l'API réelle"""
        from streaming.binance_rest_client import BinanceRESTClient
        client = BinanceRESTClient()

        klines = client._fetch_klines_batch("BTCUSDT", "1m", limit=100)

        assert isinstance(klines, list)
        assert len(klines) == 100, f"Expected 100 klines, got {len(klines)}"

        # Vérifier la structure de chaque kline
        for i, kline in enumerate(klines):
            assert len(kline) == 12, f"Kline {i} has {len(kline)} fields, expected 12"
            open_price = float(kline[1])
            high = float(kline[2])
            low = float(kline[3])
            close = float(kline[4])
            volume = float(kline[5])

            assert open_price > 0, f"Kline {i}: invalid open price {open_price}"
            assert high >= low, f"Kline {i}: high ({high}) < low ({low})"
            assert volume >= 0, f"Kline {i}: invalid volume {volume}"

        client.close()
        print(f"\n✅ 100 klines BTCUSDT récupérées — prix: {float(klines[-1][4]):.2f} USDT")

    @skip_integration
    @patch("streaming.binance_rest_client.BinanceKafkaProducer")
    def test_fetch_multiple_symbols(self, mock_producer):
        """Récupère des klines pour 3 paires différentes"""
        from streaming.binance_rest_client import BinanceRESTClient
        client = BinanceRESTClient()

        symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
        for symbol in symbols:
            klines = client._fetch_klines_batch(symbol, "1m", limit=10)
            assert len(klines) == 10, f"{symbol}: expected 10, got {len(klines)}"
            close_price = float(klines[-1][4])
            assert close_price > 0
            print(f"  ✅ {symbol}: {close_price:.2f} USDT")

        client.close()

    @skip_integration
    @patch("streaming.binance_rest_client.BinanceKafkaProducer")
    def test_format_real_klines_as_websocket(self, mock_producer):
        """Formate les klines réelles au format WebSocket Binance"""
        from streaming.binance_rest_client import BinanceRESTClient
        client = BinanceRESTClient()

        klines = client._fetch_klines_batch("BTCUSDT", "1m", limit=5)

        for kline in klines:
            msg = client._format_kline_as_websocket("BTCUSDT", "1m", kline)

            assert msg["stream"] == "btcusdt@kline_1m"
            assert msg["source"] == "rest_api"
            assert msg["data"]["e"] == "kline"
            assert msg["data"]["s"] == "BTCUSDT"

            k = msg["data"]["k"]
            assert float(k["o"]) > 0
            assert float(k["h"]) >= float(k["l"])
            assert k["x"] is True
            assert isinstance(k["n"], int)

        client.close()
        print(f"\n✅ 5 klines réelles formatées au format WebSocket")


# ═══════════════════════════════════════════════════════════════════════════
# TEST 2: Pipeline Spark ETL avec données réelles
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.integration
class TestSparkETLRealData:
    """Exécute le pipeline Spark ETL complet avec des données réelles Binance"""

    @pytest.fixture(scope="class")
    def spark(self):
        from pyspark.sql import SparkSession
        spark_session = (
            SparkSession.builder
            .appName("RealData-E2E-Test")
            .master("local[2]")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.driver.memory", "1g")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        spark_session.sparkContext.setLogLevel("ERROR")
        yield spark_session
        spark_session.stop()

    @pytest.fixture(scope="class")
    def real_klines(self):
        """Récupère 200 klines réelles BTCUSDT depuis Binance"""
        import requests
        url = "https://api.binance.com/api/v3/klines"
        params = {"symbol": "BTCUSDT", "interval": "1m", "limit": 200}
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        return resp.json()

    @skip_integration
    def test_real_data_to_spark_dataframe(self, spark, real_klines):
        """Les données réelles sont parsées correctement en DataFrame Spark"""
        from pyspark.sql import Row

        rows = []
        for kline in real_klines:
            rows.append(Row(
                symbol="BTCUSDT",
                interval="1m",
                timestamp=int(kline[0]),
                open=float(kline[1]),
                high=float(kline[2]),
                low=float(kline[3]),
                close=float(kline[4]),
                volume=float(kline[5]),
                trades=int(kline[8]),
            ))

        df = spark.createDataFrame(rows)
        assert df.count() == 200
        print(f"\n✅ 200 klines réelles parsées en Spark DataFrame")

    @skip_integration
    def test_full_etl_pipeline_real_data(self, spark, real_klines):
        """Pipeline ETL complet avec données réelles: clean → indicateurs → transformations"""
        from pyspark.sql import Row
        from processing.config import CleaningConfig
        from processing.data_cleaning import clean_ohlcv_data
        from processing.technical_indicators import calculate_all_indicators
        from processing.transformations import (
            calculate_price_change, add_candle_pattern, add_metadata_columns
        )

        # 1. Parser les données réelles
        rows = []
        for kline in real_klines:
            rows.append(Row(
                symbol="BTCUSDT",
                interval="1m",
                timestamp=int(kline[0]),
                open=float(kline[1]),
                high=float(kline[2]),
                low=float(kline[3]),
                close=float(kline[4]),
                volume=float(kline[5]),
                trades=int(kline[8]),
            ))

        df_raw = spark.createDataFrame(rows)
        initial_count = df_raw.count()
        print(f"\n📊 Données brutes: {initial_count} klines")

        # 2. Nettoyage
        config = CleaningConfig()
        df_clean, metrics = clean_ohlcv_data(df_raw, config)
        clean_count = df_clean.count()
        print(f"🧹 Après nettoyage: {clean_count} klines (qualité: {metrics['quality_rate']:.1f}%)")
        assert clean_count > 0
        assert metrics["quality_rate"] >= 90.0, f"Qualité trop basse: {metrics['quality_rate']}%"

        # 3. Indicateurs techniques
        df = df_clean.drop("zscore_close", "is_outlier")
        df = calculate_all_indicators(df)

        indicator_cols = ["sma_20", "rsi_14", "macd", "bollinger_upper", "bollinger_lower"]
        for col in indicator_cols:
            assert col in df.columns, f"Missing indicator: {col}"
        print(f"📈 Indicateurs calculés: {', '.join(indicator_cols)}")

        # 4. Transformations
        df = calculate_price_change(df)
        df = add_candle_pattern(df)
        df = add_metadata_columns(df)

        assert "price_change" in df.columns
        assert "candle_type" in df.columns
        assert "processed_at" in df.columns
        assert df.count() == clean_count

        # 5. Vérifier les valeurs RSI
        from pyspark.sql import functions as F
        rsi_valid = df.filter(F.col("rsi_14").isNotNull())
        if rsi_valid.count() > 0:
            rsi_out = rsi_valid.filter((F.col("rsi_14") < 0) | (F.col("rsi_14") > 100)).count()
            assert rsi_out == 0, f"{rsi_out} RSI values out of range"
            rsi_sample = rsi_valid.select("rsi_14").first().rsi_14
            print(f"📉 RSI actuel: {rsi_sample:.2f}")

        # 6. Vérifier la sérialisabilité JSON (MongoDB)
        documents = [row.asDict() for row in df.limit(5).collect()]
        for doc in documents:
            json.dumps(doc, default=str)  # Doit pas lever d'exception
            assert doc["symbol"] == "BTCUSDT"
            assert doc["close"] > 0

        print(f"✅ Pipeline ETL complet réussi — {df.count()} klines traitées")


# ═══════════════════════════════════════════════════════════════════════════
# TEST 3: Feature Engineering P4 avec données réelles
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.integration
class TestFeatureEngineeringRealData:
    """Teste le feature engineering P4 avec des données de marché réelles"""

    @skip_integration
    def test_build_feature_matrix_real_data(self):
        """Construit la matrice de features à partir de données réelles"""
        import requests
        import pandas as pd
        from ml.feature_engineering import CryptoFeatureEngineer

        # Récupérer 500 klines
        url = "https://api.binance.com/api/v3/klines"
        params = {"symbol": "BTCUSDT", "interval": "1m", "limit": 500}
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        raw_klines = resp.json()

        # Construire le DataFrame OHLCV au format attendu
        df = pd.DataFrame(raw_klines, columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_volume", "trades", "taker_buy_base",
            "taker_buy_quote", "ignore"
        ])
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = df[col].astype(float)
        df["timestamp"] = df["open_time"]
        df["symbol"] = "BTCUSDT"
        df["interval"] = "1m"

        # Appliquer les étapes de feature engineering
        df = CryptoFeatureEngineer.add_price_features(df)
        df = CryptoFeatureEngineer.add_volume_features(df)
        df = CryptoFeatureEngineer.add_candle_features(df)
        df = CryptoFeatureEngineer.add_lag_features(df)
        df = CryptoFeatureEngineer.add_rolling_features(df)
        df = CryptoFeatureEngineer.add_target_variables(df)

        assert len(df) > 0
        assert "return_1" in df.columns
        assert "target_direction" in df.columns

        # Vérifier les colonnes de features
        exclude = {"symbol", "interval", "timestamp", "open_time", "close_time",
                   "quote_volume", "taker_buy_base", "taker_buy_quote", "ignore",
                   "target_direction", "target_return_pct", "anomaly_label"}
        feature_cols = [c for c in df.columns if c not in exclude]

        print(f"\n✅ Feature matrix: {df.shape[0]} lignes × {len(feature_cols)} features")
        print(f"📋 Features: {', '.join(feature_cols[:10])}...")

        # Vérifier NaN ratio
        df_clean = df.dropna()
        kept_ratio = len(df_clean) / len(df)
        print(f"🔍 Lignes conservées après dropna: {kept_ratio:.1%}")
        assert kept_ratio > 0.5, f"Trop de lignes perdues: {kept_ratio:.1%}"


# ═══════════════════════════════════════════════════════════════════════════
# TEST 4: Model Training avec données réelles
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.integration
class TestModelTrainingRealData:
    """Entraîne les modèles XGBoost et LSTM avec des données de marché réelles"""

    @skip_integration
    def test_train_xgboost_real_data(self):
        """Entraîne XGBoost sur des données réelles Binance"""
        import requests
        import pandas as pd
        from ml.model_training import CryptoPricePredictor
        from ml.feature_engineering import CryptoFeatureEngineer

        # Récupérer 500 klines
        url = "https://api.binance.com/api/v3/klines"
        params = {"symbol": "BTCUSDT", "interval": "1m", "limit": 500}
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        raw_klines = resp.json()

        # Préparer les features
        df = pd.DataFrame(raw_klines, columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_volume", "trades", "taker_buy_base",
            "taker_buy_quote", "ignore"
        ])
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = df[col].astype(float)
        df["timestamp"] = df["open_time"]
        df["symbol"] = "BTCUSDT"
        df["interval"] = "1m"

        # Feature engineering
        df = CryptoFeatureEngineer.add_price_features(df)
        df = CryptoFeatureEngineer.add_volume_features(df)
        df = CryptoFeatureEngineer.add_candle_features(df)
        df = CryptoFeatureEngineer.add_target_variables(df)
        df = df.dropna()

        assert len(df) > 50, f"Not enough data after dropna: {len(df)}"

        # Prédiction de direction (classification binaire)
        predictor = CryptoPricePredictor(test_size=0.2, sequence_length=10)
        exclude = {"symbol", "interval", "timestamp", "open_time", "close_time",
                   "quote_volume", "taker_buy_base", "taker_buy_quote", "ignore",
                   "target_direction", "target_return_pct", "anomaly_label"}
        feature_names = [c for c in df.columns if c not in exclude]

        X_train, X_test, y_train, y_test = predictor.prepare_data(
            df, feature_names, target_col="target_direction"
        )

        model, metrics = predictor.train_xgboost(X_train, y_train, X_test, y_test)
        predictions = predictor.predict(model, X_test, model_type="xgboost")

        assert len(predictions) == len(y_test)
        print(f"\n✅ XGBoost entraîné sur données réelles — Accuracy: {metrics['accuracy']:.2%}")

    @skip_integration
    def test_train_lstm_real_data(self):
        """Entraîne LSTM sur des données réelles Binance"""
        import requests
        import pandas as pd
        from ml.model_training import CryptoPricePredictor
        from ml.feature_engineering import CryptoFeatureEngineer

        # Récupérer 500 klines
        url = "https://api.binance.com/api/v3/klines"
        params = {"symbol": "BTCUSDT", "interval": "1m", "limit": 500}
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        raw_klines = resp.json()

        # Préparer les features
        df = pd.DataFrame(raw_klines, columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_volume", "trades", "taker_buy_base",
            "taker_buy_quote", "ignore"
        ])
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = df[col].astype(float)
        df["timestamp"] = df["open_time"]
        df["symbol"] = "BTCUSDT"
        df["interval"] = "1m"

        # Feature engineering
        df = CryptoFeatureEngineer.add_price_features(df)
        df = CryptoFeatureEngineer.add_volume_features(df)
        df = CryptoFeatureEngineer.add_candle_features(df)
        df = CryptoFeatureEngineer.add_target_variables(df)
        df = df.dropna()

        assert len(df) > 50, f"Not enough data: {len(df)}"

        predictor = CryptoPricePredictor(test_size=0.2, sequence_length=10)
        exclude = {"symbol", "interval", "timestamp", "open_time", "close_time",
                   "quote_volume", "taker_buy_base", "taker_buy_quote", "ignore",
                   "target_direction", "target_return_pct", "anomaly_label"}
        feature_names = [c for c in df.columns if c not in exclude]

        X_train, X_test, y_train, y_test = predictor.prepare_data(
            df, feature_names, target_col="target_direction"
        )

        model, metrics = predictor.train_lstm(
            X_train, y_train, X_test, y_test, epochs=5, batch_size=32
        )
        predictions = predictor.predict(model, X_test, model_type="lstm")

        assert len(predictions) > 0, "No predictions returned"
        print(f"\n✅ LSTM entraîné sur données réelles — Accuracy: {metrics['accuracy']:.2%} ({len(predictions)} prédictions)")


# ═══════════════════════════════════════════════════════════════════════════
# TEST 5: WebSocket Streaming live
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.integration
class TestWebSocketLive:
    """Teste la connexion WebSocket live à Binance"""

    @skip_integration
    def test_websocket_receives_live_data(self):
        """Se connecte au WebSocket Binance et reçoit des données en temps réel"""
        import websocket

        messages = []
        connected = threading.Event()
        done = threading.Event()

        def on_message(ws, message):
            data = json.loads(message)
            messages.append(data)
            if len(messages) >= 5:
                done.set()
                ws.close()

        def on_open(ws):
            connected.set()

        def on_error(ws, error):
            print(f"WebSocket error: {error}")
            done.set()

        url = "wss://stream.binance.com:9443/ws/btcusdt@kline_1m"
        ws = websocket.WebSocketApp(
            url,
            on_message=on_message,
            on_open=on_open,
            on_error=on_error,
        )

        # Run in thread with timeout
        ws_thread = threading.Thread(target=ws.run_forever, kwargs={"ping_interval": 20})
        ws_thread.daemon = True
        ws_thread.start()

        # Wait max 30 seconds
        done.wait(timeout=30)
        ws.close()

        assert len(messages) >= 1, "No WebSocket messages received"
        print(f"\n✅ WebSocket live: {len(messages)} messages reçus en temps réel")

        # Valider le format du premier message
        msg = messages[0]
        assert "e" in msg, f"Missing event type in message: {msg.keys()}"
        assert msg["e"] == "kline"
        assert "k" in msg
        assert float(msg["k"]["c"]) > 0
        print(f"📡 Prix live BTCUSDT: {msg['k']['c']} USDT")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])

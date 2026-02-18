"""
Tests Unitaires + Intégration - Phase Ingestion (P1)
Tests pour le pipeline Binance WebSocket → Kafka

Modules testés:
- src/streaming/config.py (BinanceConfig, KafkaConfig)
- src/streaming/kafka_producer.py (BinanceKafkaProducer)
- src/streaming/binance_websocket.py (BinanceWebSocketConnector)
- src/streaming/pipeline.py (BinanceKafkaPipeline)
"""

import os
import sys
import json
import pytest
from unittest.mock import patch, MagicMock, PropertyMock
from datetime import datetime

# Ajouter src au path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


# ============================================
# TEST 1: Configuration (config.py)
# ============================================
class TestStreamingConfig:
    """Tests pour BinanceConfig et KafkaConfig"""

    def test_binance_config_defaults(self):
        """Vérifie les valeurs par défaut de BinanceConfig"""
        from streaming.config import BinanceConfig
        config = BinanceConfig()

        assert config.TRADING_PAIRS == ["btcusdt", "ethusdt", "bnbusdt"]
        assert config.SOCKET_BASE_URL == "wss://stream.binance.com:9443/ws"
        assert config.MAX_RECONNECT_ATTEMPTS == 5
        assert config.RECONNECT_DELAY == 5

    def test_binance_config_stream_types(self):
        """Vérifie que les types de streams sont bien définis"""
        from streaming.config import BinanceConfig
        config = BinanceConfig()

        assert "trade" in config.STREAM_TYPES
        assert "kline_1m" in config.STREAM_TYPES
        assert config.STREAM_TYPES["trade"] == "aggTrade"

    def test_binance_config_custom_pairs(self):
        """Vérifie qu'on peut personnaliser les paires"""
        from streaming.config import BinanceConfig
        config = BinanceConfig(TRADING_PAIRS=["solusdt", "dogeusdt"])

        assert config.TRADING_PAIRS == ["solusdt", "dogeusdt"]

    def test_kafka_config_defaults(self):
        """Vérifie les valeurs par défaut de KafkaConfig"""
        from streaming.config import KafkaConfig
        config = KafkaConfig()

        assert config.BOOTSTRAP_SERVERS is not None
        assert len(config.BOOTSTRAP_SERVERS) > 0

    def test_kafka_config_topics(self):
        """Vérifie que les topics sont bien définis"""
        from streaming.config import KafkaConfig
        config = KafkaConfig()

        assert "raw_trades" in config.TOPICS
        assert "raw_klines" in config.TOPICS
        assert config.TOPICS["raw_trades"] == "raw_trades"
        assert config.TOPICS["raw_klines"] == "raw_klines"

    def test_kafka_config_env_override(self):
        """Vérifie que KAFKA_BOOTSTRAP_SERVERS override la valeur par défaut"""
        from streaming.config import KafkaConfig
        with patch.dict(os.environ, {"KAFKA_BOOTSTRAP_SERVERS": "custom-kafka:9092"}):
            config = KafkaConfig()
            assert config.BOOTSTRAP_SERVERS == ["custom-kafka:9092"]

    def test_kafka_producer_config(self):
        """Vérifie la config du producer"""
        from streaming.config import KafkaConfig
        config = KafkaConfig()

        assert config.PRODUCER_CONFIG["acks"] == "all"
        assert config.PRODUCER_CONFIG["compression_type"] == "gzip"
        assert config.PRODUCER_CONFIG["retries"] == 3
        assert config.PRODUCER_CONFIG["linger_ms"] == 10

    def test_kafka_consumer_config(self):
        """Vérifie la config du consumer"""
        from streaming.config import KafkaConfig
        config = KafkaConfig()

        assert config.CONSUMER_CONFIG["group_id"] == "binance_consumer_group"
        assert config.CONSUMER_CONFIG["auto_offset_reset"] == "latest"
        assert config.CONSUMER_CONFIG["enable_auto_commit"] is True


# ============================================
# TEST 2: Kafka Producer (kafka_producer.py)
# ============================================
class TestKafkaProducer:
    """Tests pour BinanceKafkaProducer (avec mock Kafka)"""

    @patch("streaming.kafka_producer.KafkaProducer")
    def test_determine_topic_trades(self, mock_kafka):
        """aggTrade → raw_trades"""
        from streaming.kafka_producer import BinanceKafkaProducer
        producer = BinanceKafkaProducer()

        assert producer._determine_topic("btcusdt@aggTrade") == "raw_trades"
        assert producer._determine_topic("ethusdt@trade") == "raw_trades"

    @patch("streaming.kafka_producer.KafkaProducer")
    def test_determine_topic_klines(self, mock_kafka):
        """kline → raw_klines"""
        from streaming.kafka_producer import BinanceKafkaProducer
        producer = BinanceKafkaProducer()

        assert producer._determine_topic("btcusdt@kline_1m") == "raw_klines"
        assert producer._determine_topic("ethusdt@kline_5m") == "raw_klines"

    @patch("streaming.kafka_producer.KafkaProducer")
    def test_determine_topic_unknown(self, mock_kafka):
        """Stream inconnu → raw_trades (défaut)"""
        from streaming.kafka_producer import BinanceKafkaProducer
        producer = BinanceKafkaProducer()

        assert producer._determine_topic("unknown@stream") == "raw_trades"

    @patch("streaming.kafka_producer.KafkaProducer")
    def test_extract_trading_pair(self, mock_kafka):
        """Extrait la paire de trading du stream name"""
        from streaming.kafka_producer import BinanceKafkaProducer
        producer = BinanceKafkaProducer()

        assert producer._extract_trading_pair("btcusdt@aggTrade") == "btcusdt"
        assert producer._extract_trading_pair("ethusdt@kline_1m") == "ethusdt"
        assert producer._extract_trading_pair("bnbusdt@ticker") == "bnbusdt"

    @patch("streaming.kafka_producer.KafkaProducer")
    def test_send_message_calls_producer(self, mock_kafka_class):
        """send_message appelle producer.send avec le bon topic et la bonne clé"""
        from streaming.kafka_producer import BinanceKafkaProducer

        mock_producer_instance = MagicMock()
        mock_future = MagicMock()
        mock_producer_instance.send.return_value = mock_future
        mock_kafka_class.return_value = mock_producer_instance

        producer = BinanceKafkaProducer()

        test_data = {
            "stream": "btcusdt@aggTrade",
            "data": {"s": "BTCUSDT", "p": "50000.00"},
            "received_at": datetime.now().isoformat()
        }

        result = producer.send_message(test_data)

        assert result is True
        mock_producer_instance.send.assert_called_once_with(
            topic="raw_trades",
            key="btcusdt",
            value=test_data
        )

    @patch("streaming.kafka_producer.KafkaProducer")
    def test_send_message_kline_topic(self, mock_kafka_class):
        """send_message route correctement les klines"""
        from streaming.kafka_producer import BinanceKafkaProducer

        mock_producer_instance = MagicMock()
        mock_future = MagicMock()
        mock_producer_instance.send.return_value = mock_future
        mock_kafka_class.return_value = mock_producer_instance

        producer = BinanceKafkaProducer()

        test_data = {
            "stream": "ethusdt@kline_1m",
            "data": {"s": "ETHUSDT", "k": {}},
            "received_at": datetime.now().isoformat()
        }

        producer.send_message(test_data)

        mock_producer_instance.send.assert_called_once_with(
            topic="raw_klines",
            key="ethusdt",
            value=test_data
        )

    @patch("streaming.kafka_producer.KafkaProducer")
    def test_stats_initial(self, mock_kafka):
        """Les stats sont initialisées à zéro"""
        from streaming.kafka_producer import BinanceKafkaProducer
        producer = BinanceKafkaProducer()
        stats = producer.get_stats()

        assert stats["messages_sent"] == 0
        assert stats["messages_failed"] == 0
        assert stats["by_topic"] == {}

    @patch("streaming.kafka_producer.KafkaProducer")
    def test_on_send_success_updates_stats(self, mock_kafka):
        """Le callback success met à jour les stats"""
        from streaming.kafka_producer import BinanceKafkaProducer
        producer = BinanceKafkaProducer()

        producer._on_send_success(MagicMock(), "raw_trades")
        producer._on_send_success(MagicMock(), "raw_trades")
        producer._on_send_success(MagicMock(), "raw_klines")

        assert producer.stats["messages_sent"] == 3
        assert producer.stats["by_topic"]["raw_trades"] == 2
        assert producer.stats["by_topic"]["raw_klines"] == 1

    @patch("streaming.kafka_producer.KafkaProducer")
    def test_on_send_error_updates_stats(self, mock_kafka):
        """Le callback error met à jour les stats"""
        from streaming.kafka_producer import BinanceKafkaProducer
        producer = BinanceKafkaProducer()

        producer._on_send_error(Exception("test"), "raw_trades")

        assert producer.stats["messages_failed"] == 1

    @patch("streaming.kafka_producer.KafkaProducer")
    def test_flush(self, mock_kafka_class):
        """flush() appelle producer.flush()"""
        from streaming.kafka_producer import BinanceKafkaProducer
        mock_instance = MagicMock()
        mock_kafka_class.return_value = mock_instance

        producer = BinanceKafkaProducer()
        producer.flush()

        mock_instance.flush.assert_called_once()

    @patch("streaming.kafka_producer.KafkaProducer")
    def test_close(self, mock_kafka_class):
        """close() appelle flush + close sur le producer"""
        from streaming.kafka_producer import BinanceKafkaProducer
        mock_instance = MagicMock()
        mock_kafka_class.return_value = mock_instance

        producer = BinanceKafkaProducer()
        producer.close()

        mock_instance.flush.assert_called_once()
        mock_instance.close.assert_called_once()


# ============================================
# TEST 3: WebSocket Binance (binance_websocket.py)
# ============================================
class TestBinanceWebSocket:
    """Tests pour BinanceWebSocketConnector (avec mock WebSocket)"""

    @patch("streaming.binance_websocket.websocket")
    def test_build_stream_url(self, mock_ws_module):
        """Construit la bonne URL multi-stream"""
        from streaming.binance_websocket import BinanceWebSocketConnector

        connector = BinanceWebSocketConnector(
            trading_pairs=["btcusdt", "ethusdt"],
            stream_types=["trade"]
        )

        url = connector._build_stream_url()

        assert "stream.binance.com" in url
        assert "btcusdt@aggTrade" in url
        assert "ethusdt@aggTrade" in url

    @patch("streaming.binance_websocket.websocket")
    def test_build_stream_url_multiple_types(self, mock_ws_module):
        """URL avec plusieurs types de streams"""
        from streaming.binance_websocket import BinanceWebSocketConnector

        connector = BinanceWebSocketConnector(
            trading_pairs=["btcusdt"],
            stream_types=["trade", "kline_1m"]
        )

        url = connector._build_stream_url()

        assert "btcusdt@aggTrade" in url
        assert "btcusdt@kline_1m" in url

    @patch("streaming.binance_websocket.websocket")
    def test_on_message_calls_callback(self, mock_ws_module):
        """_on_message parse le JSON et appelle le callback"""
        from streaming.binance_websocket import BinanceWebSocketConnector

        callback = MagicMock()
        connector = BinanceWebSocketConnector(
            trading_pairs=["btcusdt"],
            stream_types=["trade"],
            message_callback=callback
        )

        # Simuler un message Binance
        test_message = json.dumps({
            "stream": "btcusdt@aggTrade",
            "data": {
                "e": "aggTrade",
                "s": "BTCUSDT",
                "p": "50000.00",
                "q": "0.001"
            }
        })

        connector._on_message(MagicMock(), test_message)

        # Le callback doit avoir été appelé
        assert callback.called
        call_args = callback.call_args[0][0]
        assert call_args["stream"] == "btcusdt@aggTrade"

    @patch("streaming.binance_websocket.websocket")
    def test_on_message_updates_stats(self, mock_ws_module):
        """_on_message incrémente le compteur de messages"""
        from streaming.binance_websocket import BinanceWebSocketConnector

        connector = BinanceWebSocketConnector(
            trading_pairs=["btcusdt"],
            stream_types=["trade"]
        )

        test_message = json.dumps({
            "stream": "btcusdt@aggTrade",
            "data": {"e": "aggTrade"}
        })

        connector._on_message(MagicMock(), test_message)

        assert connector.stats["messages_received"] >= 1

    @patch("streaming.binance_websocket.websocket")
    def test_stats_initial(self, mock_ws_module):
        """Les stats sont initialisées correctement"""
        from streaming.binance_websocket import BinanceWebSocketConnector

        connector = BinanceWebSocketConnector(
            trading_pairs=["btcusdt"],
            stream_types=["trade"]
        )

        stats = connector.get_stats()
        assert stats["messages_received"] == 0
        assert stats["errors"] == 0

    @patch("streaming.binance_websocket.websocket")
    def test_on_error_updates_stats(self, mock_ws_module):
        """_on_error incrémente le compteur d'erreurs"""
        from streaming.binance_websocket import BinanceWebSocketConnector

        connector = BinanceWebSocketConnector(
            trading_pairs=["btcusdt"],
            stream_types=["trade"]
        )

        connector._on_error(MagicMock(), Exception("test error"))

        assert connector.stats["errors"] == 1

    @patch("streaming.binance_websocket.websocket")
    def test_default_trading_pairs(self, mock_ws_module):
        """Sans paramètres, utilise les paires par défaut"""
        from streaming.binance_websocket import BinanceWebSocketConnector

        connector = BinanceWebSocketConnector()

        assert "btcusdt" in connector.trading_pairs
        assert "ethusdt" in connector.trading_pairs
        assert "bnbusdt" in connector.trading_pairs


# ============================================
# TEST 4: Pipeline (pipeline.py)
# ============================================
class TestPipeline:
    """Tests pour BinanceKafkaPipeline (avec mocks)"""

    @patch("streaming.pipeline.BinanceWebSocketConnector")
    @patch("streaming.pipeline.BinanceKafkaProducer")
    def test_pipeline_init(self, mock_producer_class, mock_ws_class):
        """Le pipeline initialise correctement les composants"""
        from streaming.pipeline import BinanceKafkaPipeline

        pipeline = BinanceKafkaPipeline(
            trading_pairs=["btcusdt"],
            stream_types=["trade"]
        )

        assert pipeline.trading_pairs == ["btcusdt"]
        assert pipeline.stream_types == ["trade"]
        mock_producer_class.assert_called_once()
        mock_ws_class.assert_called_once()

    @patch("streaming.pipeline.BinanceWebSocketConnector")
    @patch("streaming.pipeline.BinanceKafkaProducer")
    def test_handle_message_sends_to_kafka(self, mock_producer_class, mock_ws_class):
        """Le callback transmet les messages au producer"""
        from streaming.pipeline import BinanceKafkaPipeline

        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer

        pipeline = BinanceKafkaPipeline(
            trading_pairs=["btcusdt"],
            stream_types=["trade"]
        )

        test_data = {
            "stream": "btcusdt@aggTrade",
            "data": {"s": "BTCUSDT", "p": "50000.00"},
            "received_at": "2024-01-01T00:00:00"
        }

        pipeline._handle_websocket_message(test_data)

        mock_producer.send_message.assert_called_once_with(test_data)

    @patch("streaming.pipeline.BinanceWebSocketConnector")
    @patch("streaming.pipeline.BinanceKafkaProducer")
    def test_stop_closes_components(self, mock_producer_class, mock_ws_class):
        """stop() ferme le WebSocket et le Producer"""
        from streaming.pipeline import BinanceKafkaPipeline

        mock_producer = MagicMock()
        mock_ws = MagicMock()
        mock_producer_class.return_value = mock_producer
        mock_ws_class.return_value = mock_ws

        pipeline = BinanceKafkaPipeline(
            trading_pairs=["btcusdt"],
            stream_types=["trade"]
        )

        # Mock get_stats pour _print_global_stats
        mock_ws.get_stats.return_value = {"messages_received": 0, "errors": 0}
        mock_producer.get_stats.return_value = {
            "messages_sent": 0, "messages_failed": 0, "by_topic": {}
        }

        pipeline.stop()

        mock_ws.stop.assert_called_once()
        mock_producer.close.assert_called_once()

    @patch("streaming.pipeline.BinanceWebSocketConnector")
    @patch("streaming.pipeline.BinanceKafkaProducer")
    def test_default_trading_pairs(self, mock_producer_class, mock_ws_class):
        """Sans paramètres, utilise les paires par défaut de la config"""
        from streaming.pipeline import BinanceKafkaPipeline

        pipeline = BinanceKafkaPipeline()

        assert "btcusdt" in pipeline.trading_pairs
        assert "ethusdt" in pipeline.trading_pairs
        assert "bnbusdt" in pipeline.trading_pairs

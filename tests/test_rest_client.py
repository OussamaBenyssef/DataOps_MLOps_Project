"""
Tests unitaires + intégration — BinanceRESTClient (P2)

Tests:
1. TestRESTClientFormat  — formatage des klines au format WebSocket
2. TestRESTClientPagination — logique de pagination
3. TestRESTClientIngestion — publication vers Kafka (mocké)
4. TestRESTClientUtils — utilitaires (_iso_to_ms, stats)
5. TestRESTClientIntegration — test d'intégration avec vraie API Binance
"""

import os
import sys
import time
import pytest
from unittest.mock import patch, MagicMock, call
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

# Condition pour les tests d'intégration (nécessite RUN_INTEGRATION=1)
RUN_INTEGRATION = os.getenv("RUN_INTEGRATION", "0") == "1"
skip_integration = pytest.mark.skipif(
    not RUN_INTEGRATION,
    reason="Tests d'intégration désactivés. Lancer avec RUN_INTEGRATION=1 pour les activer."
)


# ============================================
# TEST 1: Formatage kline → format WebSocket
# ============================================
class TestRESTClientFormat:
    """Tests pour _format_kline_as_websocket"""

    @patch("streaming.binance_rest_client.BinanceKafkaProducer")
    def setup_method(self, method, mock_producer):
        """Crée un client avec producer mocké"""
        from streaming.binance_rest_client import BinanceRESTClient
        self.client = BinanceRESTClient()

    def _make_kline(self, open_=50000, high=50100, low=49900, close=50050, volume=123.456):
        """Crée une kline brute au format Binance REST"""
        return [
            1705276800000,   # 0: open_time (ms)
            str(open_),      # 1: open
            str(high),       # 2: high
            str(low),        # 3: low
            str(close),      # 4: close
            str(volume),     # 5: volume
            1705276859999,   # 6: close_time (ms)
            "6172800.123",   # 7: quote_volume
            1500,            # 8: num_trades
            "61.728",        # 9: taker_buy_base_volume
            "3086400.0",     # 10: taker_buy_quote_volume
            "0",             # 11: ignore
        ]

    @patch("streaming.binance_rest_client.BinanceKafkaProducer")
    def test_format_stream_name(self, mock_producer):
        """Le stream name est en minuscules format <symbol>@kline_<interval>"""
        from streaming.binance_rest_client import BinanceRESTClient
        client = BinanceRESTClient()
        msg = client._format_kline_as_websocket("BTCUSDT", "1m", self._make_kline())
        assert msg["stream"] == "btcusdt@kline_1m"

    @patch("streaming.binance_rest_client.BinanceKafkaProducer")
    def test_format_source_field(self, mock_producer):
        """Le champ source doit être 'rest_api'"""
        from streaming.binance_rest_client import BinanceRESTClient
        client = BinanceRESTClient()
        msg = client._format_kline_as_websocket("BTCUSDT", "1m", self._make_kline())
        assert msg["source"] == "rest_api"

    @patch("streaming.binance_rest_client.BinanceKafkaProducer")
    def test_format_data_structure(self, mock_producer):
        """La structure data est identique au format WebSocket Binance"""
        from streaming.binance_rest_client import BinanceRESTClient
        client = BinanceRESTClient()
        msg = client._format_kline_as_websocket("ETHUSDT", "5m", self._make_kline())

        data = msg["data"]
        assert data["e"] == "kline"
        assert data["s"] == "ETHUSDT"
        assert "E" in data           # event timestamp
        assert "k" in data

    @patch("streaming.binance_rest_client.BinanceKafkaProducer")
    def test_format_kline_ohlcv(self, mock_producer):
        """Les valeurs OHLCV sont correctement mappées"""
        from streaming.binance_rest_client import BinanceRESTClient
        client = BinanceRESTClient()
        kline = self._make_kline(open_=50000, high=50100, low=49900, close=50050, volume=123.456)
        msg = client._format_kline_as_websocket("BTCUSDT", "1m", kline)

        k = msg["data"]["k"]
        assert k["o"] == "50000"
        assert k["h"] == "50100"
        assert k["l"] == "49900"
        assert k["c"] == "50050"
        assert k["v"] == "123.456"
        assert k["i"] == "1m"
        assert k["x"] is True       # Historique = kline toujours fermée

    @patch("streaming.binance_rest_client.BinanceKafkaProducer")
    def test_format_timestamps(self, mock_producer):
        """Les timestamps open/close sont bien mappés"""
        from streaming.binance_rest_client import BinanceRESTClient
        client = BinanceRESTClient()
        msg = client._format_kline_as_websocket("BTCUSDT", "1m", self._make_kline())

        k = msg["data"]["k"]
        assert k["t"] == 1705276800000   # open_time
        assert k["T"] == 1705276859999   # close_time

    @patch("streaming.binance_rest_client.BinanceKafkaProducer")
    def test_format_num_trades(self, mock_producer):
        """Le nombre de trades est bien mappé"""
        from streaming.binance_rest_client import BinanceRESTClient
        client = BinanceRESTClient()
        msg = client._format_kline_as_websocket("BTCUSDT", "1m", self._make_kline())
        assert msg["data"]["k"]["n"] == 1500

    @patch("streaming.binance_rest_client.BinanceKafkaProducer")
    def test_format_received_at(self, mock_producer):
        """received_at est bien formaté en ISO 8601"""
        from streaming.binance_rest_client import BinanceRESTClient
        client = BinanceRESTClient()
        msg = client._format_kline_as_websocket("BTCUSDT", "1m", self._make_kline())
        # Doit être parseable
        datetime.strptime(msg["received_at"], "%Y-%m-%dT%H:%M:%SZ")

    @patch("streaming.binance_rest_client.BinanceKafkaProducer")
    def test_format_symbol_uppercase(self, mock_producer):
        """Le symbole dans data est toujours en MAJUSCULES"""
        from streaming.binance_rest_client import BinanceRESTClient
        client = BinanceRESTClient()
        msg = client._format_kline_as_websocket("btcusdt", "1m", self._make_kline())
        assert msg["data"]["s"] == "BTCUSDT"
        assert msg["data"]["k"]["s"] == "BTCUSDT"


# ============================================
# TEST 2: Utilitaires (_iso_to_ms, stats)
# ============================================
class TestRESTClientUtils:
    """Tests pour les fonctions utilitaires"""

    def test_iso_to_ms_full_format(self):
        """Parse ISO 8601 complet avec Z → timestamp UTC en ms"""
        from streaming.binance_rest_client import BinanceRESTClient
        ms = BinanceRESTClient._iso_to_ms("2024-01-15T10:30:00Z")
        expected = int(datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc).timestamp() * 1000)
        assert ms == expected

    def test_iso_to_ms_without_z(self):
        """Parse ISO 8601 sans Z → même résultat qu'avec Z"""
        from streaming.binance_rest_client import BinanceRESTClient
        ms = BinanceRESTClient._iso_to_ms("2024-01-15T10:30:00")
        expected = int(datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc).timestamp() * 1000)
        assert ms == expected

    def test_iso_to_ms_date_only(self):
        """Parse format date uniquement"""
        from streaming.binance_rest_client import BinanceRESTClient
        ms = BinanceRESTClient._iso_to_ms("2024-01-15")
        expected = int(datetime(2024, 1, 15, tzinfo=timezone.utc).timestamp() * 1000)
        assert ms == expected

    def test_iso_to_ms_invalid_raises(self):
        """Format invalide lève une ValueError"""
        from streaming.binance_rest_client import BinanceRESTClient
        with pytest.raises(ValueError):
            BinanceRESTClient._iso_to_ms("not-a-date")

    @patch("streaming.binance_rest_client.BinanceKafkaProducer")
    def test_stats_initial_values(self, mock_producer):
        """Les stats sont initialisées à zéro"""
        from streaming.binance_rest_client import BinanceRESTClient
        client = BinanceRESTClient()
        stats = client.get_stats()
        assert stats["requests_made"] == 0
        assert stats["klines_fetched"] == 0
        assert stats["messages_published"] == 0
        assert stats["messages_failed"] == 0

    @patch("streaming.binance_rest_client.BinanceKafkaProducer")
    def test_close_calls_producer_and_session(self, mock_producer_class):
        """close() ferme le producer Kafka et la session HTTP"""
        from streaming.binance_rest_client import BinanceRESTClient
        mock_prod = MagicMock()
        mock_producer_class.return_value = mock_prod

        client = BinanceRESTClient()
        with patch.object(client.session, "close") as mock_session_close:
            client.close()
            mock_prod.close.assert_called_once()
            mock_session_close.assert_called_once()


# ============================================
# TEST 3: Ingestion + publication Kafka (mocké)
# ============================================
class TestRESTClientIngestion:
    """Tests pour ingest_historical et ingest_multiple_symbols"""

    def _make_raw_klines(self, n=5):
        """Génère n klines brutes"""
        return [
            [1705276800000 + i * 60000, "50000", "50100", "49900", "50050",
             "123.456", 1705276859999 + i * 60000, "6172800", 1500, "61.728",
             "3086400", "0"]
            for i in range(n)
        ]

    @patch("streaming.binance_rest_client.BinanceKafkaProducer")
    def test_ingest_publishes_all_klines(self, mock_producer_class):
        """ingest_historical publie toutes les klines dans Kafka"""
        from streaming.binance_rest_client import BinanceRESTClient
        mock_prod = MagicMock()
        mock_prod.send_message.return_value = True
        mock_producer_class.return_value = mock_prod

        client = BinanceRESTClient()

        with patch.object(client, "_fetch_klines_batch", return_value=self._make_raw_klines(5)):
            count = client.ingest_historical("BTCUSDT", "1m", limit=5)

        assert count == 5
        assert mock_prod.send_message.call_count == 5

    @patch("streaming.binance_rest_client.BinanceKafkaProducer")
    def test_ingest_counts_failures(self, mock_producer_class):
        """ingest_historical comptabilise les échecs d'envoi"""
        from streaming.binance_rest_client import BinanceRESTClient
        mock_prod = MagicMock()
        mock_prod.send_message.return_value = False   # Toujours en échec
        mock_producer_class.return_value = mock_prod

        client = BinanceRESTClient()

        with patch.object(client, "_fetch_klines_batch", return_value=self._make_raw_klines(3)):
            count = client.ingest_historical("BTCUSDT", "1m", limit=3)

        assert count == 0
        assert client.stats["messages_failed"] == 3

    @patch("streaming.binance_rest_client.BinanceKafkaProducer")
    def test_ingest_calls_flush_at_end(self, mock_producer_class):
        """ingest_historical appelle flush() à la fin"""
        from streaming.binance_rest_client import BinanceRESTClient
        mock_prod = MagicMock()
        mock_prod.send_message.return_value = True
        mock_producer_class.return_value = mock_prod

        client = BinanceRESTClient()

        with patch.object(client, "_fetch_klines_batch", return_value=self._make_raw_klines(2)):
            client.ingest_historical("BTCUSDT", "1m", limit=2)

        mock_prod.flush.assert_called_once()

    @patch("streaming.binance_rest_client.BinanceKafkaProducer")
    def test_ingest_empty_response_stops(self, mock_producer_class):
        """ingest_historical s'arrête si l'API retourne une liste vide"""
        from streaming.binance_rest_client import BinanceRESTClient
        mock_prod = MagicMock()
        mock_prod.send_message.return_value = True
        mock_producer_class.return_value = mock_prod

        client = BinanceRESTClient()

        with patch.object(client, "_fetch_klines_batch", return_value=[]):
            count = client.ingest_historical("BTCUSDT", "1m")

        assert count == 0

    @patch("streaming.binance_rest_client.BinanceKafkaProducer")
    def test_ingest_updates_stats(self, mock_producer_class):
        """ingest_historical met à jour les stats"""
        from streaming.binance_rest_client import BinanceRESTClient
        mock_prod = MagicMock()
        mock_prod.send_message.return_value = True
        mock_producer_class.return_value = mock_prod

        client = BinanceRESTClient()

        with patch.object(client, "_fetch_klines_batch", return_value=self._make_raw_klines(4)):
            client.ingest_historical("BTCUSDT", "1m", limit=4)

        assert client.stats["klines_fetched"] == 4
        assert client.stats["messages_published"] == 4

    @patch("streaming.binance_rest_client.BinanceKafkaProducer")
    def test_ingest_multiple_symbols(self, mock_producer_class):
        """ingest_multiple_symbols itère sur toutes les paires"""
        from streaming.binance_rest_client import BinanceRESTClient
        mock_prod = MagicMock()
        mock_prod.send_message.return_value = True
        mock_producer_class.return_value = mock_prod

        client = BinanceRESTClient()

        with patch.object(client, "ingest_historical", return_value=10) as mock_ingest:
            results = client.ingest_multiple_symbols(
                symbols=["BTCUSDT", "ETHUSDT"],
                interval="1m",
                limit=10
            )

        assert mock_ingest.call_count == 2
        assert results["BTCUSDT"] == 10
        assert results["ETHUSDT"] == 10

    @patch("streaming.binance_rest_client.BinanceKafkaProducer")
    def test_ingest_multiple_handles_errors(self, mock_producer_class):
        """ingest_multiple_symbols continue si une paire échoue"""
        from streaming.binance_rest_client import BinanceRESTClient
        mock_prod = MagicMock()
        mock_producer_class.return_value = mock_prod

        client = BinanceRESTClient()

        def failing_ingest(symbol, **kwargs):
            if symbol == "BTCUSDT":
                raise Exception("API error")
            return 5

        with patch.object(client, "ingest_historical", side_effect=failing_ingest):
            results = client.ingest_multiple_symbols(
                symbols=["BTCUSDT", "ETHUSDT"],
                limit=5
            )

        assert results["BTCUSDT"] == 0   # Erreur → 0
        assert results["ETHUSDT"] == 5   # OK → 5


# ============================================
# TEST 4: Intégration (vraie API Binance, skip si hors ligne)
# ============================================
class TestRESTClientIntegration:
    """Tests d'intégration — appels réels à l'API Binance (sans Kafka)"""

    @pytest.mark.integration
    @skip_integration
    @patch("streaming.binance_rest_client.BinanceKafkaProducer")
    def test_fetch_real_klines_returns_data(self, mock_producer):
        """Vérifie qu'on obtient bien des données de l'API Binance"""
        from streaming.binance_rest_client import BinanceRESTClient
        client = BinanceRESTClient()

        klines = client._fetch_klines_batch("BTCUSDT", "1m", limit=5)

        assert isinstance(klines, list)
        assert len(klines) == 5
        # Chaque kline a 12 champs
        assert len(klines[0]) == 12
        client.close()

    @pytest.mark.integration
    @skip_integration
    @patch("streaming.binance_rest_client.BinanceKafkaProducer")
    def test_fetch_klines_format_valid(self, mock_producer):
        """Les klines retournées ont des valeurs numériques valides"""
        from streaming.binance_rest_client import BinanceRESTClient
        client = BinanceRESTClient()

        klines = client._fetch_klines_batch("ETHUSDT", "1m", limit=3)

        for kline in klines:
            open_time = int(kline[0])
            open_price = float(kline[1])
            high = float(kline[2])
            low = float(kline[3])
            close = float(kline[4])
            volume = float(kline[5])

            assert open_time > 0
            assert high >= low
            assert high >= open_price
            assert high >= close
            assert volume >= 0

        client.close()

    @pytest.mark.integration
    @skip_integration
    @patch("streaming.binance_rest_client.BinanceKafkaProducer")
    def test_format_and_publish_pipeline(self, mock_producer_class):
        """Test E2E: fetch → format → publication (producer mocké)"""
        from streaming.binance_rest_client import BinanceRESTClient
        mock_prod = MagicMock()
        mock_prod.send_message.return_value = True
        mock_producer_class.return_value = mock_prod

        client = BinanceRESTClient()

        # Récupère 5 vraies klines et simule la publication
        klines = client._fetch_klines_batch("BTCUSDT", "1m", limit=5)

        for kline in klines:
            msg = client._format_kline_as_websocket("BTCUSDT", "1m", kline)
            client.producer.send_message(msg)

        # Vérifie le format de chaque message publié
        assert mock_prod.send_message.call_count == 5
        for call_args in mock_prod.send_message.call_args_list:
            msg = call_args[0][0]
            assert msg["stream"] == "btcusdt@kline_1m"
            assert msg["source"] == "rest_api"
            assert "data" in msg
            assert msg["data"]["e"] == "kline"
            assert msg["data"]["k"]["x"] is True

        client.close()

"""
Connecteur WebSocket Binance pour streaming temps réel
P2: Kafka Engineer - Mouad

Récupère les données de trading en temps réel depuis Binance via WebSocket:
- Trades agrégés (aggTrade)
- Candlesticks/Klines (1 minute)
- Ticker 24h
- Order book depth

Paires supportées: BTC/USDT, ETH/USDT, BNB/USDT
"""

import json
import time
import logging
from typing import Callable, Dict, List, Optional
from datetime import datetime
import websocket
import threading

from .config import binance_config

# Configuration du logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class BinanceWebSocketConnector:
    """
    Connecteur WebSocket pour Binance Spot Market

    Fonctionnalités:
    - Connexion multi-stream (plusieurs paires simultanément)
    - Reconnexion automatique en cas de déconnexion
    - Gestion des erreurs et logging
    - Callbacks personnalisés pour traitement des messages
    """

    def __init__(
        self,
        trading_pairs: Optional[List[str]] = None,
        stream_types: Optional[List[str]] = None,
        message_callback: Optional[Callable] = None,
    ):
        """
        Initialise le connecteur WebSocket

        Args:
            trading_pairs: Liste des paires (ex: ['btcusdt', 'ethusdt'])
            stream_types: Types de streams à écouter (ex: ['trade', 'kline_1m'])
            message_callback: Fonction à appeler lors de la réception d'un message
        """
        self.trading_pairs = trading_pairs or binance_config.TRADING_PAIRS
        self.stream_types = stream_types or ["trade", "kline_1m"]
        self.message_callback = message_callback

        self.ws: Optional[websocket.WebSocketApp] = None
        self.ws_thread: Optional[threading.Thread] = None
        self.is_running = False
        self.reconnect_attempts = 0

        # Statistiques
        self.stats = {
            "messages_received": 0,
            "messages_processed": 0,
            "errors": 0,
            "last_message_time": None,
            "start_time": None,
        }

        logger.info(f"Initialisation WebSocket Binance - Paires: {self.trading_pairs}")

    def _build_stream_url(self) -> str:
        """
        Construit l'URL WebSocket pour les streams combinés

        Format Binance: wss://stream.binance.com:9443/stream?streams=<stream1>/<stream2>/...
        """
        streams = []

        for pair in self.trading_pairs:
            for stream_type in self.stream_types:
                if stream_type == "trade":
                    streams.append(f"{pair}@aggTrade")
                elif stream_type == "kline_1m":
                    streams.append(f"{pair}@kline_1m")
                elif stream_type == "ticker":
                    streams.append(f"{pair}@ticker")
                elif stream_type == "depth":
                    streams.append(f"{pair}@depth20@100ms")

        stream_path = "/stream?streams=" + "/".join(streams)
        url = binance_config.SOCKET_BASE_URL.replace("/ws", stream_path)

        logger.info(f"URL WebSocket: {url}")
        return url

    def _on_message(self, ws, message: str):
        """
        Callback appelé lors de la réception d'un message

        Args:
            ws: Instance WebSocket
            message: Message JSON reçu
        """
        try:
            self.stats["messages_received"] += 1
            self.stats["last_message_time"] = datetime.now()

            # Parse le message JSON
            data = json.loads(message)

            # Extraction du stream et des données
            if "stream" in data and "data" in data:
                stream_name = data["stream"]
                stream_data = data["data"]

                # Enrichissement avec métadonnées
                enriched_data = {
                    "stream": stream_name,
                    "data": stream_data,
                    "received_at": datetime.now().isoformat(),
                    "source": "binance_websocket",
                }

                # Log périodique (tous les 100 messages)
                if self.stats["messages_received"] % 100 == 0:
                    logger.info(f"📊 Messages reçus: {self.stats['messages_received']} | " f"Stream: {stream_name}")

                # Appel du callback si défini
                if self.message_callback:
                    self.message_callback(enriched_data)
                    self.stats["messages_processed"] += 1

        except json.JSONDecodeError as e:
            logger.error(f"❌ Erreur décodage JSON: {e}")
            self.stats["errors"] += 1
        except Exception as e:
            logger.error(f"❌ Erreur traitement message: {e}")
            self.stats["errors"] += 1

    def _on_error(self, ws, error):
        """Callback appelé lors d'une erreur"""
        logger.error(f"❌ Erreur WebSocket: {error}")
        self.stats["errors"] += 1

    def _on_close(self, ws, close_status_code, close_msg):
        """Callback appelé lors de la fermeture de la connexion"""
        logger.warning(f"⚠️  Connexion WebSocket fermée - " f"Code: {close_status_code}, Message: {close_msg}")

        # Tentative de reconnexion si le service est toujours actif
        if self.is_running and self.reconnect_attempts < binance_config.MAX_RECONNECT_ATTEMPTS:
            self._reconnect()

    def _on_open(self, ws):
        """Callback appelé lors de l'ouverture de la connexion"""
        logger.info("✅ Connexion WebSocket établie avec succès!")
        self.reconnect_attempts = 0
        self.stats["start_time"] = datetime.now()

        # Log des streams actifs
        logger.info(f"🔴 Streaming en cours pour: {', '.join(self.trading_pairs)}")
        logger.info(f"📡 Types de données: {', '.join(self.stream_types)}")

    def _reconnect(self):
        """Tente de reconnecter au WebSocket"""
        self.reconnect_attempts += 1
        delay = binance_config.RECONNECT_DELAY * self.reconnect_attempts

        logger.info(
            f"🔄 Tentative de reconnexion {self.reconnect_attempts}/"
            f"{binance_config.MAX_RECONNECT_ATTEMPTS} dans {delay}s..."
        )

        time.sleep(delay)

        if self.is_running:
            self._start_websocket()

    def _start_websocket(self):
        """Démarre la connexion WebSocket"""
        url = self._build_stream_url()

        self.ws = websocket.WebSocketApp(
            url, on_message=self._on_message, on_error=self._on_error, on_close=self._on_close, on_open=self._on_open
        )

        # Lance le WebSocket dans un thread séparé
        self.ws_thread = threading.Thread(target=self.ws.run_forever)
        self.ws_thread.daemon = True
        self.ws_thread.start()

    def start(self):
        """
        Démarre le streaming WebSocket

        Le streaming s'exécute dans un thread séparé pour ne pas bloquer
        """
        if self.is_running:
            logger.warning("⚠️  Le streaming est déjà en cours")
            return

        logger.info("🚀 Démarrage du streaming Binance WebSocket...")
        self.is_running = True
        self._start_websocket()

    def stop(self):
        """
        Arrête le streaming WebSocket proprement
        """
        if not self.is_running:
            logger.warning("⚠️  Le streaming n'est pas en cours")
            return

        logger.info("🛑 Arrêt du streaming...")
        self.is_running = False

        if self.ws:
            self.ws.close()

        if self.ws_thread:
            self.ws_thread.join(timeout=5)

        # Affiche les statistiques finales
        self._print_stats()
        logger.info("✅ Streaming arrêté avec succès")

    def _print_stats(self):
        """Affiche les statistiques du connecteur"""
        duration = None
        if self.stats["start_time"]:
            duration = (datetime.now() - self.stats["start_time"]).total_seconds()

        logger.info("=" * 60)
        logger.info("📊 STATISTIQUES DU STREAMING")
        logger.info("=" * 60)
        logger.info(f"Messages reçus     : {self.stats['messages_received']}")
        logger.info(f"Messages traités   : {self.stats['messages_processed']}")
        logger.info(f"Erreurs            : {self.stats['errors']}")
        if duration:
            logger.info(f"Durée              : {duration:.2f}s")
            if self.stats["messages_received"] > 0:
                rate = self.stats["messages_received"] / duration
                logger.info(f"Débit              : {rate:.2f} msg/s")
        logger.info("=" * 60)

    def get_stats(self) -> Dict:
        """Retourne les statistiques actuelles"""
        return self.stats.copy()


# Fonction helper pour tester le connecteur
def test_connector():
    """
    Fonction de test pour vérifier le connecteur
    """

    def print_message(data):
        """Callback simple pour afficher les messages"""
        stream = data["stream"]
        print(f"\n📨 [{stream}]")

        if "aggTrade" in stream:
            trade = data["data"]
            print(
                f"   Prix: {trade['p']} | Quantité: {trade['q']} | " f"Temps: {datetime.fromtimestamp(trade['T']/1000)}"
            )

        elif "kline" in stream:
            kline = data["data"]["k"]
            print(f"   O: {kline['o']} | H: {kline['h']} | " f"L: {kline['l']} | C: {kline['c']}")

    # Crée et lance le connecteur
    connector = BinanceWebSocketConnector(
        trading_pairs=["btcusdt", "ethusdt"], stream_types=["trade"], message_callback=print_message
    )

    try:
        connector.start()
        logger.info("⏱️  Streaming pendant 30 secondes...")
        time.sleep(30)
    except KeyboardInterrupt:
        logger.info("\n⚠️  Interruption utilisateur")
    finally:
        connector.stop()


if __name__ == "__main__":
    test_connector()

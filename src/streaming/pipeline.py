"""
Pipeline complet Binance WebSocket → Kafka
P2: Kafka Engineer - Mouad

Intègre le WebSocket Binance avec Kafka Producer pour un pipeline
de streaming temps réel complet.
"""

import signal
import sys
import logging
from typing import Optional, List

from .binance_websocket import BinanceWebSocketConnector
from .kafka_producer import BinanceKafkaProducer
from .config import binance_config, kafka_config


# Configuration du logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class BinanceKafkaPipeline:
    """
    Pipeline complet de streaming: Binance WebSocket → Kafka

    Architecture:
    1. Connexion WebSocket Binance (multi-streams)
    2. Réception des données temps réel
    3. Envoi automatique vers Kafka (partitionnement intelligent)
    4. Monitoring et statistiques

    Usage:
        pipeline = BinanceKafkaPipeline(
            trading_pairs=["btcusdt", "ethusdt", "bnbusdt"],
            stream_types=["trade", "kline_1m"]
        )
        pipeline.start()
    """

    def __init__(
        self,
        trading_pairs: Optional[List[str]] = None,
        stream_types: Optional[List[str]] = None,
        kafka_bootstrap_servers: Optional[List[str]] = None,
    ):
        """
        Initialise le pipeline complet

        Args:
            trading_pairs: Liste des paires à suivre (ex: ['btcusdt', 'ethusdt'])
            stream_types: Types de streams (ex: ['trade', 'kline_1m'])
            kafka_bootstrap_servers: Brokers Kafka
        """
        self.trading_pairs = trading_pairs or binance_config.TRADING_PAIRS
        self.stream_types = stream_types or ["trade", "kline_1m"]

        # Initialisation du producer Kafka
        logger.info("🔧 Initialisation du Kafka Producer...")
        self.producer = BinanceKafkaProducer(bootstrap_servers=kafka_bootstrap_servers)

        # Initialisation du connecteur WebSocket avec callback
        logger.info("🔧 Initialisation du WebSocket Binance...")
        self.websocket = BinanceWebSocketConnector(
            trading_pairs=self.trading_pairs,
            stream_types=self.stream_types,
            message_callback=self._handle_websocket_message,
        )

        # Gestion du signal d'arrêt
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        logger.info("✅ Pipeline initialisé avec succès")

    def _handle_websocket_message(self, data: dict):
        """
        Callback appelé pour chaque message WebSocket
        Envoie automatiquement le message vers Kafka

        Args:
            data: Données reçues du WebSocket
        """
        # Envoi vers Kafka
        self.producer.send_message(data)

    def _signal_handler(self, sig, frame):
        """Gestion propre de l'arrêt (Ctrl+C)"""
        logger.info("\n⚠️  Signal d'arrêt reçu...")
        self.stop()
        sys.exit(0)

    def start(self):
        """
        Démarre le pipeline complet

        Lance le WebSocket qui commencera à recevoir les données
        et à les envoyer automatiquement vers Kafka
        """
        logger.info("=" * 60)
        logger.info("🚀 DÉMARRAGE DU PIPELINE BINANCE → KAFKA")
        logger.info("=" * 60)
        logger.info(f"📊 Paires de trading: {', '.join(self.trading_pairs)}")
        logger.info(f"📡 Types de streams: {', '.join(self.stream_types)}")
        logger.info(f"🔌 Kafka brokers: {', '.join(kafka_config.BOOTSTRAP_SERVERS)}")
        logger.info("=" * 60)
        logger.info("")
        logger.info("💡 Utilisez Ctrl+C pour arrêter le pipeline")
        logger.info("")

        # Démarrage du WebSocket (non-bloquant)
        self.websocket.start()

        # Maintien du programme actif
        try:
            # Le WebSocket tourne dans son propre thread
            # On garde juste le programme actif
            signal.pause()
        except KeyboardInterrupt:
            logger.info("\n⚠️  Interruption utilisateur")
            self.stop()

    def stop(self):
        """
        Arrête le pipeline proprement

        1. Arrête le WebSocket
        2. Flush et ferme le producer Kafka
        3. Affiche les statistiques
        """
        logger.info("")
        logger.info("=" * 60)
        logger.info("🛑 ARRÊT DU PIPELINE")
        logger.info("=" * 60)

        # Arrêt du WebSocket
        logger.info("📡 Arrêt du WebSocket...")
        self.websocket.stop()

        # Arrêt du producer Kafka
        logger.info("📤 Fermeture du producer Kafka...")
        self.producer.close()

        # Statistiques globales
        self._print_global_stats()

        logger.info("=" * 60)
        logger.info("✅ Pipeline arrêté avec succès")
        logger.info("=" * 60)

    def _print_global_stats(self):
        """Affiche les statistiques globales du pipeline"""
        ws_stats = self.websocket.get_stats()
        kafka_stats = self.producer.get_stats()

        logger.info("")
        logger.info("📊 STATISTIQUES GLOBALES DU PIPELINE")
        logger.info("-" * 60)

        # Stats WebSocket
        logger.info("🌐 WebSocket Binance:")
        logger.info(f"   Messages reçus: {ws_stats['messages_received']}")
        logger.info(f"   Erreurs: {ws_stats['errors']}")

        # Stats Kafka
        logger.info("\n📤 Kafka Producer:")
        logger.info(f"   Messages envoyés: {kafka_stats['messages_sent']}")
        logger.info(f"   Messages échoués: {kafka_stats['messages_failed']}")

        # Répartition par topic
        if kafka_stats["by_topic"]:
            logger.info("\n   Répartition par topic:")
            for topic, count in kafka_stats["by_topic"].items():
                logger.info(f"      {topic}: {count}")

        # Taux de succès
        total_received = ws_stats["messages_received"]
        total_sent = kafka_stats["messages_sent"]
        if total_received > 0:
            success_rate = (total_sent / total_received) * 100
            logger.info(f"\n✅ Taux de succès: {success_rate:.2f}%")


def main():
    """
    Point d'entrée principal du pipeline

    Peut être appelé directement ou importé
    """
    # Configuration par défaut pour toutes les paires
    pipeline = BinanceKafkaPipeline(trading_pairs=["btcusdt", "ethusdt", "bnbusdt"], stream_types=["trade", "kline_1m"])

    try:
        pipeline.start()
    except Exception as e:
        logger.error(f"❌ Erreur fatale: {e}")
        pipeline.stop()
        sys.exit(1)


if __name__ == "__main__":
    main()

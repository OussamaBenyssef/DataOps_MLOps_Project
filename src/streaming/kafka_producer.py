"""
Kafka Producer pour streaming de données Binance
P2: Kafka Engineer - Mouad

Envoie les données reçues du WebSocket Binance vers Kafka:
- Sérialisation JSON
- Partitionnement par paire de trading
- Gestion des erreurs et retry
- Métriques de performance
"""

import json
import logging
from typing import Dict, Optional, Any
from datetime import datetime
from kafka import KafkaProducer

from .config import kafka_config

# Configuration du logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class BinanceKafkaProducer:
    """
    Producer Kafka optimisé pour les données Binance

    Fonctionnalités:
    - Routage automatique vers le bon topic selon le type de données
    - Partitionnement par paire de trading pour parallélisme
    - Compression gzip pour économiser la bande passante
    - Statistiques en temps réel
    """

    def __init__(self, bootstrap_servers: Optional[list] = None):
        """
        Initialise le producer Kafka

        Args:
            bootstrap_servers: Liste des brokers Kafka
        """
        self.bootstrap_servers = bootstrap_servers or kafka_config.BOOTSTRAP_SERVERS
        self.producer: Optional[KafkaProducer] = None

        # Statistiques
        self.stats = {"messages_sent": 0, "messages_failed": 0, "bytes_sent": 0, "by_topic": {}, "start_time": None}

        self._init_producer()

    def _init_producer(self):
        """Initialise le producer Kafka avec la configuration optimale"""
        try:
            logger.info("🔌 Connexion à Kafka...")
            logger.info(f"Brokers: {self.bootstrap_servers}")

            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                # Sérialisation JSON
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                # Configuration optimale pour streaming temps réel
                acks=kafka_config.PRODUCER_CONFIG["acks"],
                compression_type=kafka_config.PRODUCER_CONFIG["compression_type"],
                max_in_flight_requests_per_connection=kafka_config.PRODUCER_CONFIG[
                    "max_in_flight_requests_per_connection"
                ],
                retries=kafka_config.PRODUCER_CONFIG["retries"],
                request_timeout_ms=kafka_config.PRODUCER_CONFIG["request_timeout_ms"],
                linger_ms=kafka_config.PRODUCER_CONFIG["linger_ms"],
                batch_size=kafka_config.PRODUCER_CONFIG["batch_size"],
            )

            self.stats["start_time"] = datetime.now()
            logger.info("✅ Producer Kafka initialisé avec succès")

        except Exception as e:
            logger.error(f"❌ Erreur initialisation producer Kafka: {e}")
            raise

    def _determine_topic(self, stream_name: str) -> str:
        """
        Détermine le topic Kafka selon le type de stream

        Args:
            stream_name: Nom du stream Binance (ex: 'btcusdt@aggTrade')

        Returns:
            Nom du topic Kafka
        """
        if "aggTrade" in stream_name or "trade" in stream_name:
            return kafka_config.TOPICS["raw_trades"]
        elif "kline" in stream_name:
            return kafka_config.TOPICS["raw_klines"]
        elif "ticker" in stream_name:
            return kafka_config.TOPICS["raw_ticker"]
        elif "depth" in stream_name:
            return kafka_config.TOPICS["raw_depth"]
        else:
            logger.warning(f"⚠️  Stream inconnu: {stream_name}, utilisation du topic par défaut")
            return kafka_config.TOPICS["raw_trades"]

    def _extract_trading_pair(self, stream_name: str) -> str:
        """
        Extrait la paire de trading du nom du stream

        Args:
            stream_name: Nom du stream (ex: 'btcusdt@aggTrade')

        Returns:
            Paire de trading (ex: 'btcusdt')
        """
        return stream_name.split("@")[0]

    def send_message(self, data: Dict[str, Any]) -> bool:
        """
        Envoie un message vers Kafka

        Args:
            data: Données formatées depuis le WebSocket Binance
                  Doit contenir les clés 'stream', 'data', 'received_at'

        Returns:
            True si l'envoi est réussi, False sinon
        """
        try:
            # Extraction des informations
            stream_name = data.get("stream", "unknown")
            topic = self._determine_topic(stream_name)
            trading_pair = self._extract_trading_pair(stream_name)

            # La clé permet le partitionnement par paire de trading
            key = trading_pair

            # Envoi asynchrone vers Kafka
            future = self.producer.send(topic=topic, key=key, value=data)

            # Callback pour gérer le succès/échec
            future.add_callback(self._on_send_success, topic)
            future.add_errback(self._on_send_error, topic)

            # Log périodique (tous les 100 messages)
            if self.stats["messages_sent"] % 100 == 0:
                logger.info(
                    f"📤 Messages envoyés: {self.stats['messages_sent']} | " f"Topic: {topic} | Pair: {trading_pair}"
                )

            return True

        except Exception as e:
            logger.error(f"❌ Erreur envoi message: {e}")
            self.stats["messages_failed"] += 1
            return False

    def _on_send_success(self, record_metadata, topic: str):
        """
        Callback appelé en cas de succès d'envoi

        Args:
            record_metadata: Métadonnées du record envoyé
            topic: Topic Kafka
        """
        self.stats["messages_sent"] += 1

        # Stats par topic
        if topic not in self.stats["by_topic"]:
            self.stats["by_topic"][topic] = 0
        self.stats["by_topic"][topic] += 1

    def _on_send_error(self, exc, topic: str):
        """
        Callback appelé en cas d'erreur d'envoi

        Args:
            exc: Exception levée
            topic: Topic Kafka
        """
        self.stats["messages_failed"] += 1
        logger.error(f"❌ Échec envoi vers {topic}: {exc}")

    def flush(self):
        """Force l'envoi de tous les messages en attente"""
        if self.producer:
            self.producer.flush()
            logger.info("✅ Tous les messages ont été envoyés")

    def close(self):
        """Ferme le producer proprement"""
        if self.producer:
            logger.info("🛑 Fermeture du producer...")
            self.producer.flush()
            self.producer.close()
            self._print_stats()
            logger.info("✅ Producer fermé avec succès")

    def _print_stats(self):
        """Affiche les statistiques du producer"""
        duration = None
        if self.stats["start_time"]:
            duration = (datetime.now() - self.stats["start_time"]).total_seconds()

        logger.info("=" * 60)
        logger.info("📊 STATISTIQUES DU PRODUCER KAFKA")
        logger.info("=" * 60)
        logger.info(f"Messages envoyés   : {self.stats['messages_sent']}")
        logger.info(f"Messages échoués   : {self.stats['messages_failed']}")

        if duration:
            logger.info(f"Durée              : {duration:.2f}s")
            if self.stats["messages_sent"] > 0:
                rate = self.stats["messages_sent"] / duration
                logger.info(f"Débit              : {rate:.2f} msg/s")

        logger.info("\n📦 Répartition par topic:")
        for topic, count in self.stats["by_topic"].items():
            logger.info(f"   {topic}: {count} messages")
        logger.info("=" * 60)

    def get_stats(self) -> Dict:
        """Retourne les statistiques actuelles"""
        return self.stats.copy()


# Fonction helper pour tester le producer
def test_producer():
    """
    Fonction de test pour vérifier le producer
    """
    logger.info("🧪 Test du Kafka Producer...")

    producer = BinanceKafkaProducer()

    # Données de test
    test_messages = [
        {
            "stream": "btcusdt@aggTrade",
            "data": {
                "e": "aggTrade",
                "E": 1639584000000,
                "s": "BTCUSDT",
                "p": "50000.00",
                "q": "0.001",
                "T": 1639584000000,
            },
            "received_at": datetime.now().isoformat(),
            "source": "test",
        },
        {
            "stream": "ethusdt@kline_1m",
            "data": {
                "e": "kline",
                "E": 1639584000000,
                "s": "ETHUSDT",
                "k": {"t": 1639584000000, "o": "4000.00", "h": "4010.00", "l": "3995.00", "c": "4005.00", "v": "100.0"},
            },
            "received_at": datetime.now().isoformat(),
            "source": "test",
        },
    ]

    # Envoi des messages de test
    for msg in test_messages:
        success = producer.send_message(msg)
        if success:
            logger.info(f"✅ Message test envoyé: {msg['stream']}")
        else:
            logger.error(f"❌ Échec envoi: {msg['stream']}")

    # Fermeture propre
    producer.close()
    logger.info("✅ Test terminé")


if __name__ == "__main__":
    test_producer()

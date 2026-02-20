"""
Module de streaming temps réel - Binance WebSocket & Kafka
P2: Kafka Engineer
"""

from .binance_websocket import BinanceWebSocketConnector
from .binance_rest_client import BinanceRESTClient
from .kafka_producer import BinanceKafkaProducer
from .pipeline import BinanceKafkaPipeline
from .config import binance_config, kafka_config, rest_config

__version__ = "2.0.0"

__all__ = [
    "BinanceWebSocketConnector",
    "BinanceRESTClient",
    "BinanceKafkaProducer",
    "BinanceKafkaPipeline",
    "binance_config",
    "kafka_config",
    "rest_config",
]

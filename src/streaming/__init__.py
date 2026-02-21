"""
Module de streaming temps réel - Binance WebSocket & Kafka
P2: Kafka Engineer
"""

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

# Imports paresseux : évitent de charger kafka-python / websocket / requests
# dès qu'on fait "import streaming" (ex: pour lire __version__ seulement)
def __getattr__(name):
    if name == "BinanceWebSocketConnector":
        from .binance_websocket import BinanceWebSocketConnector
        return BinanceWebSocketConnector
    if name == "BinanceRESTClient":
        from .binance_rest_client import BinanceRESTClient
        return BinanceRESTClient
    if name == "BinanceKafkaProducer":
        from .kafka_producer import BinanceKafkaProducer
        return BinanceKafkaProducer
    if name == "BinanceKafkaPipeline":
        from .pipeline import BinanceKafkaPipeline
        return BinanceKafkaPipeline
    if name in ("binance_config", "kafka_config", "rest_config"):
        from . import config as _config
        return getattr(_config, name)
    raise AttributeError(f"module 'streaming' has no attribute {name!r}")

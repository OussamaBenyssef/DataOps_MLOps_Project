"""
Configuration pour le streaming Binance et Kafka
"""

import os
from typing import List
from dataclasses import dataclass


@dataclass
class BinanceConfig:
    """Configuration pour le WebSocket Binance"""
    
    # Paires de trading à suivre
    TRADING_PAIRS: List[str] = None
    
    # URL WebSocket Binance
    SOCKET_BASE_URL: str = "wss://stream.binance.com:9443/ws"
    
    # Streams disponibles
    STREAM_TYPES: dict = None
    
    # Reconnexion
    MAX_RECONNECT_ATTEMPTS: int = 5
    RECONNECT_DELAY: int = 5  # secondes
    
    def __post_init__(self):
        if self.TRADING_PAIRS is None:
            self.TRADING_PAIRS = ["btcusdt", "ethusdt", "bnbusdt"]
        
        if self.STREAM_TYPES is None:
            self.STREAM_TYPES = {
                "trade": "aggTrade",      # Trades agrégés
                "kline_1m": "kline_1m",   # Candlesticks 1 minute
                "ticker": "ticker",       # Ticker 24h
                "depth": "depth20@100ms"  # Order book
            }


@dataclass
class KafkaConfig:
    """Configuration pour Kafka"""
    
    # Brokers Kafka
    BOOTSTRAP_SERVERS: List[str] = None
    
    # Topics
    TOPICS: dict = None
    
    # Producer settings
    PRODUCER_CONFIG: dict = None
    
    # Consumer settings
    CONSUMER_CONFIG: dict = None
    
    def __post_init__(self):
        if self.BOOTSTRAP_SERVERS is None:
            # Détection automatique du mode (Docker ou hôte)
            # Si KAFKA_BOOTSTRAP_SERVERS est défini en env, on l'utilise
            # Sinon, port 29092 pour connexions depuis l'hôte (hors Docker)
            kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
            self.BOOTSTRAP_SERVERS = [kafka_servers]
        
        if self.TOPICS is None:
            self.TOPICS = {
                "raw_trades": "raw_trades",
                "raw_klines": "raw_klines",
                "raw_ticker": "raw_ticker",
                "raw_depth": "raw_depth"
            }
        
        if self.PRODUCER_CONFIG is None:
            self.PRODUCER_CONFIG = {
                "acks": "all",                    # Attendre ack de tous les replicas
                "compression_type": "gzip",       # Compression pour économiser bande passante
                "max_in_flight_requests_per_connection": 5,
                "retries": 3,
                "request_timeout_ms": 30000,
                "linger_ms": 10,                  # Batch messages pendant 10ms
                "batch_size": 16384,              # 16KB batch size
            }
        
        if self.CONSUMER_CONFIG is None:
            self.CONSUMER_CONFIG = {
                "group_id": "binance_consumer_group",
                "auto_offset_reset": "latest",
                "enable_auto_commit": True,
                "auto_commit_interval_ms": 5000,
                "session_timeout_ms": 30000,
                "max_poll_records": 500,
            }


# Instances globales
binance_config = BinanceConfig()
kafka_config = KafkaConfig()

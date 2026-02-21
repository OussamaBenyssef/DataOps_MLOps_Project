"""
Binance REST API Client - Données historiques OHLCV
P2: Kafka Engineer - Mouad

Récupère les données OHLCV historiques via l'API REST Binance et les publie
dans Kafka sous le même format que le WebSocket pour garantir la qualité
des données via le même pipeline ETL Spark.

Flux: REST API Binance → Kafka (topic: raw_klines) → Spark ETL → MongoDB

Format de message identique au WebSocket:
{
  "stream": "btcusdt@kline_1m",
  "data": {
    "e": "kline",
    "E": <timestamp_ms>,
    "s": "BTCUSDT",
    "k": {
      "t": <open_time_ms>,
      "T": <close_time_ms>,
      "s": "BTCUSDT",
      "i": "1m",
      "o": "50000.00",
      "h": "50100.00",
      "l": "49900.00",
      "c": "50050.00",
      "v": "123.456",
      "n": 1500,
      "x": true
    }
  },
  "received_at": "2024-01-15T10:30:00Z",
  "source": "rest_api"          # distingue des données websocket
}
"""

import time
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone

import requests

from .config import binance_config, rest_config
from .kafka_producer import BinanceKafkaProducer


# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BinanceRESTClient:
    """
    Client REST Binance pour données historiques OHLCV

    Fonctionnalités:
    - Récupération des klines historiques via GET /api/v3/klines
    - Formatage identique au WebSocket (même schéma JSON)
    - Publication vers Kafka (topic: raw_klines) via BinanceKafkaProducer
    - Support du backfill par lots avec respect du rate-limit
    - Statistiques d'ingestion

    Exemple d'utilisation:
        client = BinanceRESTClient()
        client.ingest_historical(
            symbol="BTCUSDT",
            interval="1m",
            start_time="2024-01-01T00:00:00Z",
            end_time="2024-01-02T00:00:00Z"
        )
    """

    # Paramètres REST lus depuis rest_config (centralisé)
    BASE_URL: str = rest_config.BASE_URL
    KLINES_ENDPOINT: str = rest_config.KLINES_ENDPOINT
    MAX_KLINES_PER_REQUEST: int = rest_config.MAX_KLINES_PER_REQUEST
    REQUEST_DELAY_S: float = rest_config.REQUEST_DELAY_S

    def __init__(
        self,
        kafka_bootstrap_servers: Optional[List[str]] = None,
        request_timeout: int = 10,
    ):
        """
        Initialise le client REST et le producer Kafka.

        Args:
            kafka_bootstrap_servers: Brokers Kafka (utilise la config par défaut si None)
            request_timeout: Timeout (en secondes) pour les requêtes HTTP
        """
        self.request_timeout = request_timeout
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

        # Statistiques
        self.stats: Dict[str, Any] = {
            "requests_made": 0,
            "klines_fetched": 0,
            "messages_published": 0,
            "messages_failed": 0,
            "start_time": None,
        }

        logger.info("🔧 Initialisation du Kafka Producer (REST client)...")
        self.producer = BinanceKafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers
        )
        logger.info("✅ BinanceRESTClient prêt")

    # ------------------------------------------------------------------
    # Formatage
    # ------------------------------------------------------------------

    def _format_kline_as_websocket(
        self,
        symbol: str,
        interval: str,
        kline: List,
    ) -> Dict[str, Any]:
        """
        Convertit une kline REST au format identique au message WebSocket Binance.

        Args:
            symbol  : Paire de trading en MAJUSCULES (ex: 'BTCUSDT')
            interval: Intervalle de la kline (ex: '1m', '5m', '1h')
            kline   : Ligne brute retournée par /api/v3/klines

        Retourne:
            Dict au format WebSocket Binance enrichi avec ``source: rest_api``

        Format kline REST (index → valeur):
            0  open_time      (ms)
            1  open
            2  high
            3  low
            4  close
            5  volume
            6  close_time     (ms)
            7  quote_volume
            8  num_trades
            9  taker_buy_base_volume
            10 taker_buy_quote_volume
            11 ignore
        """
        symbol_lower = symbol.lower()
        stream_name = f"{symbol_lower}@kline_{interval}"

        open_time_ms: int = int(kline[0])
        close_time_ms: int = int(kline[6])
        num_trades: int = int(kline[8])
        # Capturer une seule fois pour éviter des timestamps incohérents
        now = datetime.now(timezone.utc)
        now_ms: int = int(now.timestamp() * 1000)
        received_at: str = now.strftime("%Y-%m-%dT%H:%M:%SZ")

        return {
            "stream": stream_name,
            "data": {
                "e": "kline",
                "E": now_ms,        # timestamp de réception (now)
                "s": symbol.upper(),
                "k": {
                    "t": open_time_ms,          # kline open time
                    "T": close_time_ms,         # kline close time
                    "s": symbol.upper(),
                    "i": interval,
                    "o": str(kline[1]),         # open
                    "h": str(kline[2]),         # high
                    "l": str(kline[3]),         # low
                    "c": str(kline[4]),         # close
                    "v": str(kline[5]),         # volume
                    "n": num_trades,            # number of trades
                    "x": True,                  # kline is closed (historical = always closed)
                    "q": str(kline[7]),         # quote asset volume
                    "V": str(kline[9]),         # taker buy base volume
                    "Q": str(kline[10]),        # taker buy quote volume
                },
            },
            "received_at": received_at,
            "source": rest_config.SOURCE_LABEL,  # distingue des données "websocket"
        }

    # ------------------------------------------------------------------
    # Appels API
    # ------------------------------------------------------------------

    def _fetch_klines_batch(
        self,
        symbol: str,
        interval: str,
        start_time_ms: Optional[int] = None,
        end_time_ms: Optional[int] = None,
        limit: int = MAX_KLINES_PER_REQUEST,
    ) -> List[List]:
        """
        Récupère un lot de klines depuis l'API REST Binance.

        Args:
            symbol       : Paire (ex: 'BTCUSDT')
            interval     : Intervalle (ex: '1m')
            start_time_ms: Début de la fenêtre en millisecondes UTC (optionnel)
            end_time_ms  : Fin de la fenêtre en millisecondes UTC (optionnel)
            limit        : Nombre de klines à récupérer (max 1000)

        Returns:
            Liste brute de klines
        """
        params: Dict[str, Any] = {
            "symbol": symbol.upper(),
            "interval": interval,
            "limit": min(limit, self.MAX_KLINES_PER_REQUEST),
        }
        if start_time_ms is not None:
            params["startTime"] = start_time_ms
        if end_time_ms is not None:
            params["endTime"] = end_time_ms

        url = self.BASE_URL + self.KLINES_ENDPOINT
        response = self.session.get(url, params=params, timeout=self.request_timeout)
        response.raise_for_status()

        self.stats["requests_made"] += 1
        klines = response.json()
        logger.debug(f"Binance REST → {len(klines)} klines pour {symbol} {interval}")
        return klines

    # ------------------------------------------------------------------
    # Ingestion principale
    # ------------------------------------------------------------------

    def ingest_historical(
        self,
        symbol: str,
        interval: str = "1m",
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> int:
        """
        Récupère les klines historiques et les publie dans Kafka.

        Gère automatiquement la pagination pour dépasser la limite des 1000
        klines par requête.

        Args:
            symbol    : Paire de trading (ex: 'BTCUSDT', 'ETHUSDT')
            interval  : Intervalle de la kline (ex: '1m', '5m', '1h', '1d')
            start_time: Début en ISO 8601 ou None pour les 1000 dernières klines
                        (ex: '2024-01-01T00:00:00Z')
            end_time  : Fin en ISO 8601 ou None pour maintenant
                        (ex: '2024-01-02T00:00:00Z')
            limit     : Nombre total de klines à récupérer (None = toutes sur la période)

        Returns:
            Nombre de messages publiés dans Kafka
        """
        self.stats["start_time"] = datetime.now(timezone.utc)
        logger.info(
            f"🚀 Début ingestion historique | symbol={symbol} | interval={interval} | "
            f"start={start_time} | end={end_time} | limit={limit}"
        )

        # Conversion des timestamps ISO → millisecondes
        start_ms: Optional[int] = self._iso_to_ms(start_time) if start_time else None
        end_ms: Optional[int] = self._iso_to_ms(end_time) if end_time else None

        total_published = 0
        total_fetched = 0   # Compteur séparé pour la pagination (indépendant des succès Kafka)
        current_start_ms = start_ms

        while True:
            # Calcul du batch_limit basé sur total_fetched (pas total_published)
            if limit is not None:
                remaining = limit - total_fetched
                if remaining <= 0:
                    break
                batch_limit = min(remaining, self.MAX_KLINES_PER_REQUEST)
            else:
                batch_limit = self.MAX_KLINES_PER_REQUEST

            try:
                klines = self._fetch_klines_batch(
                    symbol=symbol,
                    interval=interval,
                    start_time_ms=current_start_ms,
                    end_time_ms=end_ms,
                    limit=batch_limit,
                )
            except requests.HTTPError as e:
                logger.error(f"❌ Erreur HTTP lors de la récupération des klines: {e}")
                break
            except requests.RequestException as e:
                logger.error(f"❌ Erreur réseau: {e}")
                break

            if not klines:
                logger.info("✅ Aucune kline supplémentaire disponible.")
                break

            self.stats["klines_fetched"] += len(klines)
            total_fetched += len(klines)

            # Formatage + publication vers Kafka
            for kline in klines:
                message = self._format_kline_as_websocket(symbol, interval, kline)
                success = self.producer.send_message(message)
                if success:
                    total_published += 1
                    self.stats["messages_published"] += 1
                else:
                    self.stats["messages_failed"] += 1

            logger.info(
                f"📤 Lot publié: {len(klines)} klines | "
                f"Total publié: {total_published} | symbol={symbol}"
            )

            # Si moins que le batch_limit demandé sont retournées → on a tout récupéré
            if len(klines) < batch_limit:
                break

            # Pagination: le prochain lot commence juste après la dernière kline
            # close_time + 1 ms pour éviter les doublons
            last_close_time_ms: int = int(klines[-1][6])
            current_start_ms = last_close_time_ms + 1

            # Si on dépasse end_ms, on s'arrête
            if end_ms is not None and current_start_ms >= end_ms:
                break

            # Respect du rate-limit Binance
            time.sleep(self.REQUEST_DELAY_S)

        # S'assurer que tous les messages sont bien envoyés avant de continuer
        self.producer.flush()
        self._print_stats(symbol, interval)
        return total_published

    def ingest_multiple_symbols(
        self,
        symbols: Optional[List[str]] = None,
        interval: str = "1m",
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, int]:
        """
        Ingère les données historiques pour plusieurs paires de trading.

        Args:
            symbols   : Liste des paires (utilise binance_config.TRADING_PAIRS si None)
            interval  : Intervalle commun pour toutes les paires
            start_time: Début de la fenêtre (ISO 8601)
            end_time  : Fin de la fenêtre (ISO 8601)
            limit     : Nombre max de klines par paire

        Returns:
            Dict {symbol: nb_messages_publiés}
        """
        pairs = symbols or [p.upper() for p in binance_config.TRADING_PAIRS]
        results: Dict[str, int] = {}

        logger.info(f"🔄 Ingestion multi-symboles: {pairs}")

        for symbol in pairs:
            try:
                count = self.ingest_historical(
                    symbol=symbol,
                    interval=interval,
                    start_time=start_time,
                    end_time=end_time,
                    limit=limit,
                )
                results[symbol] = count
            except Exception as e:
                logger.error(f"❌ Erreur ingestion {symbol}: {e}")
                results[symbol] = 0
            # Petit délai entre les symboles
            time.sleep(self.REQUEST_DELAY_S)

        total = sum(results.values())
        logger.info(f"✅ Ingestion terminée | Total: {total} messages | Détail: {results}")
        return results

    # ------------------------------------------------------------------
    # Utilitaires
    # ------------------------------------------------------------------

    @staticmethod
    def _iso_to_ms(iso_str: str) -> int:
        """
        Convertit une chaîne ISO 8601 en timestamp millisecondes UTC.

        Formats supportés:
        - '2024-01-15T10:30:00Z'
        - '2024-01-15T10:30:00'
        - '2024-01-15'
        """
        iso_str = iso_str.strip().rstrip("Z")
        formats = ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d"]
        for fmt in formats:
            try:
                dt = datetime.strptime(iso_str, fmt).replace(tzinfo=timezone.utc)
                return int(dt.timestamp() * 1000)
            except ValueError:
                continue
        raise ValueError(
            f"Format de date non reconnu: '{iso_str}'. "
            "Utiliser ISO 8601, ex: '2024-01-15T10:30:00Z'"
        )

    def _print_stats(self, symbol: str, interval: str):
        """Affiche les statistiques d'ingestion."""
        duration = None
        if self.stats["start_time"]:
            duration = (
                datetime.now(timezone.utc) - self.stats["start_time"]
            ).total_seconds()

        logger.info("=" * 60)
        logger.info("📊 STATISTIQUES D'INGESTION REST → KAFKA")
        logger.info("=" * 60)
        logger.info(f"Symbole            : {symbol} ({interval})")
        logger.info(f"Requêtes REST      : {self.stats['requests_made']}")
        logger.info(f"Klines récupérées  : {self.stats['klines_fetched']}")
        logger.info(f"Messages publiés   : {self.stats['messages_published']}")
        logger.info(f"Messages échoués   : {self.stats['messages_failed']}")
        if duration:
            logger.info(f"Durée              : {duration:.2f}s")
            if self.stats["messages_published"] > 0:
                rate = self.stats["messages_published"] / duration
                logger.info(f"Débit              : {rate:.1f} msg/s")
        logger.info("=" * 60)

    def get_stats(self) -> Dict[str, Any]:
        """Retourne les statistiques courantes."""
        return self.stats.copy()

    def close(self):
        """Ferme proprement le producer Kafka et la session HTTP."""
        logger.info("🛑 Fermeture du BinanceRESTClient...")
        self.producer.close()
        self.session.close()
        logger.info("✅ BinanceRESTClient fermé")


# ---------------------------------------------------------------------------
# Fonction de test rapide
# ---------------------------------------------------------------------------

def test_rest_client():
    """
    Test rapide du client REST: récupère 10 klines BTCUSDT 1m et les publie.
    """
    logger.info("🧪 Test du BinanceRESTClient...")

    client = BinanceRESTClient()

    try:
        count = client.ingest_historical(
            symbol="BTCUSDT",
            interval="1m",
            limit=10,
        )
        logger.info(f"✅ Test réussi — {count} messages publiés dans Kafka")
    except Exception as e:
        logger.error(f"❌ Erreur durant le test: {e}")
        raise
    finally:
        client.close()


if __name__ == "__main__":
    test_rest_client()

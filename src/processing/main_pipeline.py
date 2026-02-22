"""
Pipeline ETL Principal - Orchestration du traitement Spark
==========================================================
Ce script orchestre le pipeline complet:
    1. Lecture des données depuis Kafka (streaming ou batch)
    2. Nettoyage et validation des données
    3. Calcul des indicateurs techniques (RSI, MACD, Bollinger)
    4. Persistance vers MongoDB

Usage:
    python main_pipeline.py --mode streaming
    python main_pipeline.py --mode batch
    python main_pipeline.py --mode test --debug
"""
import argparse
import logging
import sys
import time
from typing import Optional

from config import ProcessingConfig
from spark_session import create_spark_session, stop_spark_session
from kafka_consumer import get_klines_stream, get_trades_stream, aggregate_trades_to_ohlcv
from data_cleaning import clean_ohlcv_data, clean_trades_data
from technical_indicators import calculate_all_indicators
from mongodb_writer import (
    write_ohlcv_stream,
    write_indicators_stream,
    write_to_console,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ETL-Pipeline")


def run_streaming_pipeline(config: ProcessingConfig, debug: bool = False) -> None:
    """
    Lance le pipeline en mode streaming temps réel (Kafka → Spark → MongoDB).

    Args:
        config: Configuration complète du pipeline
        debug: Si True, sortie console au lieu de MongoDB
    """
    logger.info("=" * 60)
    logger.info("[Pipeline] Démarrage du mode STREAMING")
    logger.info("=" * 60)

    spark = create_spark_session(config.spark)
    queries = []

    try:
        # ── 1. Stream OHLCV (klines) ──────────────────────────────────────
        logger.info("[Pipeline] Lecture du stream OHLCV depuis Kafka...")
        klines_stream = get_klines_stream(spark, config.kafka)

        # Nettoyage (streaming = évalué en mode lazy)
        klines_cleaned = (
            klines_stream
            .filter(
                (klines_stream["open"] > 0) &
                (klines_stream["close"] > 0) &
                (klines_stream["high"] >= klines_stream["low"]) &
                klines_stream["symbol"].isNotNull()
            )
        )

        # Calcul des indicateurs
        klines_with_indicators = calculate_all_indicators(klines_cleaned, config.indicators)

        # ── 2. Écriture vers MongoDB (ou console en debug) ─────────────────
        checkpoint_dir = config.spark.checkpoint_location

        if debug:
            logger.info("[Pipeline] Mode DEBUG: sortie console")
            q1 = write_to_console(klines_cleaned, num_rows=5)
            q2 = write_to_console(klines_with_indicators, num_rows=5)
            queries.extend([q1, q2])
        else:
            logger.info("[Pipeline] Écriture vers MongoDB...")
            q1 = write_ohlcv_stream(klines_cleaned, config.mongodb, checkpoint_dir)
            q2 = write_indicators_stream(klines_with_indicators, config.mongodb, checkpoint_dir)
            queries.extend([q1, q2])

        logger.info(f"[Pipeline] {len(queries)} streams actifs. En attente des données...")

        # Attendre jusqu'à interruption (Ctrl+C)
        spark.streams.awaitAnyTermination()

    except KeyboardInterrupt:
        logger.info("[Pipeline] Arrêt demandé par l'utilisateur (Ctrl+C)")
    except Exception as e:
        logger.error(f"[Pipeline] Erreur critique: {e}", exc_info=True)
        raise
    finally:
        for q in queries:
            try:
                q.stop()
            except Exception:
                pass
        stop_spark_session(spark)
        logger.info("[Pipeline] Pipeline arrêté proprement")


def run_test_pipeline(config: ProcessingConfig) -> None:
    """
    Mode test: génère des données mock et valide le pipeline ETL + indicateurs.
    Aucune connexion Kafka ou MongoDB requise.
    """
    from pyspark.sql import Row
    from datetime import datetime

    logger.info("=" * 60)
    logger.info("[Pipeline] Démarrage du mode TEST")
    logger.info("=" * 60)

    spark = create_spark_session(config.spark)

    try:
        # ── Données mock OHLCV ─────────────────────────────────────────────
        now_ms = int(datetime.utcnow().timestamp() * 1000)
        interval_ms = 60_000  # 1 minute

        # Générer 30 bougies simulées pour BTCUSDT
        base_price = 45000.0
        rows = []
        for i in range(30):
            ts = now_ms - (30 - i) * interval_ms
            price_variation = (i % 5 - 2) * 100
            open_p  = base_price + price_variation
            close_p = open_p + (50 if i % 2 == 0 else -50)
            high_p  = max(open_p, close_p) + 100
            low_p   = min(open_p, close_p) - 100
            rows.append(Row(
                symbol="BTCUSDT",
                interval="1m",
                timestamp=ts,
                open=float(open_p),
                high=float(high_p),
                low=float(low_p),
                close=float(close_p),
                volume=100.0 + i * 10,
                trades=500 + i * 5,
            ))

        df_mock = spark.createDataFrame(rows)
        logger.info(f"[TEST] Données mock créées: {df_mock.count()} bougies BTCUSDT")

        # ── Test Nettoyage ─────────────────────────────────────────────────
        logger.info("[TEST] === Test du nettoyage des données ===")
        df_cleaned, metrics = clean_ohlcv_data(df_mock, config.cleaning)
        logger.info(f"[TEST] Métriques de nettoyage: {metrics}")
        assert metrics["final_count"] == metrics["initial_count"], "Données mock valides attendues!"
        logger.info("[TEST] ✅ Nettoyage RÉUSSI")

        # ── Test Indicateurs ───────────────────────────────────────────────
        logger.info("[TEST] === Test du calcul des indicateurs ===")
        df_indicators = calculate_all_indicators(df_cleaned, config.indicators)

        # Validation SMA_20
        sma_result = df_indicators.select("symbol", "timestamp", "sma_20", "rsi_14", "macd_line", "bb_middle").orderBy("timestamp")
        sma_result.show(5, truncate=False)

        # Vérifications de base
        total_rows = df_indicators.count()
        assert total_rows == df_cleaned.count(), "Le count doit être inchangé après calcul indicateurs"

        # Vérifier RSI dans [0, 100]
        rsi_out_of_range = df_indicators.filter(
            (df_indicators["rsi_14"] < 0) | (df_indicators["rsi_14"] > 100)
        ).count()
        assert rsi_out_of_range == 0, f"RSI hors plage [0,100]: {rsi_out_of_range} lignes"

        # Vérifier Bollinger: upper >= middle >= lower
        bb_invalid = df_indicators.filter(
            (df_indicators["bb_upper"] < df_indicators["bb_middle"]) |
            (df_indicators["bb_middle"] < df_indicators["bb_lower"])
        ).count()
        assert bb_invalid == 0, f"Bollinger Bands invalides: {bb_invalid} lignes"

        logger.info("[TEST] ✅ Indicateurs techniques RÉUSSIS")
        logger.info("[TEST] ✅ RSI(14) dans la plage [0, 100]")
        logger.info("[TEST] ✅ Bollinger Bands: upper ≥ middle ≥ lower")
        logger.info("[TEST] ✅ MACD calculé (line, signal, histogram)")
        logger.info(f"[TEST] ✅ {total_rows} lignes traitées avec succès")
        logger.info("")
        logger.info("[TEST] 🎉 Tous les tests ont passé!")

    except AssertionError as e:
        logger.error(f"[TEST] ❌ Assertion échouée: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"[TEST] ❌ Erreur: {e}", exc_info=True)
        sys.exit(1)
    finally:
        stop_spark_session(spark)


def main() -> None:
    """Point d'entrée principal du pipeline ETL"""
    parser = argparse.ArgumentParser(
        description="Pipeline ETL Spark - Nettoyage + Indicateurs Techniques Crypto"
    )
    parser.add_argument(
        "--mode",
        choices=["streaming", "batch", "test"],
        default="test",
        help="Mode d'exécution: streaming (Kafka temps réel), batch (historique), test (données mock)"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Mode debug: sortie console au lieu de MongoDB"
    )
    args = parser.parse_args()

    logger.info(f"[Pipeline] Mode: {args.mode} | Debug: {args.debug}")

    config = ProcessingConfig.from_env()

    if args.mode == "streaming":
        run_streaming_pipeline(config, debug=args.debug)
    elif args.mode == "batch":
        logger.warning("[Pipeline] Mode batch non encore implémenté, utilisation du mode test")
        run_test_pipeline(config)
    elif args.mode == "test":
        run_test_pipeline(config)


if __name__ == "__main__":
    main()

"""
Data Lineage Emitter - P5
Emits the full data lineage graph to DataHub:
    Kafka → Spark ETL → MongoDB → ML

Usage:
    # Local (requires DataHub GMS running)
    python -m src.lineage.emit_lineage

    # Docker
    docker exec datahub-actions python /app/src/lineage/emit_lineage.py

    # Custom GMS URL
    DATAHUB_GMS_URL=http://datahub-gms:8080 python -m src.lineage.emit_lineage
"""

from __future__ import annotations
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, List, Optional

# DataHub SDK imports
try:
    import datahub.emitter.mce_builder as builder
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.emitter.rest_emitter import DatahubRestEmitter
    from datahub.metadata.schema_classes import (
        UpstreamClass,
        UpstreamLineageClass,
        DatasetLineageTypeClass,
        DatasetPropertiesClass,
    )
    DATAHUB_AVAILABLE = True
except ImportError:
    # Fallbacks de typage pour éviter une erreur au chargement du module.
    builder = None
    MetadataChangeProposalWrapper = Any
    DatahubRestEmitter = Any
    UpstreamClass = Any
    UpstreamLineageClass = Any
    DatasetPropertiesClass = Any
    DATAHUB_AVAILABLE = False

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from src.lineage.config import lineage_config, LineageConfig

logger = logging.getLogger(__name__)


class DataLineageEmitter:
    """
    Emits the complete data lineage graph to DataHub GMS.

    Lineage edges:
        1. Kafka raw_trades   → Spark trades_cleaning
        2. Kafka raw_klines   → Spark ohlcv_processing
        3. Spark trades_cleaning  → MongoDB raw_trades
        4. Spark ohlcv_processing → MongoDB ohlcv
        5. Spark ohlcv_processing → MongoDB indicators
        6. MongoDB ohlcv + indicators → ML feature_engineering
        7. ML feature_engineering → ML xgboost_predictor + anomaly_detector
    """

    def __init__(self, config: Optional[LineageConfig] = None):
        self.config = config or lineage_config
        self.emitter: Optional[DatahubRestEmitter] = None
        self._events_emitted = 0

    # ── URN Builders ──────────────────────────────────────────────

    def make_kafka_urn(self, topic: str) -> str:
        """Build a DataHub URN for a Kafka topic."""
        return builder.make_dataset_urn(
            self.config.platforms.kafka, topic, self.config.datahub.environment
        )

    def make_spark_urn(self, job_key: str) -> str:
        """Build a DataHub URN for a Spark job (logical dataset)."""
        name = self.config.spark_jobs[job_key]
        return builder.make_dataset_urn(
            self.config.platforms.spark, name, self.config.datahub.environment
        )

    def make_mongodb_urn(self, collection: str) -> str:
        """Build a DataHub URN for a MongoDB collection."""
        return builder.make_dataset_urn(
            self.config.platforms.mongodb, collection, self.config.datahub.environment
        )

    def make_ml_urn(self, dataset_key: str) -> str:
        """Build a DataHub URN for an ML dataset."""
        name = self.config.ml_datasets[dataset_key]
        return builder.make_dataset_urn(
            self.config.platforms.ml, name, self.config.datahub.environment
        )

    # ── Core Emission ─────────────────────────────────────────────

    def _emit_upstream_lineage(
        self,
        downstream_urn: str,
        upstream_urns: List[str],
        lineage_type: str = "TRANSFORMED",
    ) -> MetadataChangeProposalWrapper:
        """
        Creates and (optionally) emits an UpstreamLineage aspect.

        Args:
            downstream_urn: The downstream dataset URN
            upstream_urns:  List of upstream dataset URNs
            lineage_type:   TRANSFORMED, COPY, or VIEW

        Returns:
            The MetadataChangeProposalWrapper event
        """
        upstreams = [
            UpstreamClass(
                dataset=urn,
                type=lineage_type,
            )
            for urn in upstream_urns
        ]

        lineage_aspect = UpstreamLineageClass(upstreams=upstreams)

        event = MetadataChangeProposalWrapper(
            entityUrn=downstream_urn,
            aspect=lineage_aspect,
        )

        if self.emitter:
            self.emitter.emit(event)
            self._events_emitted += 1

        return event

    def _emit_dataset_properties(
        self,
        urn: str,
        description: str,
        custom_properties: Optional[dict] = None,
    ) -> MetadataChangeProposalWrapper:
        """Emits dataset properties (description + metadata)."""
        props = DatasetPropertiesClass(
            description=description,
            customProperties=custom_properties or {},
        )
        event = MetadataChangeProposalWrapper(entityUrn=urn, aspect=props)

        if self.emitter:
            self.emitter.emit(event)
            self._events_emitted += 1

        return event

    # ── Lineage Edges ─────────────────────────────────────────────

    def emit_kafka_to_spark(self) -> List[MetadataChangeProposalWrapper]:
        """
        Edge 1-2: Kafka topics → Spark ETL jobs.
            raw_trades → trades_cleaning
            raw_klines → ohlcv_processing
        """
        logger.info("[Lineage] Kafka → Spark ETL")
        events = []

        # raw_trades → Spark trades_cleaning
        events.append(self._emit_upstream_lineage(
            downstream_urn=self.make_spark_urn("trades_cleaning"),
            upstream_urns=[self.make_kafka_urn("raw_trades")],
        ))

        # raw_klines → Spark ohlcv_processing
        events.append(self._emit_upstream_lineage(
            downstream_urn=self.make_spark_urn("ohlcv_processing"),
            upstream_urns=[self.make_kafka_urn("raw_klines")],
        ))

        return events

    def emit_spark_to_mongodb(self) -> List[MetadataChangeProposalWrapper]:
        """
        Edge 3-5: Spark ETL → MongoDB collections.
            trades_cleaning  → raw_trades
            ohlcv_processing → ohlcv
            ohlcv_processing → indicators (via indicators_calc)
        """
        logger.info("[Lineage] Spark ETL → MongoDB")
        events = []

        # Spark trades_cleaning → MongoDB raw_trades
        events.append(self._emit_upstream_lineage(
            downstream_urn=self.make_mongodb_urn("cryptomarket.raw_trades"),
            upstream_urns=[self.make_spark_urn("trades_cleaning")],
        ))

        # Spark ohlcv_processing → MongoDB ohlcv
        events.append(self._emit_upstream_lineage(
            downstream_urn=self.make_mongodb_urn("cryptomarket.ohlcv"),
            upstream_urns=[self.make_spark_urn("ohlcv_processing")],
        ))

        # Spark indicators_calc → MongoDB indicators
        events.append(self._emit_upstream_lineage(
            downstream_urn=self.make_mongodb_urn("cryptomarket.indicators"),
            upstream_urns=[self.make_spark_urn("indicators_calc")],
        ))

        return events

    def emit_mongodb_to_ml(self) -> List[MetadataChangeProposalWrapper]:
        """
        Edge 6-7: MongoDB → ML models.
            ohlcv + indicators → feature_engineering
            feature_engineering → xgboost_predictor + anomaly_detector
        """
        logger.info("[Lineage] MongoDB → ML Models")
        events = []

        # MongoDB ohlcv + indicators → ML feature_engineering
        events.append(self._emit_upstream_lineage(
            downstream_urn=self.make_ml_urn("feature_engineering"),
            upstream_urns=[
                self.make_mongodb_urn("cryptomarket.ohlcv"),
                self.make_mongodb_urn("cryptomarket.indicators"),
            ],
        ))

        # ML feature_engineering → xgboost_predictor
        events.append(self._emit_upstream_lineage(
            downstream_urn=self.make_ml_urn("xgboost_predictor"),
            upstream_urns=[self.make_ml_urn("feature_engineering")],
        ))

        # ML feature_engineering → anomaly_detector
        events.append(self._emit_upstream_lineage(
            downstream_urn=self.make_ml_urn("anomaly_detector"),
            upstream_urns=[self.make_ml_urn("feature_engineering")],
        ))

        return events

    def emit_dataset_descriptions(self) -> List[MetadataChangeProposalWrapper]:
        """Emits human-readable descriptions for all datasets in the lineage."""
        logger.info("[Lineage] Emitting dataset descriptions")
        events = []
        ts = datetime.now(timezone.utc).isoformat()

        descriptions = {
            # Kafka
            self.make_kafka_urn("raw_trades"):
                "Real-time trade events from Binance WebSocket (BTCUSDT, ETHUSDT, BNBUSDT)",
            self.make_kafka_urn("raw_klines"):
                "1-minute kline/candlestick data from Binance WebSocket",
            self.make_kafka_urn("processed_data"):
                "Processed and enriched OHLCV data after Spark ETL",
            self.make_kafka_urn("anomalies"):
                "Detected market anomalies (flash crashes, volume spikes)",
            # Spark
            self.make_spark_urn("trades_cleaning"):
                "Spark ETL: validates, cleans, and enriches raw trade events",
            self.make_spark_urn("ohlcv_processing"):
                "Spark ETL: aggregates trades to OHLCV candles, calculates price changes",
            self.make_spark_urn("indicators_calc"):
                "Spark ETL: calculates RSI, MACD, Bollinger, SMA, EMA indicators",
            self.make_spark_urn("metrics_aggregation"):
                "Spark ETL: aggregates daily/hourly market metrics",
            # MongoDB
            self.make_mongodb_urn("cryptomarket.raw_trades"):
                "Raw trade events stored after cleaning (symbol, price, qty, timestamp)",
            self.make_mongodb_urn("cryptomarket.ohlcv"):
                "OHLCV candles with price change and volume metrics",
            self.make_mongodb_urn("cryptomarket.indicators"):
                "Technical indicators: RSI, MACD, Bollinger Bands, SMA, EMA",
            self.make_mongodb_urn("cryptomarket.anomalies"):
                "Detected anomalies with scores and labels",
            self.make_mongodb_urn("cryptomarket.predictions"):
                "ML model predictions: direction, confidence, model type",
            # ML
            self.make_ml_urn("feature_engineering"):
                "Feature matrix: price returns, volatility, volume ratios, lag/rolling features",
            self.make_ml_urn("xgboost_predictor"):
                "XGBoost classifier for short-term price direction prediction (UP/DOWN)",
            self.make_ml_urn("anomaly_detector"):
                "Isolation Forest for unsupervised market anomaly detection",
        }

        for urn, desc in descriptions.items():
            events.append(self._emit_dataset_properties(
                urn=urn,
                description=desc,
                custom_properties={"pipeline": "crypto-mlops", "updated_at": ts},
            ))

        return events

    # ── Main Orchestrator ─────────────────────────────────────────

    def emit_all(self, dry_run: bool = False) -> int:
        """
        Emits the complete lineage graph to DataHub.

        Args:
            dry_run: If True, builds events but does not connect to DataHub

        Returns:
            Number of events emitted
        """
        if not DATAHUB_AVAILABLE:
            raise ImportError(
                "acryl-datahub is not installed. "
                "Install it with: pip install acryl-datahub"
            )

        self._events_emitted = 0

        if not dry_run:
            logger.info(f"[Lineage] Connecting to DataHub GMS: {self.config.datahub.gms_server}")
            self.emitter = DatahubRestEmitter(
                gms_server=self.config.datahub.gms_server,
                extra_headers={},
            )
            self.emitter.test_connection()
            logger.info("[Lineage] ✅ Connected to DataHub GMS")
        else:
            logger.info("[Lineage] Dry-run mode — no events will be sent")
            self.emitter = None

        # Emit lineage edges
        self.emit_kafka_to_spark()
        self.emit_spark_to_mongodb()
        self.emit_mongodb_to_ml()

        # Emit dataset descriptions
        self.emit_dataset_descriptions()

        logger.info(f"[Lineage] ✅ Emitted {self._events_emitted} events to DataHub")
        return self._events_emitted


# ── CLI Entry Point ────────────────────────────────────────────

def main():
    """CLI entry point for lineage emission."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    import argparse
    parser = argparse.ArgumentParser(description="Emit data lineage to DataHub")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build lineage events without sending to DataHub",
    )
    parser.add_argument(
        "--gms-url",
        type=str,
        default=None,
        help="DataHub GMS URL (default: from env or http://localhost:8082)",
    )
    args = parser.parse_args()

    config = lineage_config
    if args.gms_url:
        config.datahub.gms_server = args.gms_url

    emitter = DataLineageEmitter(config=config)

    try:
        count = emitter.emit_all(dry_run=args.dry_run)
        logger.info(f"Done. Total events: {count}")
    except Exception as e:
        logger.error(f"Lineage emission failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

"""
DAG Airflow — Backfill Historique Crypto (P1)
==============================================
DAG déclenché manuellement pour combler les trous de données historiques.
Paramètres configurables via Airflow UI (Trigger DAG w/ Config):
  - start_date: date de début (format: YYYY-MM-DD)
  - end_date: date de fin (format: YYYY-MM-DD)
  - symbols: liste de paires (défaut: BTCUSDT,ETHUSDT,BNBUSDT)
  - interval: intervalle klines (défaut: 1m)

Flux:
  detect_gaps → backfill_from_binance → trigger_spark_etl → validate_backfill → log_backfill_report

Schedule: None (déclenchement manuel uniquement)
"""

from datetime import datetime, timedelta, timezone
import json

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models.param import Param

# ─────────────────────── CONFIG ──────────────────────────────────────────

KAFKA_BROKER = "kafka:9092"
MONGODB_URI = "mongodb://datamlops:datamlops123@mongodb:27017/cryptomarket?authSource=admin"
TRADING_PAIRS = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]

default_args = {
    "owner": "P1-Chef-de-Projet",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}


# ─────────────────────── TASK FUNCTIONS ──────────────────────────────────


def detect_gaps(**context):
    """
    Tâche 1: Analyse MongoDB pour détecter les trous de données OHLCV.
    Compare les timestamps attendus vs existants sur la période demandée.
    """
    import logging
    from datetime import datetime, timezone, timedelta
    from pymongo import MongoClient

    logger = logging.getLogger(__name__)

    # Récupérer les paramètres du DAG
    params = context["params"]
    start_str = params.get("start_date", (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d"))
    end_str = params.get("end_date", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    symbols = params.get("symbols", "BTCUSDT,ETHUSDT,BNBUSDT").split(",")
    interval = params.get("interval", "1m")

    start_dt = datetime.strptime(start_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt = datetime.strptime(end_str, "%Y-%m-%d").replace(tzinfo=timezone.utc, hour=23, minute=59)

    logger.info(f"Détection gaps: {start_str} → {end_str} | Symboles: {symbols} | Intervalle: {interval}")

    client = MongoClient(MONGODB_URI)
    db = client["cryptomarket"]
    ohlcv = db["ohlcv"]

    # Calculer le nombre attendu de bougies (1 par minute pour 1m)
    interval_map = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}
    interval_seconds = interval_map.get(interval, 60)
    total_seconds = (end_dt - start_dt).total_seconds()
    expected_per_symbol = int(total_seconds / interval_seconds)

    gaps_report = {}
    for symbol in symbols:
        # Compter les bougies existantes dans la période
        existing = ohlcv.count_documents(
            {"symbol": symbol.strip(), "interval": interval, "timestamp": {"$gte": start_dt, "$lte": end_dt}}
        )

        missing = max(0, expected_per_symbol - existing)
        coverage_pct = round((existing / expected_per_symbol * 100), 1) if expected_per_symbol > 0 else 0

        gaps_report[symbol.strip()] = {
            "expected": expected_per_symbol,
            "existing": existing,
            "missing": missing,
            "coverage_pct": coverage_pct,
        }
        logger.info(f"  {symbol}: {existing}/{expected_per_symbol} ({coverage_pct}%) — {missing} manquantes")

    client.close()

    context["ti"].xcom_push(key="gaps_report", value=gaps_report)
    context["ti"].xcom_push(
        key="backfill_params",
        value={
            "start_date": start_str,
            "end_date": end_str,
            "symbols": symbols,
            "interval": interval,
        },
    )
    return gaps_report


def backfill_from_binance(**context):
    """
    Tâche 2: Collecte les données manquantes via Binance REST API → Kafka.
    Utilise les paramètres start_date/end_date du DAG.
    """
    import requests
    import time
    import logging
    from datetime import datetime, timezone
    from kafka import KafkaProducer

    logger = logging.getLogger(__name__)
    ti = context["ti"]

    backfill_params = ti.xcom_pull(task_ids="detect_gaps", key="backfill_params")
    start_str = backfill_params["start_date"]
    end_str = backfill_params["end_date"]
    symbols = backfill_params["symbols"]
    interval = backfill_params["interval"]

    start_dt = datetime.strptime(start_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt = datetime.strptime(end_str, "%Y-%m-%d").replace(tzinfo=timezone.utc, hour=23, minute=59)
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
    )

    total_klines = 0
    stats = {}

    for symbol in symbols:
        symbol = symbol.strip()
        logger.info(f"Backfill {symbol}: {start_str} → {end_str}")
        symbol_count = 0
        current_start = start_ms

        while current_start < end_ms:
            try:
                url = "https://api.binance.com/api/v3/klines"
                params = {
                    "symbol": symbol,
                    "interval": interval,
                    "startTime": current_start,
                    "endTime": end_ms,
                    "limit": 1000,
                }
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                klines = response.json()

                if not klines:
                    break

                for kline in klines:
                    message = {
                        "symbol": symbol,
                        "interval": interval,
                        "open_time": kline[0],
                        "close_time": kline[6],
                        "open": float(kline[1]),
                        "high": float(kline[2]),
                        "low": float(kline[3]),
                        "close": float(kline[4]),
                        "volume": float(kline[5]),
                        "quote_volume": float(kline[7]),
                        "trades_count": int(kline[8]),
                    }
                    producer.send("raw_klines", key=symbol.lower(), value=message)
                    symbol_count += 1

                current_start = klines[-1][6] + 1
                time.sleep(0.2)

            except requests.exceptions.RequestException as e:
                logger.error(f"Erreur Binance pour {symbol}: {e}")
                time.sleep(5)
                continue

        producer.flush()
        stats[symbol] = symbol_count
        total_klines += symbol_count
        logger.info(f"  {symbol}: {symbol_count} klines")

    producer.close()
    logger.info(f"Backfill total: {total_klines} klines")

    context["ti"].xcom_push(key="backfill_stats", value=stats)
    context["ti"].xcom_push(key="backfill_total", value=total_klines)
    return stats


def validate_backfill(**context):
    """
    Tâche 4: Vérifie que le backfill a amélioré la couverture.
    Compare les gaps avant/après.
    """
    import logging

    logger = logging.getLogger(__name__)
    ti = context["ti"]

    gaps_before = ti.xcom_pull(task_ids="detect_gaps", key="gaps_report") or {}
    backfill_stats = ti.xcom_pull(task_ids="backfill_from_binance", key="backfill_stats") or {}

    logger.info("Validation du backfill:")
    logger.info(f"  Klines collectées: {backfill_stats}")

    validation = {}
    for symbol, before in gaps_before.items():
        collected = backfill_stats.get(symbol, 0)
        validation[symbol] = {
            "avant_coverage": before["coverage_pct"],
            "klines_collectées": collected,
            "manquantes_avant": before["missing"],
        }
        logger.info(
            f"  {symbol}: couverture avant={before['coverage_pct']}%, "
            f"collectées={collected}, manquantes={before['missing']}"
        )

    context["ti"].xcom_push(key="validation", value=validation)
    return validation


def log_backfill_report(**context):
    """
    Tâche 5: Rapport final du backfill.
    """
    import logging

    logger = logging.getLogger(__name__)
    ti = context["ti"]

    gaps = ti.xcom_pull(task_ids="detect_gaps", key="gaps_report") or {}
    stats = ti.xcom_pull(task_ids="backfill_from_binance", key="backfill_stats") or {}
    total = ti.xcom_pull(task_ids="backfill_from_binance", key="backfill_total") or 0
    params = ti.xcom_pull(task_ids="detect_gaps", key="backfill_params") or {}

    report = [
        "=" * 60,
        "📦 RAPPORT BACKFILL HISTORIQUE",
        "=" * 60,
        f"Période: {params.get('start_date')} → {params.get('end_date')}",
        f"Intervalle: {params.get('interval', '1m')}",
        f"Total klines collectées: {total}",
        "",
    ]
    for symbol in gaps:
        g = gaps[symbol]
        s = stats.get(symbol, 0)
        report.append(
            f"  {symbol}: {g['existing']}→{g['existing']+s} / {g['expected']} ({g['coverage_pct']}% → ajout {s})"
        )

    report.extend(["", "=" * 60])
    logger.info("\n".join(report))
    return "\n".join(report)


# ─────────────────────── DAG BACKFILL ────────────────────────────────────

with DAG(
    dag_id="crypto_backfill",
    default_args=default_args,
    description="Backfill historique: détection gaps → collecte Binance → ETL Spark → validation",
    schedule_interval=None,  # Déclenchement manuel uniquement
    catchup=False,
    max_active_runs=1,
    tags=["crypto", "backfill", "P1", "manual"],
    params={
        "start_date": Param(
            default=(datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d"),
            type="string",
            description="Date de début (YYYY-MM-DD)",
        ),
        "end_date": Param(
            default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            type="string",
            description="Date de fin (YYYY-MM-DD)",
        ),
        "symbols": Param(
            default="BTCUSDT,ETHUSDT,BNBUSDT",
            type="string",
            description="Paires de trading (séparées par des virgules)",
        ),
        "interval": Param(
            default="1m",
            type="string",
            enum=["1m", "5m", "15m", "1h", "4h", "1d"],
            description="Intervalle des klines",
        ),
    },
) as dag_backfill:

    t_gaps = PythonOperator(
        task_id="detect_gaps",
        python_callable=detect_gaps,
        provide_context=True,
    )

    t_backfill = PythonOperator(
        task_id="backfill_from_binance",
        python_callable=backfill_from_binance,
        provide_context=True,
        execution_timeout=timedelta(hours=2),
    )

    t_spark = BashOperator(
        task_id="trigger_spark_etl",
        bash_command=(
            "docker exec spark-master "
            "/opt/spark/bin/spark-submit "
            "--master local[*] "
            "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0 "
            "--conf spark.mongodb.write.connection.uri=" + MONGODB_URI + " "
            "--conf spark.jars.ivy=/tmp/.ivy2 "
            "/opt/spark/work-dir/src/processing/main_pipeline.py "
            "--mode batch"
        ),
        execution_timeout=timedelta(hours=1),
    )

    t_validate = PythonOperator(
        task_id="validate_backfill",
        python_callable=validate_backfill,
        provide_context=True,
    )

    t_report = PythonOperator(
        task_id="log_backfill_report",
        python_callable=log_backfill_report,
        provide_context=True,
    )

    t_gaps >> t_backfill >> t_spark >> t_validate >> t_report

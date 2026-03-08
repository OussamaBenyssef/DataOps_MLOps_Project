"""
DAG Airflow — Pipeline Quotidien Crypto (P1)
=============================================
Orchestre le pipeline quotidien de données crypto :
  1. Vérification des services (Kafka, MongoDB, Spark)
  2. Collecte historique OHLCV via Binance REST API → Kafka
  3. ETL batch Spark (cleaning + indicateurs + MongoDB)
  4. Calcul métriques qualité
  5. Résumé pipeline
  6. Ingestion métadonnées + lineage → DataHub

Schedule: @daily
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# ─────────────────────── CONFIG ──────────────────────────────────────────

KAFKA_BROKER = "kafka:9092"
MONGODB_URI = "mongodb://datamlops:datamlops123@mongodb:27017/cryptomarket?authSource=admin"
SPARK_MASTER = "spark://spark-master:7077"
TRADING_PAIRS = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
KLINE_INTERVAL = "1m"

default_args = {
    "owner": "P1-Chef-de-Projet",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}


# ─────────────────────── TASK FUNCTIONS ──────────────────────────────────

def check_services(**context):
    """
    Tâche 1: Vérifie la disponibilité de Kafka, MongoDB et Spark Master.
    Échoue si un service critique est inaccessible.
    """
    import socket
    import logging

    logger = logging.getLogger(__name__)
    services = {
        "Kafka": ("kafka", 9092),
        "MongoDB": ("mongodb", 27017),
        "Spark Master": ("spark-master", 7077),
    }

    results = {}
    for name, (host, port) in services.items():
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            sock.connect((host, port))
            results[name] = "✅ UP"
            logger.info(f"{name} ({host}:{port}) — UP")
        except (socket.timeout, ConnectionRefusedError, OSError) as e:
            results[name] = f"❌ DOWN ({e})"
            logger.error(f"{name} ({host}:{port}) — DOWN: {e}")
        finally:
            if sock:
                sock.close()

    # Vérifier que tous les services sont UP
    failed = [name for name, status in results.items() if "DOWN" in status]
    if failed:
        raise RuntimeError(
            f"Services indisponibles : {', '.join(failed)}. "
            f"Résultats complets : {results}"
        )

    logger.info(f"Tous les services sont opérationnels : {results}")
    context["ti"].xcom_push(key="services_status", value=results)
    return results


def collect_historical_data(**context):
    """
    Tâche 2: Collecte les données OHLCV historiques via l'API REST Binance
    et les publie dans Kafka (topic: raw_klines).
    Couvre les dernières 24h.
    """
    import requests
    import json
    import time
    import logging
    from datetime import datetime, timezone, timedelta
    from kafka import KafkaProducer

    logger = logging.getLogger(__name__)

    # Configurer le producer Kafka
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
    )

    # Période: dernières 24h
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=24)
    start_ms = int(start_time.timestamp() * 1000)
    end_ms = int(end_time.timestamp() * 1000)

    total_klines = 0
    stats = {}

    for symbol in TRADING_PAIRS:
        logger.info(f"Collecte {symbol} — {start_time.strftime('%Y-%m-%d %H:%M')} → {end_time.strftime('%Y-%m-%d %H:%M')}")
        symbol_count = 0
        current_start = start_ms

        while current_start < end_ms:
            try:
                # Appel API REST Binance
                url = "https://api.binance.com/api/v3/klines"
                params = {
                    "symbol": symbol,
                    "interval": KLINE_INTERVAL,
                    "startTime": current_start,
                    "endTime": end_ms,
                    "limit": 1000,
                }
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                klines = response.json()

                if not klines:
                    break

                # Publier chaque kline dans Kafka (format plat RAW_KLINES_SCHEMA)
                for kline in klines:
                    message = {
                        "symbol": symbol,
                        "interval": KLINE_INTERVAL,
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

                    producer.send(
                        "raw_klines",
                        key=symbol.lower(),
                        value=message,
                    )
                    symbol_count += 1

                # Avancer après la dernière kline récupérée
                current_start = klines[-1][6] + 1  # close_time + 1ms
                time.sleep(0.2)  # Rate limit

            except requests.exceptions.RequestException as e:
                logger.error(f"Erreur API Binance pour {symbol}: {e}")
                time.sleep(5)
                continue

        producer.flush()
        stats[symbol] = symbol_count
        total_klines += symbol_count
        logger.info(f"  {symbol}: {symbol_count} klines publiées")

    producer.close()
    logger.info(f"Total: {total_klines} klines publiées dans Kafka")

    context["ti"].xcom_push(key="collection_stats", value=stats)
    context["ti"].xcom_push(key="total_klines", value=total_klines)
    return stats


def compute_quality_metrics(**context):
    """
    Tâche 4: Calcule les métriques de qualité des données dans MongoDB.
    Vérifie le nombre de documents, les taux de null, et la couverture.
    """
    import logging
    from datetime import datetime, timezone, timedelta
    from pymongo import MongoClient

    logger = logging.getLogger(__name__)

    client = MongoClient(MONGODB_URI)
    db = client["cryptomarket"]

    # Période d'analyse : dernières 24h
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)

    metrics = {}

    # 1. Comptage par collection
    collections = ["raw_trades", "ohlcv", "indicators", "anomalies", "predictions"]
    for coll_name in collections:
        coll = db[coll_name]
        total = coll.count_documents({})
        metrics[f"{coll_name}_total"] = total
        logger.info(f"  {coll_name}: {total} documents au total")

    # 2. OHLCV — couverture par symbole (dernières 24h)
    ohlcv_coll = db["ohlcv"]
    for symbol in TRADING_PAIRS:
        count = ohlcv_coll.count_documents({"symbol": symbol})
        metrics[f"ohlcv_{symbol}"] = count
        logger.info(f"  OHLCV {symbol}: {count} bougies")

    # 3. Indicateurs — vérification couverture
    indicators_coll = db["indicators"]
    for symbol in TRADING_PAIRS:
        count = indicators_coll.count_documents({"symbol": symbol})
        metrics[f"indicators_{symbol}"] = count
        logger.info(f"  Indicators {symbol}: {count} enregistrements")

    # 4. Vérification nulls dans OHLCV (échantillon)
    sample = list(ohlcv_coll.find().limit(100))
    if sample:
        null_counts = {}
        required_fields = ["open", "high", "low", "close", "volume"]
        for field in required_fields:
            nulls = sum(1 for doc in sample if doc.get(field) is None)
            null_counts[field] = nulls
        metrics["ohlcv_null_sample"] = null_counts
        null_rate = sum(null_counts.values()) / (len(sample) * len(required_fields)) * 100
        metrics["ohlcv_null_rate_pct"] = round(null_rate, 2)
        logger.info(f"  OHLCV null rate: {null_rate:.2f}%")
    else:
        metrics["ohlcv_null_rate_pct"] = 0
        logger.warning("  Aucune donnée OHLCV trouvée pour vérification nulls")

    client.close()

    logger.info(f"Métriques qualité: {json.dumps(metrics, indent=2, default=str)}")
    context["ti"].xcom_push(key="quality_metrics", value=metrics)
    return metrics


def log_pipeline_summary(**context):
    """
    Tâche 5: Résumé final du pipeline quotidien.
    Agrège les résultats des tâches précédentes.
    """
    import logging

    logger = logging.getLogger(__name__)
    ti = context["ti"]

    # Récupérer les données des tâches précédentes
    services = ti.xcom_pull(task_ids="check_services", key="services_status") or {}
    collection_stats = ti.xcom_pull(task_ids="collect_historical_data", key="collection_stats") or {}
    total_klines = ti.xcom_pull(task_ids="collect_historical_data", key="total_klines") or 0
    quality_metrics = ti.xcom_pull(task_ids="compute_quality_metrics", key="quality_metrics") or {}

    summary = []
    summary.append("=" * 60)
    summary.append("📊 RÉSUMÉ PIPELINE QUOTIDIEN CRYPTO")
    summary.append("=" * 60)
    summary.append(f"Date d'exécution: {context['execution_date']}")
    summary.append("")

    # Services
    summary.append("🔌 Services:")
    for name, status in services.items():
        summary.append(f"  {name}: {status}")
    summary.append("")

    # Collecte
    summary.append("📥 Collecte historique:")
    summary.append(f"  Total klines: {total_klines}")
    for symbol, count in collection_stats.items():
        summary.append(f"  {symbol}: {count} klines")
    summary.append("")

    # Qualité
    summary.append("✅ Métriques qualité:")
    for key, value in quality_metrics.items():
        if not key.startswith("ohlcv_null_sample"):
            summary.append(f"  {key}: {value}")
    summary.append("")
    summary.append("=" * 60)

    full_summary = "\n".join(summary)
    logger.info(f"\n{full_summary}")

    return full_summary


# ─────────────────────── DAG DEFINITION ──────────────────────────────────

# Import json ici pour compute_quality_metrics
import json

with DAG(
    dag_id="crypto_daily_pipeline",
    default_args=default_args,
    description="Pipeline quotidien: collecte historique Binance → ETL Spark → métriques qualité → DataHub",
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["crypto", "pipeline", "P1", "daily", "datahub"],
) as dag:

    # Tâche 1: Vérification des services
    t_check = PythonOperator(
        task_id="check_services",
        python_callable=check_services,
        provide_context=True,
    )

    # Tâche 2: Collecte données historiques Binance → Kafka
    t_collect = PythonOperator(
        task_id="collect_historical_data",
        python_callable=collect_historical_data,
        provide_context=True,
        execution_timeout=timedelta(minutes=30),
    )

    # Tâche 3: ETL Spark batch (Kafka → cleaning → indicators → MongoDB)
    t_spark_etl = BashOperator(
        task_id="trigger_spark_etl",
        bash_command=(
            "docker exec spark-master "
            "/opt/spark/bin/spark-submit "
            "--master local[*] "
            "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.mongodb.spark:mongo-spark-connector_2.12:10.2.1 "
            "--conf spark.mongodb.write.connection.uri={{ params.mongodb_uri }} "
            "--conf spark.jars.ivy=/tmp/.ivy2 "
            "/opt/spark/work-dir/src/processing/main_pipeline.py "
            "--mode batch"
        ),
        params={"mongodb_uri": MONGODB_URI},
        execution_timeout=timedelta(minutes=45),
    )

    # Tâche 4: Métriques qualité
    t_metrics = PythonOperator(
        task_id="compute_quality_metrics",
        python_callable=compute_quality_metrics,
        provide_context=True,
    )

    # Tâche 5: Résumé final
    t_summary = PythonOperator(
        task_id="log_pipeline_summary",
        python_callable=log_pipeline_summary,
        provide_context=True,
    )

    # Tâche 6a: Ingestion métadonnées Kafka → DataHub
    t_datahub_kafka = BashOperator(
        task_id="ingest_datahub_kafka",
        bash_command=(
            "docker exec datahub-actions "
            "datahub ingest -c /etc/datahub/recipes/kafka_recipe.yml"
        ),
        execution_timeout=timedelta(minutes=10),
    )

    # Tâche 6b: Ingestion métadonnées MongoDB → DataHub
    t_datahub_mongodb = BashOperator(
        task_id="ingest_datahub_mongodb",
        bash_command=(
            "docker exec datahub-actions "
            "datahub ingest -c /etc/datahub/recipes/mongodb_recipe.yml"
        ),
        execution_timeout=timedelta(minutes=10),
    )

    # Tâche 6c: Émission du lineage données → DataHub
    t_datahub_lineage = BashOperator(
        task_id="emit_datahub_lineage",
        bash_command=(
            "docker exec "
            "-e DATAHUB_GMS_URL=http://datahub-gms:8080 "
            "datahub-actions python3 /tmp/emit_all.py"
        ),
        execution_timeout=timedelta(minutes=30),
    )

    # Définition du flux
    #   Pipeline principal: check → collect → spark_etl → metrics → summary
    #   Puis DataHub en parallèle: kafka + mongodb → lineage
    t_check >> t_collect >> t_spark_etl >> t_metrics >> t_summary
    t_summary >> [t_datahub_kafka, t_datahub_mongodb] >> t_datahub_lineage

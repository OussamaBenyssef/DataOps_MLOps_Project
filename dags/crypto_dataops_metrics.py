"""
DAG Airflow — Collecte Métriques DataOps (P5)
===============================================
Collecte périodique des métriques opérationnelles du pipeline :
  1. Santé des services (Kafka, MongoDB, Spark, MLflow, Airflow, Metabase, FastAPI)
  2. Volume de données par collection MongoDB
  3. Fraîcheur des données (dernière insertion)
  4. Couverture par symbole (BTCUSDT, ETHUSDT, BNBUSDT)
  5. Taux de nulls sur champs critiques OHLCV
  6. Métriques MLflow (expériences, runs, modèles)
  7. Persistance dans MongoDB (collection dataops_metrics, TTL 30 jours)

Les métriques sont visualisées dans le dashboard Metabase "DataOps Monitoring".

Schedule: toutes les 10 minutes
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# ─────────────────────── CONFIG ──────────────────────────────────────────

MONGODB_URI = "mongodb://datamlops:datamlops123@mongodb:27017/cryptomarket?authSource=admin"
MLFLOW_URL = "http://mlflow:5000"
TRADING_PAIRS = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
OHLCV_REQUIRED_FIELDS = ["open", "high", "low", "close", "volume"]
METRICS_TTL_DAYS = 30

# Services à monitorer (depuis le réseau Docker interne)
SERVICES = {
    "kafka": {"host": "kafka", "port": 9092, "type": "tcp"},
    "mongodb": {"host": "mongodb", "port": 27017, "type": "tcp"},
    "spark": {"url": "http://spark-master:8080", "type": "http"},
    "mlflow": {"url": "http://mlflow:5000", "type": "http"},
    "airflow": {"url": "http://airflow-webserver:8080/health", "type": "http"},
    "metabase": {"url": "http://metabase:3000/api/health", "type": "http"},
    "fastapi": {"url": "http://fastapi:8000/health", "type": "http"},
}

default_args = {
    "owner": "P5-Data-Analyst",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2024, 1, 1),
}


# ─────────────────────── TASK FUNCTIONS ──────────────────────────────────


def collect_services_health(**context):
    """
    Tâche 1: Vérifie la santé de tous les services du pipeline.
    Teste chaque service via TCP ou HTTP.
    """
    import socket
    import logging
    import requests as req

    logger = logging.getLogger(__name__)
    logger.info("🔌 Vérification des services...")

    results = {}
    for name, cfg in SERVICES.items():
        try:
            if cfg["type"] == "tcp":
                sock = socket.create_connection((cfg["host"], cfg["port"]), timeout=5)
                sock.close()
                alive = True
            else:
                resp = req.get(cfg["url"], timeout=5)
                alive = resp.status_code < 400
        except Exception:
            alive = False

        status = "up" if alive else "down"
        results[name] = status
        icon = "✅" if alive else "❌"
        logger.info(f"  {icon} {name}: {status}")

    services_up = sum(1 for s in results.values() if s == "up")
    health_score = round(services_up / len(results) * 100)
    logger.info(f"💚 Score de santé: {health_score}% ({services_up}/{len(results)})")

    context["ti"].xcom_push(key="services", value=results)
    context["ti"].xcom_push(key="services_up", value=services_up)
    context["ti"].xcom_push(key="health_score", value=health_score)
    return results


def collect_data_metrics(**context):
    """
    Tâche 2: Collecte les métriques de données MongoDB.
    - Volume par collection
    - Fraîcheur (timestamp le plus récent)
    - Couverture par symbole
    - Taux de nulls OHLCV
    """
    import logging
    from datetime import datetime, timezone
    from pymongo import MongoClient, DESCENDING

    logger = logging.getLogger(__name__)
    client = MongoClient(MONGODB_URI)
    db = client["cryptomarket"]
    now = datetime.now(timezone.utc)

    # ── Volume ──
    logger.info("📦 Volume des données...")
    collections = ["raw_trades", "ohlcv", "indicators", "anomalies", "predictions"]
    volume = {}
    total = 0
    for coll_name in collections:
        count = db[coll_name].count_documents({})
        volume[coll_name] = count
        total += count
        logger.info(f"  {coll_name}: {count:,}")
    volume["total"] = total

    # ── Fraîcheur ──
    logger.info("⏱️ Fraîcheur des données...")
    freshness_collections = {
        "ohlcv": "processed_at",
        "indicators": "processed_at",
        "anomalies": "processed_at",
    }
    freshness = {}
    for coll_name, ts_field in freshness_collections.items():
        doc = db[coll_name].find_one(
            {ts_field: {"$exists": True}},
            sort=[(ts_field, DESCENDING)],
            projection={ts_field: 1, "_id": 0},
        )
        if doc and ts_field in doc:
            latest = doc[ts_field]
            if isinstance(latest, datetime):
                age = (now - latest.replace(tzinfo=timezone.utc)).total_seconds() / 60
            else:
                age = -1
            freshness[coll_name] = {
                "latest": latest.isoformat() if isinstance(latest, datetime) else str(latest),
                "age_minutes": round(age, 1),
            }
            logger.info(f"  {coll_name}: âge {age:.0f} min")
        else:
            freshness[coll_name] = {"latest": None, "age_minutes": -1}

    # ── Couverture par symbole ──
    logger.info("🎯 Couverture par symbole...")
    coverage = {}
    for symbol in TRADING_PAIRS:
        coverage[symbol] = {
            "ohlcv": db["ohlcv"].count_documents({"symbol": symbol}),
            "indicators": db["indicators"].count_documents({"symbol": symbol}),
            "anomalies": db["anomalies"].count_documents({"symbol": symbol}),
        }
        logger.info(f"  {symbol}: {coverage[symbol]}")

    # ── Nulls OHLCV ──
    logger.info("🧪 Taux de nulls OHLCV...")
    sample = list(db["ohlcv"].find().sort("processed_at", DESCENDING).limit(500))
    if sample:
        total_nulls = 0
        field_nulls = {}
        for field in OHLCV_REQUIRED_FIELDS:
            nulls = sum(1 for doc in sample if doc.get(field) is None)
            pct = round(nulls / len(sample) * 100, 2)
            field_nulls[field] = {"nulls": nulls, "pct": pct}
            total_nulls += nulls
        overall_pct = round(total_nulls / (len(sample) * len(OHLCV_REQUIRED_FIELDS)) * 100, 2)
        null_rates = {"overall_null_pct": overall_pct, "fields": field_nulls, "sample_size": len(sample)}
    else:
        null_rates = {"overall_null_pct": 0, "fields": {}, "sample_size": 0}
    logger.info(f"  Taux global: {null_rates['overall_null_pct']}%")

    client.close()

    context["ti"].xcom_push(key="data_volume", value=volume)
    context["ti"].xcom_push(key="data_freshness", value=freshness)
    context["ti"].xcom_push(key="symbol_coverage", value=coverage)
    context["ti"].xcom_push(key="null_rates", value=null_rates)


def collect_mlflow_metrics(**context):
    """
    Tâche 3: Collecte les métriques MLflow via l'API REST.
    Expériences, runs, modèles enregistrés, modèles en production.
    """
    import logging
    import requests as req

    logger = logging.getLogger(__name__)
    logger.info("🤖 Métriques MLflow...")

    metrics = {
        "experiments": 0,
        "total_runs": 0,
        "registered_models": 0,
        "production_models": 0,
        "available": False,
    }

    try:
        # Expériences
        resp = req.get(f"{MLFLOW_URL}/api/2.0/mlflow/experiments/search", params={"max_results": 100}, timeout=5)
        if resp.status_code == 200:
            experiments = resp.json().get("experiments", [])
            metrics["experiments"] = len(experiments)
            metrics["available"] = True

            # Comptage total des runs
            total_runs = 0
            for exp in experiments:
                resp_r = req.post(
                    f"{MLFLOW_URL}/api/2.0/mlflow/runs/search",
                    json={"experiment_ids": [exp["experiment_id"]], "max_results": 1000},
                    timeout=5,
                )
                if resp_r.status_code == 200:
                    total_runs += len(resp_r.json().get("runs", []))
            metrics["total_runs"] = total_runs

        # Modèles enregistrés
        resp = req.get(f"{MLFLOW_URL}/api/2.0/mlflow/registered-models/search", params={"max_results": 100}, timeout=5)
        if resp.status_code == 200:
            models = resp.json().get("registered_models", [])
            metrics["registered_models"] = len(models)
            prod = sum(
                1 for m in models for v in m.get("latest_versions", []) if v.get("current_stage") == "Production"
            )
            metrics["production_models"] = prod

        logger.info(
            f"  Expériences: {metrics['experiments']}, "
            f"Runs: {metrics['total_runs']}, "
            f"Modèles: {metrics['registered_models']} "
            f"(prod: {metrics['production_models']})"
        )

    except Exception as e:
        logger.warning(f"  ⚠️ MLflow inaccessible: {e}")

    context["ti"].xcom_push(key="mlflow", value=metrics)


def persist_metrics(**context):
    """
    Tâche 4: Agrège toutes les métriques et les persiste dans MongoDB.
    Collection: dataops_metrics (avec TTL 30 jours).
    """
    import logging
    from datetime import datetime, timezone, timedelta
    from pymongo import MongoClient, DESCENDING

    logger = logging.getLogger(__name__)
    ti = context["ti"]

    now = datetime.now(timezone.utc)

    # Récupérer les métriques des tâches précédentes
    services = ti.xcom_pull(task_ids="collect_services_health", key="services") or {}
    services_up = ti.xcom_pull(task_ids="collect_services_health", key="services_up") or 0
    health_score = ti.xcom_pull(task_ids="collect_services_health", key="health_score") or 0
    data_volume = ti.xcom_pull(task_ids="collect_data_metrics", key="data_volume") or {}
    data_freshness = ti.xcom_pull(task_ids="collect_data_metrics", key="data_freshness") or {}
    symbol_coverage = ti.xcom_pull(task_ids="collect_data_metrics", key="symbol_coverage") or {}
    null_rates = ti.xcom_pull(task_ids="collect_data_metrics", key="null_rates") or {}
    mlflow = ti.xcom_pull(task_ids="collect_mlflow_metrics", key="mlflow") or {}

    # Construire le document
    metrics_doc = {
        "timestamp": now,
        "health_score": health_score,
        "services": services,
        "services_up": services_up,
        "services_total": len(SERVICES),
        "data_volume": data_volume,
        "data_freshness": data_freshness,
        "symbol_coverage": symbol_coverage,
        "null_rates": null_rates,
        "mlflow": mlflow,
        "source": "airflow_dag",
        "expire_at": now + timedelta(days=METRICS_TTL_DAYS),
    }

    # Persister dans MongoDB
    client = MongoClient(MONGODB_URI)
    db = client["cryptomarket"]
    coll = db["dataops_metrics"]

    # Index TTL et timestamp (idempotent)
    existing_indexes = {idx["name"] for idx in coll.list_indexes()}
    if "expire_at_ttl" not in existing_indexes:
        coll.create_index("expire_at", expireAfterSeconds=0, name="expire_at_ttl")
        logger.info("🗂️ Index TTL créé")
    if "timestamp_desc" not in existing_indexes:
        coll.create_index([("timestamp", DESCENDING)], name="timestamp_desc")

    result = coll.insert_one(metrics_doc)
    client.close()

    logger.info(f"✅ Métriques persistées (id={result.inserted_id})")
    logger.info(
        f"💚 Santé: {health_score}% | 📦 Total docs: {data_volume.get('total', 0):,} | "
        f"🧪 Nulls: {null_rates.get('overall_null_pct', 0)}% | "
        f"🤖 MLflow: {mlflow.get('experiments', 0)} exp, "
        f"{mlflow.get('registered_models', 0)} modèles"
    )


# ─────────────────────── DAG DEFINITION ──────────────────────────────────

with DAG(
    dag_id="crypto_dataops_metrics",
    default_args=default_args,
    description="Collecte métriques DataOps toutes les 10 min → MongoDB → dashboard Metabase",
    schedule_interval="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["crypto", "dataops", "monitoring", "P5", "metrics"],
) as dag:

    # Tâche 1: Santé des services
    t_services = PythonOperator(
        task_id="collect_services_health",
        python_callable=collect_services_health,
        provide_context=True,
    )

    # Tâche 2: Métriques données (volume, fraîcheur, couverture, nulls)
    t_data = PythonOperator(
        task_id="collect_data_metrics",
        python_callable=collect_data_metrics,
        provide_context=True,
    )

    # Tâche 3: Métriques MLflow
    t_mlflow = PythonOperator(
        task_id="collect_mlflow_metrics",
        python_callable=collect_mlflow_metrics,
        provide_context=True,
    )

    # Tâche 4: Persistance dans MongoDB
    t_persist = PythonOperator(
        task_id="persist_metrics",
        python_callable=persist_metrics,
        provide_context=True,
    )

    # Flux: services + data + mlflow en parallèle → persistance
    [t_services, t_data, t_mlflow] >> t_persist

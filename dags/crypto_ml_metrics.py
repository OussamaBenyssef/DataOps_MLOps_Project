"""
DAG Airflow — ML Metrics Collector (P5)
=======================================
Collecte les métriques de performance des modèles ML depuis MLflow
et les persiste dans MongoDB pour visualisation dans Metabase.

Ce DAG complète le pipeline crypto_ml_training en capturant:
  - Les métriques par run (accuracy, precision, recall, F1)
  - Les modèles enregistrés et leurs stages
  - Les résultats de détection de drift

Schedule: @daily (décalé de 5min après crypto_ml_training)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "P5-Dashboards",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "start_date": datetime(2024, 1, 1),
}

# ─────────────────────── CONFIG ──────────────────────────────────────────

MLFLOW_URL = "http://mlflow:5000"
MONGODB_URI = "mongodb://datamlops:datamlops123@mongodb:27017/cryptomarket?authSource=admin"
EXPERIMENTS_TO_TRACK = ["crypto-price-prediction", "crypto-anomaly-detection"]
MAX_RUNS = 50


# ─────────────────────── TASK FUNCTIONS ──────────────────────────────────

def collect_ml_metrics(**context):
    """
    Collecte les métriques MLflow (runs + registry) et les persiste dans MongoDB.
    Tourne directement dans le worker Airflow (accès au réseau Docker interne).
    """
    import logging
    import requests
    from datetime import datetime, timezone, timedelta
    from pymongo import MongoClient, DESCENDING

    logger = logging.getLogger(__name__)

    # ── Helpers MLflow ──────────────────────────────────────────────────

    def mlflow_get(path, params=None):
        try:
            r = requests.get(
                f"{MLFLOW_URL}/api/2.0/mlflow/{path}",
                params=params or {},
                timeout=10,
            )
            return r.json() if r.status_code == 200 else {}
        except Exception as exc:
            logger.warning("MLflow GET %s: %s", path, exc)
            return {}

    def mlflow_post(path, payload):
        try:
            r = requests.post(
                f"{MLFLOW_URL}/api/2.0/mlflow/{path}",
                json=payload,
                timeout=10,
            )
            return r.json() if r.status_code == 200 else {}
        except Exception as exc:
            logger.warning("MLflow POST %s: %s", path, exc)
            return {}

    # ── Check MLflow ─────────────────────────────────────────────────────

    now = datetime.now(timezone.utc)
    all_runs = []
    registered_models = []

    try:
        resp = requests.get(f"{MLFLOW_URL}/health", timeout=5)
        mlflow_ok = resp.status_code < 400
    except Exception:
        mlflow_ok = False

    status_str = "OK" if mlflow_ok else "DOWN"
    logger.info("MLflow: %s", status_str)

    # ── Collect runs ─────────────────────────────────────────────────────

    if mlflow_ok:
        for exp_name in EXPERIMENTS_TO_TRACK:
            data = mlflow_get("experiments/get-by-name", {"experiment_name": exp_name})
            exp = data.get("experiment")
            if not exp:
                logger.warning("Experiment not found: %s", exp_name)
                continue
            exp_id = exp["experiment_id"]
            res = mlflow_post("runs/search", {
                "experiment_ids": [exp_id],
                "max_results": MAX_RUNS,
                "order_by": ["metrics.f1 DESC"],
            })
            for run in res.get("runs", []):
                info = run.get("info", {})
                # MLflow REST API returns metrics/params as list of {key, value} objects
                metrics_raw = run.get("data", {}).get("metrics", [])
                params_raw = run.get("data", {}).get("params", [])
                metrics = {m["key"]: m["value"] for m in (metrics_raw if isinstance(metrics_raw, list) else [])}
                params = {p["key"]: p["value"] for p in (params_raw if isinstance(params_raw, list) else [])}
                tags = {t["key"]: t["value"] for t in run.get("data", {}).get("tags", [])}
                start_ms = info.get("start_time", 0)
                all_runs.append({
                    "run_id": info.get("run_id"),
                    "run_name": info.get("run_name", ""),
                    "experiment_name": exp_name,
                    "model_type": tags.get("model_type", "unknown"),
                    "symbol": tags.get("symbol", ""),
                    "task": tags.get("task", ""),
                    "start_time": (
                        datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).isoformat()
                        if start_ms else None
                    ),
                    "accuracy": float(metrics.get("accuracy", 0) or 0),
                    "precision": float(metrics.get("precision", 0) or 0),
                    "recall": float(metrics.get("recall", 0) or 0),
                    "f1": float(metrics.get("f1", 0) or 0),
                    "n_features": int(params.get("n_features", 0) or 0),
                    "n_train_samples": int(params.get("n_train_samples", 0) or 0),
                })
            logger.info("  %s: %d runs", exp_name, len([r for r in all_runs if r["experiment_name"] == exp_name]))

        # ── Registered models ─────────────────────────────────────────────

        data = mlflow_get("registered-models/search", {"max_results": 50})
        for m in data.get("registered_models", []):
            for v in m.get("latest_versions", []):
                registered_models.append({
                    "model_name": m.get("name", ""),
                    "version": v.get("version", ""),
                    "stage": v.get("current_stage", ""),
                    "status": v.get("status", ""),
                    "run_id": v.get("run_id", ""),
                })

    # ── Health score ─────────────────────────────────────────────────────

    prod_models = [m for m in registered_models if m["stage"].lower() == "production"]
    best_f1 = max((r["f1"] for r in all_runs), default=0.0)
    health_score = min(len(prod_models) * 30, 60) + round(best_f1 * 40)

    # ── Persist to MongoDB ────────────────────────────────────────────────

    doc = {
        "timestamp": now,
        "mlflow_available": mlflow_ok,
        "health": {
            "score": health_score,
            "production_models": len(prod_models),
            "best_f1": round(best_f1, 4),
            "total_runs": len(all_runs),
            "registered_models": len(registered_models),
        },
        "all_runs": all_runs,
        "registered_models": registered_models,
        "drift_results": [],
        "expire_at": now + timedelta(days=30),
    }

    mongo_client = MongoClient(MONGODB_URI)
    db = mongo_client["cryptomarket"]
    coll = db["ml_metrics"]

    existing = [idx["name"] for idx in coll.list_indexes()]
    if "expire_at_ttl" not in existing:
        coll.create_index("expire_at", expireAfterSeconds=0, name="expire_at_ttl")
    if "timestamp_desc" not in existing:
        coll.create_index([("timestamp", DESCENDING)], name="timestamp_desc")

    result = coll.insert_one(doc)
    mongo_client.close()

    logger.info("Inserted: %s", result.inserted_id)
    logger.info(
        "Runs: %d | Models: %d | Health: %d/100",
        len(all_runs), len(registered_models), health_score,
    )

    # Push stats to XCom for summary task
    context["ti"].xcom_push(key="total_runs", value=len(all_runs))
    context["ti"].xcom_push(key="registered_models", value=len(registered_models))
    context["ti"].xcom_push(key="health_score", value=health_score)
    context["ti"].xcom_push(key="best_f1", value=round(best_f1, 4))

    return str(result.inserted_id)


def log_ml_metrics_summary(**context):
    """Résumé final de la collecte ML metrics."""
    import logging
    logger = logging.getLogger(__name__)

    ti = context["ti"]
    total_runs = ti.xcom_pull(task_ids="collect_ml_metrics", key="total_runs") or 0
    models = ti.xcom_pull(task_ids="collect_ml_metrics", key="registered_models") or 0
    score = ti.xcom_pull(task_ids="collect_ml_metrics", key="health_score") or 0
    best_f1 = ti.xcom_pull(task_ids="collect_ml_metrics", key="best_f1") or 0.0
    exec_date = context.get("execution_date", "N/A")

    logger.info("=" * 60)
    logger.info("🤖 ML METRICS — COLLECTE TERMINÉE")
    logger.info("=" * 60)
    logger.info("  Date d'exécution : %s", exec_date)
    logger.info("  📊 Runs collectés       : %d", total_runs)
    logger.info("  📦 Modèles en registry  : %d", models)
    logger.info("  💚 Score santé ML       : %d/100", score)
    logger.info("  🏆 Meilleur F1          : %.4f", best_f1)
    logger.info("  ✅ Persisté dans MongoDB (ml_metrics)")
    logger.info("  🌐 Dashboard Metabase   : http://localhost:3000/dashboard/3")
    logger.info("=" * 60)


# ─────────────────────── DAG DEFINITION ──────────────────────────────────

with DAG(
    dag_id="crypto_ml_metrics",
    default_args=default_args,
    description="Collecte métriques ML (MLflow → MongoDB) pour Metabase dashboard P5",
    schedule_interval="5 1 * * *",   # 01h05 UTC — après crypto_ml_training (01h00)
    catchup=False,
    max_active_runs=1,
    tags=["crypto", "ml", "metrics", "P5", "metabase", "mlflow"],
) as dag:

    # Tâche 1 : Collecter les métriques ML depuis MLflow → MongoDB
    t_collect = PythonOperator(
        task_id="collect_ml_metrics",
        python_callable=collect_ml_metrics,
        provide_context=True,
        execution_timeout=timedelta(minutes=10),
    )

    # Tâche 2 : Résumé
    t_summary = PythonOperator(
        task_id="log_ml_metrics_summary",
        python_callable=log_ml_metrics_summary,
        provide_context=True,
    )

    # Flow linéaire
    t_collect >> t_summary

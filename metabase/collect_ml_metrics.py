#!/usr/bin/env python3
"""
ML Metrics Collector — P5
==========================
Collecte les métriques de performance des modèles ML depuis l'API REST MLflow
et les persiste dans MongoDB (collection `ml_metrics`) pour visualisation Metabase.

Métriques collectées :
  - Tous les runs de chaque expérience (accuracy, precision, recall, f1)
  - Modèles enregistrés dans le registry (nom, version, alias/stage)
  - Meilleur run par expérience et par modèle
  - Résultat de détection de drift (si présent dans les tags MLflow)

Usage:
    python metabase/collect_ml_metrics.py
"""

import logging
import time
from datetime import datetime, timezone, timedelta

import requests
from pymongo import MongoClient, DESCENDING

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ─────────────────────── CONFIG ──────────────────────────────────────────

MLFLOW_URL = "http://localhost:5001"
MONGODB_URI = "mongodb://datamlops:datamlops123@localhost:27017/cryptomarket?authSource=admin"
MONGODB_DB = "cryptomarket"

EXPERIMENTS_TO_TRACK = [
    "crypto-price-prediction",
    "crypto-anomaly-detection",
]

METRICS_TTL_DAYS = 30
MAX_RUNS_PER_EXPERIMENT = 50


# ─────────────────────── MLflow REST helpers ─────────────────────────────

def _mlflow_get(path: str, params: dict = None) -> dict:
    """Appel GET sur l'API MLflow REST."""
    try:
        resp = requests.get(
            f"{MLFLOW_URL}/api/2.0/mlflow/{path}",
            params=params or {},
            timeout=10,
        )
        if resp.status_code == 200:
            return resp.json()
        logger.warning(f"MLflow GET {path} → {resp.status_code}: {resp.text[:200]}")
        return {}
    except Exception as e:
        logger.warning(f"MLflow GET {path} — erreur: {e}")
        return {}


def _mlflow_post(path: str, payload: dict) -> dict:
    """Appel POST sur l'API MLflow REST."""
    try:
        resp = requests.post(
            f"{MLFLOW_URL}/api/2.0/mlflow/{path}",
            json=payload,
            timeout=10,
        )
        if resp.status_code == 200:
            return resp.json()
        logger.warning(f"MLflow POST {path} → {resp.status_code}: {resp.text[:200]}")
        return {}
    except Exception as e:
        logger.warning(f"MLflow POST {path} — erreur: {e}")
        return {}


# ─────────────────────── COLLECTORS ──────────────────────────────────────

def check_mlflow_available() -> bool:
    """Vérifie que MLflow est accessible."""
    try:
        resp = requests.get(f"{MLFLOW_URL}/health", timeout=5)
        if resp.status_code < 400:
            logger.info("✅ MLflow accessible")
            return True
    except Exception:
        pass
    logger.warning("⚠️  MLflow inaccessible — métriques non collectées")
    return False


def get_experiment_id(exp_name: str) -> str | None:
    """Récupère l'ID d'une expérience par son nom."""
    data = _mlflow_get("experiments/get-by-name", {"experiment_name": exp_name})
    exp = data.get("experiment")
    if exp:
        return exp.get("experiment_id")
    return None


def collect_experiment_runs(exp_name: str, exp_id: str) -> list[dict]:
    """
    Collecte tous les runs d'une expérience avec leurs métriques et tags.
    Retourne une liste de dicts triés par f1 décroissant.
    """
    logger.info(f"  📊 Runs de '{exp_name}'...")
    payload = {
        "experiment_ids": [exp_id],
        "max_results": MAX_RUNS_PER_EXPERIMENT,
        "order_by": ["metrics.f1 DESC"],
    }
    data = _mlflow_post("runs/search", payload)
    runs_raw = data.get("runs", [])

    runs = []
    for r in runs_raw:
        info = r.get("info", {})
        metrics = r.get("data", {}).get("metrics", {})
        params = r.get("data", {}).get("params", {})
        tags = {t["key"]: t["value"] for t in r.get("data", {}).get("tags", [])}

        # Convertir les timestamps ms → datetime ISO
        start_ms = info.get("start_time", 0)
        end_ms = info.get("end_time", 0)
        start_dt = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).isoformat() if start_ms else None
        end_dt = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc).isoformat() if end_ms else None

        run_doc = {
            "run_id": info.get("run_id"),
            "run_name": info.get("run_name", ""),
            "experiment_name": exp_name,
            "experiment_id": exp_id,
            "status": info.get("status", ""),
            "model_type": tags.get("model_type", "unknown"),
            "symbol": tags.get("symbol", ""),
            "task": tags.get("task", ""),
            "start_time": start_dt,
            "end_time": end_dt,
            # Métriques
            "accuracy": float(metrics.get("accuracy", 0.0) or 0.0),
            "precision": float(metrics.get("precision", 0.0) or 0.0),
            "recall": float(metrics.get("recall", 0.0) or 0.0),
            "f1": float(metrics.get("f1", 0.0) or 0.0),
            # Params utiles
            "n_features": int(params.get("n_features", 0) or 0),
            "n_train_samples": int(params.get("n_train_samples", 0) or 0),
            "n_estimators": int(params.get("n_estimators", 0) or 0),
            "max_depth": int(params.get("max_depth", 0) or 0),
            "learning_rate": float(params.get("learning_rate", 0.0) or 0.0),
            "epochs": int(params.get("epochs", 0) or 0),
            "sequence_length": int(params.get("sequence_length", 0) or 0),
            "contamination": float(params.get("contamination", 0.0) or 0.0),
        }
        runs.append(run_doc)

    logger.info(f"    → {len(runs)} runs collectés")
    return runs


def collect_registered_models() -> list[dict]:
    """
    Collecte les modèles enregistrés dans le registry MLflow.
    Retourne une liste de dicts avec nom, version, aliases/stage, et métriques.
    """
    logger.info("📋 Modèles du registry MLflow...")
    data = _mlflow_get("registered-models/search", {"max_results": 50})
    models_raw = data.get("registered_models", [])

    models = []
    for m in models_raw:
        name = m.get("name", "")
        versions = m.get("latest_versions", [])
        for v in versions:
            aliases = v.get("aliases", [])
            # Le stage peut être stocké dans current_stage (ancien) ou aliases (nouveau)
            stage = v.get("current_stage", "")
            if not stage and aliases:
                stage = aliases[0] if aliases else "none"

            model_doc = {
                "model_name": name,
                "version": v.get("version", ""),
                "stage": stage,
                "aliases": aliases,
                "status": v.get("status", ""),
                "run_id": v.get("run_id", ""),
                "source": v.get("source", ""),
                "created_at": v.get("creation_timestamp", 0),
                "updated_at": v.get("last_updated_timestamp", 0),
            }
            models.append(model_doc)

    logger.info(f"  → {len(models)} versions de modèles collectées")
    return models


def collect_best_runs_per_model_type(all_runs: list[dict]) -> dict:
    """
    Calcule le meilleur run par type de modèle (basé sur F1).
    Return: dict { model_type: run_doc }
    """
    best: dict = {}
    for run in all_runs:
        mtype = run["model_type"]
        if mtype not in best or run["f1"] > best[mtype]["f1"]:
            best[mtype] = run
    return best


def collect_drift_results() -> list[dict]:
    """
    Collecte les résultats de drift depuis les runs MLflow tagués 'drift'.
    Cherche dans l'expérience de détection d'anomalies les runs avec
    des métriques de drift (global_drift_score, drifted_features_count).
    """
    logger.info("🌊 Résultats drift detection...")
    payload = {
        "experiment_ids": [],  # cherche dans tous
        "filter": "tags.task = 'drift_detection'",
        "max_results": 20,
        "order_by": ["start_time DESC"],
    }
    # Chercher dans toutes les expériences
    all_exp = _mlflow_get("experiments/search", {"max_results": 100})
    exp_ids = [e.get("experiment_id") for e in all_exp.get("experiments", [])]

    drift_runs = []
    if exp_ids:
        payload["experiment_ids"] = exp_ids
        data = _mlflow_post("runs/search", payload)
        for r in data.get("runs", []):
            info = r.get("info", {})
            metrics = r.get("data", {}).get("metrics", {})
            tags = {t["key"]: t["value"] for t in r.get("data", {}).get("tags", [])}
            start_ms = info.get("start_time", 0)
            drift_runs.append({
                "run_id": info.get("run_id"),
                "run_name": info.get("run_name", ""),
                "start_time": datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).isoformat() if start_ms else None,
                "drift_level": tags.get("drift_level", "unknown"),
                "global_drift_score": float(metrics.get("global_drift_score", 0.0) or 0.0),
                "drifted_features_count": int(metrics.get("drifted_features_count", 0) or 0),
                "total_features": int(metrics.get("total_features", 0) or 0),
                "ks_statistic_mean": float(metrics.get("ks_statistic_mean", 0.0) or 0.0),
            })

    logger.info(f"  → {len(drift_runs)} runs drift trouvés")
    return drift_runs


def compute_ml_health_score(runs: list[dict], models: list[dict]) -> dict:
    """
    Calcule un score de santé ML global :
    - Modèles en production : +30 pts chacun (max 60)
    - Meilleur F1 global : jusqu'à 40 points
    """
    prod_models = [m for m in models if m["stage"].lower() in ("production", "production")]
    prod_count = len(prod_models)

    best_f1 = max((r["f1"] for r in runs), default=0.0)

    prod_score = min(prod_count * 30, 60)
    f1_score = round(best_f1 * 40)
    total = prod_score + f1_score

    return {
        "score": total,
        "production_models": prod_count,
        "best_f1": round(best_f1, 4),
        "total_runs": len(runs),
        "registered_models": len(models),
    }


# ─────────────────────── MAIN ────────────────────────────────────────────

def main():
    logger.info("=" * 60)
    logger.info("🤖 ML METRICS COLLECTOR")
    logger.info("=" * 60)

    start_time = time.time()
    now = datetime.now(timezone.utc)

    # 1. Vérifier MLflow
    mlflow_available = check_mlflow_available()

    all_runs: list[dict] = []
    registered_models: list[dict] = []
    drift_results: list[dict] = []
    best_by_type: dict = {}

    if mlflow_available:
        # 2. Collecter les runs de chaque expérience
        for exp_name in EXPERIMENTS_TO_TRACK:
            exp_id = get_experiment_id(exp_name)
            if exp_id:
                runs = collect_experiment_runs(exp_name, exp_id)
                all_runs.extend(runs)
            else:
                logger.warning(f"  ⚠️  Expérience '{exp_name}' non trouvée dans MLflow")

        # 3. Modèles enregistrés
        registered_models = collect_registered_models()

        # 4. Drift results
        drift_results = collect_drift_results()

        # 5. Meilleurs runs par type de modèle
        best_by_type = collect_best_runs_per_model_type(all_runs)

    # 6. Score de santé ML
    health = compute_ml_health_score(all_runs, registered_models)

    # 7. Construire le document de métriques
    duration = round(time.time() - start_time, 2)
    metrics_doc = {
        "timestamp": now,
        "mlflow_available": mlflow_available,
        "health": health,
        "all_runs": all_runs,
        "registered_models": registered_models,
        "drift_results": drift_results,
        "best_by_model_type": best_by_type,
        "collection_duration_sec": duration,
        "expire_at": now + timedelta(days=METRICS_TTL_DAYS),
    }

    # 8. Persister dans MongoDB
    logger.info("")
    logger.info("💾 Persistance dans MongoDB (ml_metrics)...")
    mongo_client = MongoClient(MONGODB_URI)
    db = mongo_client[MONGODB_DB]
    coll = db["ml_metrics"]

    # Créer index TTL
    existing_indexes = [idx["name"] for idx in coll.list_indexes()]
    if "expire_at_ttl" not in existing_indexes:
        coll.create_index("expire_at", expireAfterSeconds=0, name="expire_at_ttl")
        logger.info("  🗂️  Index TTL créé sur ml_metrics.expire_at")
    if "timestamp_desc" not in existing_indexes:
        coll.create_index([("timestamp", DESCENDING)], name="timestamp_desc")

    result = coll.insert_one(metrics_doc)
    mongo_client.close()

    logger.info("")
    logger.info("=" * 60)
    logger.info("✅ ML MÉTRIQUES COLLECTÉES ET PERSISTÉES")
    logger.info("=" * 60)
    logger.info(f"📋 Document ID: {result.inserted_id}")
    logger.info(f"🤖 MLflow disponible: {mlflow_available}")
    logger.info(f"📊 Runs collectés: {len(all_runs)}")
    logger.info(f"📦 Modèles en registry: {len(registered_models)}")
    logger.info(f"🌊 Runs drift: {len(drift_results)}")
    logger.info(f"💚 Score santé ML: {health['score']}/100")
    logger.info(f"  → Modèles en prod: {health['production_models']}")
    logger.info(f"  → Meilleur F1: {health['best_f1']}")
    for mtype, run in best_by_type.items():
        logger.info(f"  → Meilleur {mtype}: F1={run['f1']:.4f} | acc={run['accuracy']:.4f}")
    logger.info(f"⏱️  Durée: {duration}s")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

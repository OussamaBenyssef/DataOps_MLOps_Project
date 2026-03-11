#!/usr/bin/env python3
"""
DataOps Metrics Collector — P5
================================
Collecte les métriques opérationnelles du pipeline DataOps/MLOps
et les persiste dans MongoDB pour visualisation dans Metabase.

Métriques collectées :
  - Santé des services (Kafka, MongoDB, Spark, MLflow, Airflow, Metabase, FastAPI)
  - Volume de données par collection
  - Fraîcheur des données (dernière insertion)
  - Couverture par symbole (BTCUSDT, ETHUSDT, BNBUSDT)
  - Taux de nulls sur champs critiques OHLCV
  - Métriques MLflow (expériences, runs, modèles)

Usage:
    python metabase/collect_dataops_metrics.py
"""

import json
import logging
import socket
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

MONGODB_URI = "mongodb://datamlops:datamlops123@localhost:27017/cryptomarket?authSource=admin"
MONGODB_DB = "cryptomarket"

TRADING_PAIRS = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
OHLCV_REQUIRED_FIELDS = ["open", "high", "low", "close", "volume"]

SERVICES = {
    "kafka":    {"host": "localhost", "port": 29092, "type": "tcp"},
    "mongodb":  {"host": "localhost", "port": 27017, "type": "tcp"},
    "spark":    {"url": "http://localhost:8080", "type": "http"},
    "mlflow":   {"url": "http://localhost:5001", "type": "http"},
    "airflow":  {"url": "http://localhost:8081/health", "type": "http"},
    "metabase": {"url": "http://localhost:3000/api/health", "type": "http"},
    "fastapi":  {"url": "http://localhost:8000/health", "type": "http"},
}

# TTL — les métriques expirent après 30 jours
METRICS_TTL_DAYS = 30


# ─────────────────────── COLLECTORS ──────────────────────────────────────

def check_tcp(host: str, port: int, timeout: float = 3.0) -> bool:
    """Vérifie qu'un service TCP est joignable."""
    try:
        sock = socket.create_connection((host, port), timeout=timeout)
        sock.close()
        return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False


def check_http(url: str, timeout: float = 5.0) -> bool:
    """Vérifie qu'un service HTTP répond (2xx/3xx)."""
    try:
        resp = requests.get(url, timeout=timeout)
        return resp.status_code < 400
    except (requests.ConnectionError, requests.Timeout):
        return False


def collect_services_health() -> dict:
    """Collecte l'état de santé de tous les services."""
    logger.info("🔌 Vérification des services...")
    results = {}
    for name, cfg in SERVICES.items():
        if cfg["type"] == "tcp":
            alive = check_tcp(cfg["host"], cfg["port"])
        else:
            alive = check_http(cfg["url"])
        status = "up" if alive else "down"
        results[name] = status
        icon = "✅" if alive else "❌"
        logger.info(f"  {icon} {name}: {status}")
    return results


def collect_data_volume(db) -> dict:
    """Collecte le nombre de documents par collection."""
    logger.info("📦 Volume des données...")
    collections = ["raw_trades", "ohlcv", "indicators", "anomalies", "predictions"]
    volume = {}
    total = 0
    for coll_name in collections:
        count = db[coll_name].count_documents({})
        volume[coll_name] = count
        total += count
        logger.info(f"  {coll_name}: {count:,} documents")
    volume["total"] = total
    return volume


def collect_data_freshness(db) -> dict:
    """Collecte le timestamp du document le plus récent par collection."""
    logger.info("⏱️ Fraîcheur des données...")
    collections = {
        "ohlcv": "processed_at",
        "indicators": "processed_at",
        "anomalies": "processed_at",
    }
    freshness = {}
    now = datetime.now(timezone.utc)
    for coll_name, ts_field in collections.items():
        doc = db[coll_name].find_one(
            {ts_field: {"$exists": True}},
            sort=[(ts_field, DESCENDING)],
            projection={ts_field: 1, "_id": 0},
        )
        if doc and ts_field in doc:
            latest = doc[ts_field]
            if isinstance(latest, datetime):
                age_minutes = (now - latest.replace(tzinfo=timezone.utc)).total_seconds() / 60
            else:
                age_minutes = -1
            freshness[coll_name] = {
                "latest": latest.isoformat() if isinstance(latest, datetime) else str(latest),
                "age_minutes": round(age_minutes, 1),
            }
            logger.info(f"  {coll_name}: {latest} (âge: {age_minutes:.0f} min)")
        else:
            freshness[coll_name] = {"latest": None, "age_minutes": -1}
            logger.info(f"  {coll_name}: aucune donnée")
    return freshness


def collect_symbol_coverage(db) -> dict:
    """Couverture de données par symbole."""
    logger.info("🎯 Couverture par symbole...")
    coverage = {}
    for symbol in TRADING_PAIRS:
        ohlcv_count = db["ohlcv"].count_documents({"symbol": symbol})
        indicators_count = db["indicators"].count_documents({"symbol": symbol})
        anomalies_count = db["anomalies"].count_documents({"symbol": symbol})
        coverage[symbol] = {
            "ohlcv": ohlcv_count,
            "indicators": indicators_count,
            "anomalies": anomalies_count,
        }
        logger.info(f"  {symbol}: ohlcv={ohlcv_count}, indicators={indicators_count}, anomalies={anomalies_count}")
    return coverage


def collect_null_rates(db) -> dict:
    """Calcule le taux de nulls sur les champs critiques OHLCV."""
    logger.info("🧪 Taux de nulls OHLCV...")
    sample_size = 500
    sample = list(db["ohlcv"].find().sort("processed_at", DESCENDING).limit(sample_size))

    if not sample:
        logger.warning("  Aucune donnée OHLCV pour calculer les nulls")
        return {"overall_null_pct": 0, "fields": {}, "sample_size": 0}

    actual_size = len(sample)
    field_nulls = {}
    total_nulls = 0
    for field in OHLCV_REQUIRED_FIELDS:
        nulls = sum(1 for doc in sample if doc.get(field) is None)
        pct = round(nulls / actual_size * 100, 2)
        field_nulls[field] = {"nulls": nulls, "pct": pct}
        total_nulls += nulls

    overall_pct = round(total_nulls / (actual_size * len(OHLCV_REQUIRED_FIELDS)) * 100, 2)
    logger.info(f"  Taux global de nulls: {overall_pct}% (échantillon: {actual_size} docs)")

    return {
        "overall_null_pct": overall_pct,
        "fields": field_nulls,
        "sample_size": actual_size,
    }


def collect_mlflow_metrics() -> dict:
    """Collecte les métriques MLflow via l'API REST."""
    logger.info("🤖 Métriques MLflow...")
    mlflow_url = "http://localhost:5001"
    metrics = {
        "experiments": 0,
        "total_runs": 0,
        "registered_models": 0,
        "production_models": 0,
        "available": False,
    }

    try:
        # Expériences
        resp = requests.get(
            f"{mlflow_url}/api/2.0/mlflow/experiments/search",
            params={"max_results": 100},
            timeout=5,
        )
        if resp.status_code == 200:
            experiments = resp.json().get("experiments", [])
            metrics["experiments"] = len(experiments)
            metrics["available"] = True
            logger.info(f"  Expériences: {len(experiments)}")

            # Runs par expérience
            total_runs = 0
            for exp in experiments:
                exp_id = exp.get("experiment_id")
                resp_runs = requests.post(
                    f"{mlflow_url}/api/2.0/mlflow/runs/search",
                    json={"experiment_ids": [exp_id], "max_results": 1000},
                    timeout=5,
                )
                if resp_runs.status_code == 200:
                    runs = resp_runs.json().get("runs", [])
                    total_runs += len(runs)
            metrics["total_runs"] = total_runs
            logger.info(f"  Runs total: {total_runs}")

        # Modèles enregistrés
        resp = requests.get(
            f"{mlflow_url}/api/2.0/mlflow/registered-models/search",
            params={"max_results": 100},
            timeout=5,
        )
        if resp.status_code == 200:
            models = resp.json().get("registered_models", [])
            metrics["registered_models"] = len(models)
            # Compter les modèles en production
            prod_count = 0
            for model in models:
                for version in model.get("latest_versions", []):
                    if version.get("current_stage") == "Production":
                        prod_count += 1
            metrics["production_models"] = prod_count
            logger.info(f"  Modèles enregistrés: {len(models)}, en prod: {prod_count}")

    except Exception as e:
        logger.warning(f"  ⚠️ MLflow inaccessible: {e}")

    return metrics


# ─────────────────────── MAIN ────────────────────────────────────────────

def main():
    logger.info("=" * 60)
    logger.info("📊 DATAOPS METRICS COLLECTOR")
    logger.info("=" * 60)

    start_time = time.time()
    now = datetime.now(timezone.utc)

    # Connexion MongoDB
    client = MongoClient(MONGODB_URI)
    db = client[MONGODB_DB]

    # Collecte de toutes les métriques
    services = collect_services_health()
    volume = collect_data_volume(db)
    freshness = collect_data_freshness(db)
    coverage = collect_symbol_coverage(db)
    null_rates = collect_null_rates(db)
    mlflow = collect_mlflow_metrics()

    # Score de santé global (0-100)
    services_up = sum(1 for s in services.values() if s == "up")
    services_total = len(services)
    health_score = round(services_up / services_total * 100)

    # Construire le document métriques
    metrics_doc = {
        "timestamp": now,
        "health_score": health_score,
        "services": services,
        "services_up": services_up,
        "services_total": services_total,
        "data_volume": volume,
        "data_freshness": freshness,
        "symbol_coverage": coverage,
        "null_rates": null_rates,
        "mlflow": mlflow,
        "collection_duration_sec": round(time.time() - start_time, 2),
        "expire_at": now + timedelta(days=METRICS_TTL_DAYS),
    }

    # Persister dans MongoDB
    coll = db["dataops_metrics"]

    # Créer l'index TTL si nécessaire
    existing_indexes = [idx["name"] for idx in coll.list_indexes()]
    if "expire_at_ttl" not in existing_indexes:
        coll.create_index("expire_at", expireAfterSeconds=0, name="expire_at_ttl")
        logger.info("🗂️ Index TTL créé sur dataops_metrics.expire_at")

    # Créer index sur timestamp pour les requêtes
    if "timestamp_desc" not in existing_indexes:
        coll.create_index([("timestamp", DESCENDING)], name="timestamp_desc")

    result = coll.insert_one(metrics_doc)

    logger.info("")
    logger.info("=" * 60)
    logger.info("✅ MÉTRIQUES COLLECTÉES ET PERSISTÉES")
    logger.info("=" * 60)
    logger.info(f"📋 Document ID: {result.inserted_id}")
    logger.info(f"💚 Score de santé: {health_score}% ({services_up}/{services_total} services)")
    logger.info(f"📦 Total documents: {volume['total']:,}")
    logger.info(f"🧪 Taux de nulls OHLCV: {null_rates['overall_null_pct']}%")
    logger.info(f"🤖 MLflow: {mlflow['experiments']} expériences, {mlflow['registered_models']} modèles")
    logger.info(f"⏱️ Durée collecte: {metrics_doc['collection_duration_sec']}s")
    logger.info("=" * 60)

    client.close()


if __name__ == "__main__":
    main()

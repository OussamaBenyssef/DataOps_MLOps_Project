#!/usr/bin/env python3
"""
pipeline_status.py — Tableau de bord terminal DataOps/MLOps
=============================================================
Affiche en un coup d'œil l'état complet du pipeline :
  - Volume MongoDB par collection
  - Fraîcheur des données OHLCV par symbole
  - Statut MLflow (expériences, runs, modèles en production)
  - Dernier entraînement ML
  - Santé résumée des services

Usage:
    # Avec le venv du projet :
    .venv/bin/python3 scripts/pipeline_status.py
    # Ou via Makefile :
    make status
"""

import sys
import socket
from datetime import datetime, timezone
from typing import Optional

# ─── Dépendances optionnelles ────────────────────────────────────
try:
    import requests
except ImportError:
    print("❌ Module 'requests' manquant.")
    print("   Activez le venv du projet :")
    print("   source .venv/bin/activate && pip install requests pymongo")
    print("   Ou utilisez : make status  (utilise automatiquement .venv)")
    sys.exit(1)

try:
    from pymongo import MongoClient, DESCENDING as _DESCENDING
    PYMONGO_OK = True
except ImportError:
    PYMONGO_OK = False

# ─── Couleurs ANSI ──────────────────────────────────────────────
RESET  = "\033[0m"
BOLD   = "\033[1m"
RED    = "\033[31m"
GREEN  = "\033[32m"
YELLOW = "\033[33m"
BLUE   = "\033[34m"
CYAN   = "\033[36m"
WHITE  = "\033[37m"

# ─── Config ─────────────────────────────────────────────────────
MONGODB_URI  = "mongodb://datamlops:datamlops123@localhost:27017/cryptomarket?authSource=admin"
MLFLOW_URL   = "http://localhost:5001"
AIRFLOW_URL  = "http://localhost:8081"
TRADING_PAIRS = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]

SERVICES = {
    "Kafka":     ("tcp",  "localhost", 29092),
    "MongoDB":   ("tcp",  "localhost", 27017),
    "Spark":     ("http", "http://localhost:8080", None),
    "Airflow":   ("http", "http://localhost:8081/health", None),
    "MLflow":    ("http", "http://localhost:5001", None),
    "FastAPI":   ("http", "http://localhost:8000/health", None),
    "Metabase":  ("http", "http://localhost:3000/api/health", None),
    "DataHub":   ("http", "http://localhost:8082/config", None),
}


def divider(char: str = "═", width: int = 56) -> str:
    return CYAN + char * width + RESET


def header(title: str) -> str:
    return f"\n{divider()}\n  {BOLD}{CYAN}{title}{RESET}\n{divider()}"


def fmt_count(n: Optional[int]) -> str:
    if n is None:
        return f"{YELLOW}N/A{RESET}"
    if n == 0:
        return f"{RED}0{RESET}"
    return f"{GREEN}{n:,}{RESET}"


def fmt_age(minutes: Optional[float]) -> str:
    if minutes is None:
        return f"{RED}N/A{RESET}"
    if minutes < 0:
        return f"{YELLOW}N/A{RESET}"
    if minutes < 10:
        return f"{GREEN}{minutes:.0f} min{RESET}"
    if minutes < 60:
        return f"{YELLOW}{minutes:.0f} min{RESET}"
    return f"{RED}{minutes:.0f} min{RESET}"


def check_service(name: str, cfg: tuple) -> str:
    kind, *args = cfg
    try:
        if kind == "tcp":
            _, host, port = cfg
            s = socket.create_connection((host, port), timeout=3)
            s.close()
            return f"{GREEN}✅ UP{RESET}"
        else:
            _, url, _ = cfg
            resp = requests.get(url, timeout=4)
            if resp.status_code < 400:
                return f"{GREEN}✅ UP{RESET}"
            return f"{YELLOW}⚠️  HTTP {resp.status_code}{RESET}"
    except Exception:
        return f"{RED}❌ DOWN{RESET}"


# ══════════════════════════════════════════════════════════════════
# BLOCS D'AFFICHAGE
# ══════════════════════════════════════════════════════════════════

def show_services():
    print(header("🔌 Services"))
    for name, cfg in SERVICES.items():
        status = check_service(name, cfg)
        print(f"  {BOLD}{name:<16}{RESET} {status}")


def show_mongodb():
    print(header("🗄️  MongoDB — Volumes"))
    try:
        from pymongo import MongoClient, DESCENDING
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=4000)
        db = client["cryptomarket"]

        collections = ["raw_trades", "ohlcv", "indicators", "anomalies", "predictions"]
        total = 0
        for coll in collections:
            count = db[coll].count_documents({})
            total += count
            print(f"  {BOLD}{coll:<20}{RESET} {fmt_count(count)} documents")
        print(f"  {BOLD}{'TOTAL':<20}{RESET} {fmt_count(total)} documents")

        # Fraîcheur OHLCV
        print(f"\n  {BOLD}{CYAN}⏱  Fraîcheur OHLCV{RESET}")
        now = datetime.now(timezone.utc)
        for symbol in TRADING_PAIRS:
            doc = db["ohlcv"].find_one(
                {"symbol": symbol},
                sort=[("processed_at", DESCENDING)],
                projection={"processed_at": 1}
            )
            if doc and doc.get("processed_at"):
                ts = doc["processed_at"]
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                age_min = (now - ts).total_seconds() / 60
                print(f"  {BOLD}{symbol:<20}{RESET} {fmt_age(age_min)} ago")
            else:
                print(f"  {BOLD}{symbol:<20}{RESET} {RED}Aucune donnée{RESET}")

        # Dernier training ML
        meta = db["ml_metadata"].find_one({"_id": "last_training"})
        if meta and meta.get("timestamp"):
            ts = meta["timestamp"]
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            age_h = (now - ts).total_seconds() / 3600
            total_trained = meta.get("total_all_data", "?")
            print(f"\n  {BOLD}{'Dernier training ML':<20}{RESET} il y a {YELLOW}{age_h:.1f}h{RESET} "
                  f"(sur {fmt_count(total_trained)} docs)")
        else:
            print(f"\n  {BOLD}{'Dernier training ML':<20}{RESET} {YELLOW}Jamais entraîné{RESET}")

        client.close()

    except Exception as e:
        print(f"  {RED}❌ MongoDB inaccessible : {e}{RESET}")


def show_mlflow():
    print(header("🤖 MLflow — Expériences & Modèles"))
    try:
        # Expériences
        resp = requests.get(
            f"{MLFLOW_URL}/api/2.0/mlflow/experiments/search",
            params={"max_results": 100},
            timeout=5
        )
        if resp.status_code != 200:
            print(f"  {RED}❌ MLflow inaccessible (HTTP {resp.status_code}){RESET}")
            return

        experiments = resp.json().get("experiments", [])
        print(f"  {BOLD}{'Expériences':<26}{RESET} {fmt_count(len(experiments))}")

        # Runs totaux
        total_runs = 0
        for exp in experiments:
            r = requests.post(
                f"{MLFLOW_URL}/api/2.0/mlflow/runs/search",
                json={"experiment_ids": [exp["experiment_id"]], "max_results": 1000},
                timeout=5
            )
            if r.status_code == 200:
                total_runs += len(r.json().get("runs", []))
        print(f"  {BOLD}{'Runs totaux':<26}{RESET} {fmt_count(total_runs)}")

        # Modèles registrés
        resp_m = requests.get(
            f"{MLFLOW_URL}/api/2.0/mlflow/registered-models/search",
            params={"max_results": 100},
            timeout=5
        )
        if resp_m.status_code == 200:
            models = resp_m.json().get("registered_models", [])
            prod_count = 0
            print(f"  {BOLD}{'Modèles enregistrés':<26}{RESET} {fmt_count(len(models))}")
            for model in models:
                for version in model.get("latest_versions", []):
                    if version.get("current_stage") == "Production":
                        prod_count += 1
                        print(f"    {GREEN}🏆 Production{RESET}  {BOLD}{model['name']}{RESET} "
                              f"v{version.get('version', '?')}")
            if prod_count == 0:
                print(f"  {YELLOW}  Aucun modèle en production{RESET}")

    except Exception as e:
        print(f"  {RED}❌ MLflow inaccessible : {e}{RESET}")


def show_airflow():
    print(header("✈️  Airflow — DAGs"))
    try:
        resp = requests.get(
            f"{AIRFLOW_URL}/api/v1/dags",
            auth=("admin", "admin"),
            timeout=5
        )
        if resp.status_code != 200:
            print(f"  {YELLOW}⚠️  API Airflow non disponible (HTTP {resp.status_code}){RESET}")
            return

        dags = resp.json().get("dags", [])
        for dag in dags:
            dag_id = dag.get("dag_id", "?")
            is_paused = dag.get("is_paused", True)
            schedule = dag.get("schedule_interval", "?") or "manual"
            status = f"{GREEN}actif{RESET}" if not is_paused else f"{YELLOW}pausé{RESET}"
            print(f"  {BOLD}{dag_id:<35}{RESET} {status}  {WHITE}[{schedule}]{RESET}")

    except Exception as e:
        print(f"  {YELLOW}⚠️  Airflow API inaccessible : {e}{RESET}")


# ══════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════

def main():
    print(f"\n{BOLD}{BLUE}╔══════════════════════════════════════════════════════╗{RESET}")
    print(f"{BOLD}{BLUE}║   DataOps/MLOps Pipeline — Tableau de Bord           ║{RESET}")
    print(f"{BOLD}{BLUE}╚══════════════════════════════════════════════════════╝{RESET}")
    print(f"  {WHITE}{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC{RESET}")

    show_services()
    show_mongodb()
    show_mlflow()
    show_airflow()

    print(f"\n{divider()}\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrompu.")
        sys.exit(0)

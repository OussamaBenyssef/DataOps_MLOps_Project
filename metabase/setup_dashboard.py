#!/usr/bin/env python3
"""
Metabase Dashboard Setup — P5 (Visualisation)
==============================================
Script d'auto-configuration du dashboard Metabase via l'API REST.
Crée la connexion MongoDB, les questions (visualisations), et le dashboard.

Usage:
    python metabase/setup_dashboard.py

Prérequis:
    - Metabase running sur http://localhost:3000
    - MongoDB running avec des données dans cryptomarket
    - pip install requests
"""

import sys
import time
import json
import logging
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ─────────────────────── CONFIG ──────────────────────────────────────────

METABASE_URL = "http://localhost:3000"
METABASE_EMAIL = "admin@crypto.local"
METABASE_PASSWORD = "DataMLOps2024!"
METABASE_FIRST_NAME = "Oussama"
METABASE_LAST_NAME = "Benyssef"

MONGODB_HOST = "mongodb"
MONGODB_PORT = 27017
MONGODB_DB = "cryptomarket"
MONGODB_USER = "datamlops"
MONGODB_PASSWORD = "datamlops123"
MONGODB_AUTH_DB = "admin"

DASHBOARD_NAME = "🚀 Crypto Market Overview"
DASHBOARD_DESCRIPTION = (
    "Dashboard temps réel du marché crypto : prix, volumes, indicateurs techniques, "
    "et détection d'anomalies. Données issues du pipeline DataOps/MLOps."
)


# ─────────────────────── HELPERS ─────────────────────────────────────────

class MetabaseClient:
    """Client minimal pour l'API REST Metabase (compatible v0.48+)."""

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self.session_token = None

    def _headers(self):
        h = {"Content-Type": "application/json"}
        if self.session_token:
            h["X-Metabase-Session"] = self.session_token
        return h

    def _request(self, method, path, **kwargs):
        url = f"{self.base_url}/api{path}"
        resp = requests.request(method, url, headers=self._headers(), **kwargs)
        if resp.status_code >= 400:
            logger.error(f"{method} {path} → {resp.status_code}: {resp.text[:300]}")
        return resp

    def get(self, path):
        return self._request("GET", path)

    def post(self, path, data=None):
        return self._request("POST", path, json=data)

    def put(self, path, data=None):
        return self._request("PUT", path, json=data)

    def delete(self, path):
        return self._request("DELETE", path)

    # ── Auth ──

    def wait_for_metabase(self, timeout=120):
        """Attend que Metabase soit prêt."""
        logger.info(f"⏳ Attente de Metabase sur {self.base_url}...")
        start = time.time()
        while time.time() - start < timeout:
            try:
                resp = requests.get(f"{self.base_url}/api/health", timeout=5)
                if resp.status_code == 200 and resp.json().get("status") == "ok":
                    logger.info("✅ Metabase est prêt")
                    return True
            except requests.ConnectionError:
                pass
            time.sleep(3)
        logger.error("❌ Timeout — Metabase n'a pas répondu")
        return False

    def setup_initial(self):
        """Premier setup de Metabase (création admin + skip onboarding)."""
        resp = self.get("/session/properties")
        if resp.status_code == 200:
            props = resp.json()
            if props.get("has-user-setup"):
                logger.info("ℹ️  Setup initial déjà fait — skip")
                return True

        logger.info("🔧 Configuration initiale de Metabase...")
        setup_data = {
            "token": self._get_setup_token(),
            "user": {
                "email": METABASE_EMAIL,
                "password": METABASE_PASSWORD,
                "first_name": METABASE_FIRST_NAME,
                "last_name": METABASE_LAST_NAME,
                "site_name": "CryptoMarket DataOps",
            },
            "prefs": {
                "site_name": "CryptoMarket DataOps",
                "site_locale": "fr",
                "allow_tracking": False,
            },
        }
        resp = self.post("/setup", data=setup_data)
        if resp.status_code in (200, 201):
            logger.info("✅ Setup initial terminé")
            return True
        logger.warning(f"⚠️  Setup response: {resp.status_code}")
        return False

    def _get_setup_token(self):
        resp = self.get("/session/properties")
        if resp.status_code == 200:
            return resp.json().get("setup-token", "")
        return ""

    def login(self):
        """Login et récupération du token de session."""
        resp = self.post("/session", data={
            "username": METABASE_EMAIL,
            "password": METABASE_PASSWORD,
        })
        if resp.status_code == 200:
            self.session_token = resp.json().get("id")
            logger.info("✅ Connexion Metabase réussie")
            return True
        logger.error(f"❌ Login échoué: {resp.status_code}")
        return False

    # ── Database ──

    def add_mongodb_database(self):
        """Ajoute MongoDB comme source de données."""
        resp = self.get("/database")
        if resp.status_code == 200:
            for db in resp.json().get("data", []):
                if db.get("name") == "CryptoMarket MongoDB":
                    logger.info(f"ℹ️  Database déjà configurée (id={db['id']})")
                    return db["id"]

        logger.info("📦 Ajout de MongoDB comme source de données...")
        db_config = {
            "name": "CryptoMarket MongoDB",
            "engine": "mongo",
            "details": {
                "host": MONGODB_HOST,
                "port": MONGODB_PORT,
                "dbname": MONGODB_DB,
                "user": MONGODB_USER,
                "pass": MONGODB_PASSWORD,
                "authdb": MONGODB_AUTH_DB,
                "use-srv": False,
                "ssl": False,
            },
            "is_full_sync": True,
            "auto_run_queries": True,
        }
        resp = self.post("/database", data=db_config)
        if resp.status_code in (200, 201):
            db_id = resp.json().get("id")
            logger.info(f"✅ MongoDB ajouté (id={db_id})")
            self.post(f"/database/{db_id}/sync_schema")
            logger.info("🔄 Sync du schéma lancé — attente 10s...")
            time.sleep(10)
            return db_id
        logger.error(f"❌ Erreur ajout MongoDB: {resp.text[:200]}")
        return None

    # ── Questions (Saved Questions) ──

    def _find_existing_card(self, name):
        """Cherche une question existante par nom."""
        resp = self.get("/card")
        if resp.status_code == 200:
            for card in resp.json():
                if card.get("name") == name:
                    return card.get("id")
        return None

    def create_native_question(self, name, description, db_id, collection, pipeline, display="table", viz_settings=None):
        """Crée une question MongoDB native (idempotent — skip si existe)."""
        existing_id = self._find_existing_card(name)
        if existing_id:
            logger.info(f"  ℹ️  Question '{name}' existe déjà (id={existing_id})")
            return existing_id

        question_data = {
            "name": name,
            "description": description,
            "dataset_query": {
                "type": "native",
                "native": {
                    "query": json.dumps(pipeline),
                    "collection": collection,
                },
                "database": db_id,
            },
            "display": display,
            "visualization_settings": viz_settings or {},
        }
        resp = self.post("/card", data=question_data)
        if resp.status_code in (200, 201):
            card_id = resp.json().get("id")
            logger.info(f"  ✅ Question '{name}' créée (id={card_id})")
            return card_id
        logger.error(f"  ❌ Erreur question '{name}': {resp.text[:200]}")
        return None

    # ── Dashboard ──

    def _find_existing_dashboard(self, name):
        """Cherche un dashboard existant par nom (compatible v0.48+)."""
        # Méthode 1 : recherche via collection root items
        resp = self.get("/collection/root/items?models=dashboard")
        if resp.status_code == 200:
            data = resp.json()
            items = data.get("data", data) if isinstance(data, dict) else data
            for item in items:
                if item.get("name") == name:
                    return item.get("id")
        # Méthode 2 : recherche via /search
        resp = self.get(f"/search?q={name}&models=dashboard")
        if resp.status_code == 200:
            for item in resp.json().get("data", []):
                if item.get("name") == name:
                    return item.get("id")
        return None

    def create_dashboard(self, name, description):
        """Crée un dashboard vide."""
        existing_id = self._find_existing_dashboard(name)
        if existing_id:
            logger.info(f"ℹ️  Dashboard '{name}' existe déjà (id={existing_id})")
            return existing_id

        resp = self.post("/dashboard", data={
            "name": name,
            "description": description,
        })
        if resp.status_code in (200, 201):
            dash_id = resp.json().get("id")
            logger.info(f"✅ Dashboard '{name}' créé (id={dash_id})")
            return dash_id
        logger.error(f"❌ Erreur dashboard: {resp.text[:200]}")
        return None

    def add_cards_to_dashboard(self, dashboard_id, cards_info):
        """Ajoute toutes les cartes au dashboard via PUT /dashboard/:id (v0.48+).

        Utilise le champ 'dashcards' (Metabase v0.48).
        Les cartes avec un id négatif sont traitées comme de nouvelles cartes.
        """
        # Récupérer les dashcards existantes pour ne pas les écraser
        resp = self.get(f"/dashboard/{dashboard_id}")
        existing_dashcards = []
        if resp.status_code == 200:
            existing_dashcards = resp.json().get("dashcards", [])

        # Construire les nouvelles dashcards
        new_dashcards = []
        next_negative_id = -1
        for card in cards_info:
            if card["card_id"] is None:
                continue
            # Vérifier si ce card_id est déjà dans le dashboard
            already_present = any(
                dc.get("card_id") == card["card_id"]
                for dc in existing_dashcards
            )
            if already_present:
                continue
            new_dashcards.append({
                "id": next_negative_id,
                "card_id": card["card_id"],
                "row": card["row"],
                "col": card["col"],
                "size_x": card["size_x"],
                "size_y": card["size_y"],
                "parameter_mappings": [],
                "visualization_settings": {},
            })
            next_negative_id -= 1

        if not new_dashcards and not existing_dashcards:
            logger.warning("⚠️  Aucune carte valide à ajouter")
            return False

        if not new_dashcards:
            logger.info(f"  ℹ️  Toutes les cartes sont déjà dans le dashboard")
            return True

        # Combiner existantes + nouvelles
        all_dashcards = existing_dashcards + new_dashcards

        resp = self.put(f"/dashboard/{dashboard_id}", data={
            "dashcards": all_dashcards,
        })
        if resp.status_code in (200, 201):
            result_cards = resp.json().get("dashcards", [])
            logger.info(f"  ✅ {len(new_dashcards)} cartes ajoutées au dashboard ({len(result_cards)} total)")
            return True

        logger.error(f"  ❌ Impossible d'ajouter les cartes: {resp.status_code}: {resp.text[:300]}")
        return False

    # ── Cleanup ──

    def cleanup_old_resources(self):
        """Supprime les anciennes questions et dashboards dupliqués."""
        logger.info("🧹 Nettoyage des ressources dupliquées...")

        # Trouver et supprimer les questions dupliquées (garder les plus récentes)
        resp = self.get("/card")
        if resp.status_code == 200:
            cards = resp.json()
            # Grouper par nom
            by_name = {}
            for card in cards:
                name = card.get("name", "")
                by_name.setdefault(name, []).append(card)

            deleted = 0
            for name, group in by_name.items():
                if len(group) > 1:
                    # Garder le plus récent, supprimer les autres
                    group.sort(key=lambda c: c.get("id", 0), reverse=True)
                    for old_card in group[1:]:
                        self.delete(f"/card/{old_card['id']}")
                        deleted += 1

            if deleted:
                logger.info(f"  🗑️  {deleted} questions dupliquées supprimées")
            else:
                logger.info(f"  ✅ Aucun doublon trouvé")

        # Trouver et supprimer les dashboards dupliqués
        resp = self.get("/collection/root/items?models=dashboard")
        if resp.status_code == 200:
            data = resp.json()
            items = data.get("data", data) if isinstance(data, dict) else data
            by_name = {}
            for item in items:
                name = item.get("name", "")
                by_name.setdefault(name, []).append(item)

            deleted = 0
            for name, group in by_name.items():
                if len(group) > 1:
                    group.sort(key=lambda d: d.get("id", 0), reverse=True)
                    for old_dash in group[1:]:
                        self.delete(f"/dashboard/{old_dash['id']}")
                        deleted += 1

            if deleted:
                logger.info(f"  🗑️  {deleted} dashboards dupliqués supprimés")


# ─────────────────────── QUESTIONS CONFIG ────────────────────────────────

def get_questions_config():
    """Définit les 6 questions (visualisations) du dashboard.

    Alignées sur le schéma réel MongoDB :
    - ohlcv: symbol, interval, timestamp, open, high, low, close, volume, ...
    - indicators: symbol, timestamp, rsi_14, macd, bollinger_*, sma_*, ema_*, ...
    - anomalies: symbol, timestamp, close, volume, is_price_drop_anomaly,
                 is_volume_spike_anomaly, processed_at, ...
    """
    return [
        {
            "name": "📈 Prix de Clôture (24h)",
            "description": "Évolution du prix de clôture par symbole sur les dernières 24h",
            "collection": "ohlcv",
            "pipeline": [
                {"$sort": {"timestamp": -1}},
                {"$limit": 4320},
                {"$project": {
                    "symbol": 1,
                    "timestamp": 1,
                    "close": 1,
                    "_id": 0,
                }},
                {"$sort": {"timestamp": 1}},
            ],
            "display": "line",
            "viz_settings": {
                "graph.dimensions": ["timestamp"],
                "graph.metrics": ["close"],
            },
            "row": 0, "col": 0, "size_x": 9, "size_y": 5,
        },
        {
            "name": "📊 Volume de Trading (24h)",
            "description": "Volume de trading par symbole sur les dernières 24h",
            "collection": "ohlcv",
            "pipeline": [
                {"$sort": {"timestamp": -1}},
                {"$limit": 4320},
                {"$project": {
                    "symbol": 1,
                    "timestamp": 1,
                    "volume": 1,
                    "_id": 0,
                }},
                {"$sort": {"timestamp": 1}},
            ],
            "display": "bar",
            "viz_settings": {
                "graph.dimensions": ["timestamp"],
                "graph.metrics": ["volume"],
            },
            "row": 0, "col": 9, "size_x": 9, "size_y": 5,
        },
        {
            "name": "📉 RSI-14 par Symbole",
            "description": "Relative Strength Index (14 périodes) — zones surachat/survente",
            "collection": "indicators",
            "pipeline": [
                {"$match": {"rsi_14": {"$ne": None}}},
                {"$sort": {"timestamp": -1}},
                {"$limit": 2000},
                {"$project": {
                    "symbol": 1,
                    "timestamp": 1,
                    "rsi_14": 1,
                    "_id": 0,
                }},
                {"$sort": {"timestamp": 1}},
            ],
            "display": "line",
            "viz_settings": {
                "graph.dimensions": ["timestamp"],
                "graph.metrics": ["rsi_14"],
                "graph.y_axis.title_text": "RSI-14",
            },
            "row": 5, "col": 0, "size_x": 9, "size_y": 5,
        },
        {
            "name": "🚨 Anomalies Détectées",
            "description": "Nombre d'anomalies par type (price drop / volume spike) et par symbole",
            "collection": "anomalies",
            "pipeline": [
                {"$project": {
                    "symbol": 1,
                    "anomaly_type": {
                        "$cond": {
                            "if": {"$eq": ["$is_volume_spike_anomaly", True]},
                            "then": "Volume Spike",
                            "else": {
                                "$cond": {
                                    "if": {"$eq": ["$is_price_drop_anomaly", True]},
                                    "then": "Price Drop",
                                    "else": "Other",
                                }
                            },
                        }
                    },
                    "_id": 0,
                }},
                {"$group": {
                    "_id": {"symbol": "$symbol", "type": "$anomaly_type"},
                    "count": {"$sum": 1},
                }},
                {"$project": {
                    "symbol": "$_id.symbol",
                    "anomaly_type": "$_id.type",
                    "count": 1,
                    "_id": 0,
                }},
                {"$sort": {"count": -1}},
            ],
            "display": "bar",
            "viz_settings": {
                "graph.dimensions": ["anomaly_type"],
                "graph.metrics": ["count"],
            },
            "row": 5, "col": 9, "size_x": 9, "size_y": 5,
        },
        {
            "name": "⚠️ Anomalies Récentes",
            "description": "Les 50 dernières anomalies détectées avec détails",
            "collection": "anomalies",
            "pipeline": [
                {"$sort": {"processed_at": -1}},
                {"$limit": 50},
                {"$project": {
                    "symbol": 1,
                    "timestamp": 1,
                    "close": 1,
                    "volume": 1,
                    "price_change_percent": 1,
                    "anomaly_type": {
                        "$cond": {
                            "if": {"$eq": ["$is_volume_spike_anomaly", True]},
                            "then": "🔺 Volume Spike",
                            "else": {
                                "$cond": {
                                    "if": {"$eq": ["$is_price_drop_anomaly", True]},
                                    "then": "🔻 Price Drop",
                                    "else": "Other",
                                }
                            },
                        }
                    },
                    "processed_at": 1,
                    "_id": 0,
                }},
            ],
            "display": "table",
            "viz_settings": {
                "table.columns": [
                    {"name": "timestamp", "enabled": True},
                    {"name": "symbol", "enabled": True},
                    {"name": "anomaly_type", "enabled": True},
                    {"name": "close", "enabled": True},
                    {"name": "volume", "enabled": True},
                    {"name": "price_change_percent", "enabled": True},
                    {"name": "processed_at", "enabled": True},
                ],
            },
            "row": 10, "col": 0, "size_x": 12, "size_y": 5,
        },
        {
            "name": "💰 Prix Moyen par Symbole",
            "description": "Prix moyen (close) et volume total agrégés par symbole (depuis ohlcv)",
            "collection": "ohlcv",
            "pipeline": [
                {"$group": {
                    "_id": "$symbol",
                    "avg_close": {"$avg": "$close"},
                    "max_close": {"$max": "$close"},
                    "min_close": {"$min": "$close"},
                    "total_volume": {"$sum": "$volume"},
                    "nb_candles": {"$sum": 1},
                }},
                {"$project": {
                    "symbol": "$_id",
                    "avg_close": {"$round": ["$avg_close", 2]},
                    "max_close": {"$round": ["$max_close", 2]},
                    "min_close": {"$round": ["$min_close", 2]},
                    "total_volume": {"$round": ["$total_volume", 2]},
                    "nb_candles": 1,
                    "_id": 0,
                }},
                {"$sort": {"symbol": 1}},
            ],
            "display": "table",
            "viz_settings": {
                "table.columns": [
                    {"name": "symbol", "enabled": True},
                    {"name": "avg_close", "enabled": True},
                    {"name": "min_close", "enabled": True},
                    {"name": "max_close", "enabled": True},
                    {"name": "total_volume", "enabled": True},
                    {"name": "nb_candles", "enabled": True},
                ],
            },
            "row": 10, "col": 12, "size_x": 6, "size_y": 5,
        },
    ]


# ─────────────────────── DATAOPS QUESTIONS ───────────────────────────────

DATAOPS_DASHBOARD_NAME = "📊 DataOps Monitoring"
DATAOPS_DASHBOARD_DESCRIPTION = (
    "Dashboard de monitoring du pipeline DataOps/MLOps : santé des services, "
    "qualité des données, fraîcheur, couverture, et métriques MLflow."
)


def get_dataops_questions_config():
    """Définit les 6 questions du dashboard DataOps Monitoring.

    Source : collection `dataops_metrics` (alimentée par collect_dataops_metrics.py).
    """
    return [
        {
            "name": "🟢 État des Services",
            "description": "Dernier état de santé de chaque service du pipeline",
            "collection": "dataops_metrics",
            "pipeline": [
                {"$sort": {"timestamp": -1}},
                {"$limit": 1},
                {"$project": {
                    "timestamp": 1,
                    "health_score": 1,
                    "services_up": 1,
                    "services_total": 1,
                    "kafka": "$services.kafka",
                    "mongodb": "$services.mongodb",
                    "spark": "$services.spark",
                    "mlflow": "$services.mlflow",
                    "airflow": "$services.airflow",
                    "metabase": "$services.metabase",
                    "fastapi": "$services.fastapi",
                    "_id": 0,
                }},
            ],
            "display": "table",
            "viz_settings": {
                "table.columns": [
                    {"name": "timestamp", "enabled": True},
                    {"name": "health_score", "enabled": True},
                    {"name": "kafka", "enabled": True},
                    {"name": "mongodb", "enabled": True},
                    {"name": "spark", "enabled": True},
                    {"name": "mlflow", "enabled": True},
                    {"name": "airflow", "enabled": True},
                    {"name": "metabase", "enabled": True},
                    {"name": "fastapi", "enabled": True},
                ],
            },
            "row": 0, "col": 0, "size_x": 18, "size_y": 3,
        },
        {
            "name": "📦 Volume par Collection",
            "description": "Nombre de documents par collection MongoDB (évolution)",
            "collection": "dataops_metrics",
            "pipeline": [
                {"$sort": {"timestamp": -1}},
                {"$limit": 50},
                {"$project": {
                    "timestamp": 1,
                    "ohlcv": "$data_volume.ohlcv",
                    "indicators": "$data_volume.indicators",
                    "anomalies": "$data_volume.anomalies",
                    "predictions": "$data_volume.predictions",
                    "total": "$data_volume.total",
                    "_id": 0,
                }},
                {"$sort": {"timestamp": 1}},
            ],
            "display": "line",
            "viz_settings": {
                "graph.dimensions": ["timestamp"],
                "graph.metrics": ["ohlcv", "indicators", "anomalies"],
            },
            "row": 3, "col": 0, "size_x": 9, "size_y": 5,
        },
        {
            "name": "⏱️ Fraîcheur des Données",
            "description": "Âge en minutes de la dernière donnée par collection",
            "collection": "dataops_metrics",
            "pipeline": [
                {"$sort": {"timestamp": -1}},
                {"$limit": 1},
                {"$project": {
                    "timestamp": 1,
                    "ohlcv_age_min": "$data_freshness.ohlcv.age_minutes",
                    "ohlcv_latest": "$data_freshness.ohlcv.latest",
                    "indicators_age_min": "$data_freshness.indicators.age_minutes",
                    "indicators_latest": "$data_freshness.indicators.latest",
                    "anomalies_age_min": "$data_freshness.anomalies.age_minutes",
                    "anomalies_latest": "$data_freshness.anomalies.latest",
                    "_id": 0,
                }},
            ],
            "display": "table",
            "viz_settings": {
                "table.columns": [
                    {"name": "timestamp", "enabled": True},
                    {"name": "ohlcv_latest", "enabled": True},
                    {"name": "ohlcv_age_min", "enabled": True},
                    {"name": "indicators_latest", "enabled": True},
                    {"name": "indicators_age_min", "enabled": True},
                    {"name": "anomalies_latest", "enabled": True},
                    {"name": "anomalies_age_min", "enabled": True},
                ],
            },
            "row": 3, "col": 9, "size_x": 9, "size_y": 5,
        },
        {
            "name": "🎯 Couverture par Symbole",
            "description": "Nombre de documents par symbole et par collection",
            "collection": "dataops_metrics",
            "pipeline": [
                {"$sort": {"timestamp": -1}},
                {"$limit": 1},
                {"$addFields": {
                    "pairs": [
                        {"symbol": "BTCUSDT", "ohlcv": "$symbol_coverage.BTCUSDT.ohlcv",
                         "indicators": "$symbol_coverage.BTCUSDT.indicators",
                         "anomalies": "$symbol_coverage.BTCUSDT.anomalies"},
                        {"symbol": "ETHUSDT", "ohlcv": "$symbol_coverage.ETHUSDT.ohlcv",
                         "indicators": "$symbol_coverage.ETHUSDT.indicators",
                         "anomalies": "$symbol_coverage.ETHUSDT.anomalies"},
                        {"symbol": "BNBUSDT", "ohlcv": "$symbol_coverage.BNBUSDT.ohlcv",
                         "indicators": "$symbol_coverage.BNBUSDT.indicators",
                         "anomalies": "$symbol_coverage.BNBUSDT.anomalies"},
                    ],
                }},
                {"$unwind": "$pairs"},
                {"$project": {
                    "symbol": "$pairs.symbol",
                    "ohlcv": "$pairs.ohlcv",
                    "indicators": "$pairs.indicators",
                    "anomalies": "$pairs.anomalies",
                    "_id": 0,
                }},
            ],
            "display": "bar",
            "viz_settings": {
                "graph.dimensions": ["symbol"],
                "graph.metrics": ["ohlcv", "indicators", "anomalies"],
            },
            "row": 8, "col": 0, "size_x": 9, "size_y": 5,
        },
        {
            "name": "🧪 Taux de Nulls OHLCV",
            "description": "Évolution du taux de nulls sur les champs critiques OHLCV",
            "collection": "dataops_metrics",
            "pipeline": [
                {"$sort": {"timestamp": -1}},
                {"$limit": 50},
                {"$project": {
                    "timestamp": 1,
                    "overall_null_pct": "$null_rates.overall_null_pct",
                    "sample_size": "$null_rates.sample_size",
                    "_id": 0,
                }},
                {"$sort": {"timestamp": 1}},
            ],
            "display": "line",
            "viz_settings": {
                "graph.dimensions": ["timestamp"],
                "graph.metrics": ["overall_null_pct"],
                "graph.y_axis.title_text": "Null Rate (%)",
            },
            "row": 8, "col": 9, "size_x": 9, "size_y": 5,
        },
        {
            "name": "🤖 Métriques MLflow",
            "description": "État MLflow : expériences, runs, modèles enregistrés et en production",
            "collection": "dataops_metrics",
            "pipeline": [
                {"$sort": {"timestamp": -1}},
                {"$limit": 1},
                {"$project": {
                    "timestamp": 1,
                    "experiments": "$mlflow.experiments",
                    "total_runs": "$mlflow.total_runs",
                    "registered_models": "$mlflow.registered_models",
                    "production_models": "$mlflow.production_models",
                    "mlflow_available": "$mlflow.available",
                    "_id": 0,
                }},
            ],
            "display": "table",
            "viz_settings": {
                "table.columns": [
                    {"name": "timestamp", "enabled": True},
                    {"name": "experiments", "enabled": True},
                    {"name": "total_runs", "enabled": True},
                    {"name": "registered_models", "enabled": True},
                    {"name": "production_models", "enabled": True},
                    {"name": "mlflow_available", "enabled": True},
                ],
            },
            "row": 13, "col": 0, "size_x": 18, "size_y": 3,
        },
    ]


# ─────────────────────── ML PERFORMANCE QUESTIONS ────────────────────────

ML_PERF_DASHBOARD_NAME = "🤖 ML Performance"
ML_PERF_DASHBOARD_DESCRIPTION = (
    "Dashboard de performance des modèles ML : métriques par run (accuracy, precision, recall, F1), "
    "comparaison XGBoost vs LSTM vs Isolation Forest, modèles en registry MLflow, et drift detection."
)


def get_ml_perf_questions_config():
    """Définit les 6 questions du dashboard ML Performance.

    Source : collection `ml_metrics` (alimentée par collect_ml_metrics.py).
    Structure du document :
      - all_runs[]:  run_id, experiment_name, model_type, f1, accuracy, precision, recall,
                     n_features, n_train_samples, start_time, symbol, task
      - registered_models[]: model_name, version, stage, status, run_id
      - health: score, production_models, best_f1, total_runs, registered_models
      - drift_results[]: run_id, drift_level, global_drift_score, drifted_features_count
    """
    return [
        {
            "name": "🏆 Meilleurs Runs ML par Modèle",
            "description": "Top runs par type de modèle (F1 décroissant) — accuracy, precision, recall, F1",
            "collection": "ml_metrics",
            "pipeline": [
                {"$sort": {"timestamp": -1}},
                {"$limit": 1},
                {"$unwind": "$all_runs"},
                {"$project": {
                    "run_name": "$all_runs.run_name",
                    "experiment": "$all_runs.experiment_name",
                    "model_type": "$all_runs.model_type",
                    "symbol": "$all_runs.symbol",
                    "f1": {"$round": ["$all_runs.f1", 4]},
                    "accuracy": {"$round": ["$all_runs.accuracy", 4]},
                    "precision": {"$round": ["$all_runs.precision", 4]},
                    "recall": {"$round": ["$all_runs.recall", 4]},
                    "n_features": "$all_runs.n_features",
                    "n_train_samples": "$all_runs.n_train_samples",
                    "start_time": "$all_runs.start_time",
                    "_id": 0,
                }},
                {"$sort": {"f1": -1}},
                {"$limit": 50},
            ],
            "display": "table",
            "viz_settings": {
                "table.columns": [
                    {"name": "model_type", "enabled": True},
                    {"name": "experiment", "enabled": True},
                    {"name": "symbol", "enabled": True},
                    {"name": "f1", "enabled": True},
                    {"name": "accuracy", "enabled": True},
                    {"name": "precision", "enabled": True},
                    {"name": "recall", "enabled": True},
                    {"name": "n_features", "enabled": True},
                    {"name": "n_train_samples", "enabled": True},
                    {"name": "start_time", "enabled": True},
                ],
            },
            "row": 0, "col": 0, "size_x": 18, "size_y": 6,
        },
        {
            "name": "⚖️ XGBoost vs LSTM — Prédiction Prix",
            "description": "Comparaison des métriques entre XGBoost et LSTM sur la tâche de prédiction de prix",
            "collection": "ml_metrics",
            "pipeline": [
                {"$sort": {"timestamp": -1}},
                {"$limit": 1},
                {"$project": {
                    "prediction_runs": {
                        "$filter": {
                            "input": "$all_runs",
                            "as": "r",
                            "cond": {"$eq": ["$$r.task", "price_prediction"]},
                        }
                    },
                    "_id": 0,
                }},
                {"$unwind": "$prediction_runs"},
                {"$project": {
                    "model_type": "$prediction_runs.model_type",
                    "f1": {"$round": ["$prediction_runs.f1", 4]},
                    "accuracy": {"$round": ["$prediction_runs.accuracy", 4]},
                    "precision": {"$round": ["$prediction_runs.precision", 4]},
                    "recall": {"$round": ["$prediction_runs.recall", 4]},
                    "_id": 0,
                }},
                {"$sort": {"f1": -1}},
                {"$limit": 20},
            ],
            "display": "bar",
            "viz_settings": {
                "graph.dimensions": ["model_type"],
                "graph.metrics": ["f1", "accuracy", "precision", "recall"],
                "graph.y_axis.title_text": "Score (0–1)",
            },
            "row": 6, "col": 0, "size_x": 9, "size_y": 5,
        },
        {
            "name": "🔍 Isolation Forest vs Autoencoder — Anomalies",
            "description": "Comparaison des métriques entre Isolation Forest et Autoencoder sur la détection d'anomalies",
            "collection": "ml_metrics",
            "pipeline": [
                {"$sort": {"timestamp": -1}},
                {"$limit": 1},
                {"$project": {
                    "anomaly_runs": {
                        "$filter": {
                            "input": "$all_runs",
                            "as": "r",
                            "cond": {"$eq": ["$$r.task", "anomaly_detection"]},
                        }
                    },
                    "_id": 0,
                }},
                {"$unwind": "$anomaly_runs"},
                {"$project": {
                    "model_type": "$anomaly_runs.model_type",
                    "f1": {"$round": ["$anomaly_runs.f1", 4]},
                    "accuracy": {"$round": ["$anomaly_runs.accuracy", 4]},
                    "precision": {"$round": ["$anomaly_runs.precision", 4]},
                    "recall": {"$round": ["$anomaly_runs.recall", 4]},
                    "_id": 0,
                }},
                {"$sort": {"f1": -1}},
                {"$limit": 20},
            ],
            "display": "bar",
            "viz_settings": {
                "graph.dimensions": ["model_type"],
                "graph.metrics": ["f1", "accuracy", "precision", "recall"],
                "graph.y_axis.title_text": "Score (0–1)",
            },
            "row": 6, "col": 9, "size_x": 9, "size_y": 5,
        },
        {
            "name": "📋 Modèles en Registry MLflow",
            "description": "Modèles enregistrés dans MLflow Model Registry avec leur version et stage",
            "collection": "ml_metrics",
            "pipeline": [
                {"$sort": {"timestamp": -1}},
                {"$limit": 1},
                {"$unwind": "$registered_models"},
                {"$project": {
                    "model_name": "$registered_models.model_name",
                    "version": "$registered_models.version",
                    "stage": "$registered_models.stage",
                    "status": "$registered_models.status",
                    "run_id": "$registered_models.run_id",
                    "_id": 0,
                }},
                {"$sort": {"model_name": 1, "version": -1}},
            ],
            "display": "table",
            "viz_settings": {
                "table.columns": [
                    {"name": "model_name", "enabled": True},
                    {"name": "version", "enabled": True},
                    {"name": "stage", "enabled": True},
                    {"name": "status", "enabled": True},
                    {"name": "run_id", "enabled": True},
                ],
            },
            "row": 11, "col": 0, "size_x": 10, "size_y": 4,
        },
        {
            "name": "🌊 Drift Detection — Résultats",
            "description": "Résultats des runs de détection de drift (niveau global, features driftées)",
            "collection": "ml_metrics",
            "pipeline": [
                {"$sort": {"timestamp": -1}},
                {"$limit": 5},
                {"$unwind": {"path": "$drift_results", "preserveNullAndEmptyArrays": True}},
                {"$project": {
                    "timestamp": 1,
                    "run_name": "$drift_results.run_name",
                    "drift_level": "$drift_results.drift_level",
                    "global_drift_score": {"$round": ["$drift_results.global_drift_score", 4]},
                    "drifted_features_count": "$drift_results.drifted_features_count",
                    "total_features": "$drift_results.total_features",
                    "ks_statistic_mean": {"$round": ["$drift_results.ks_statistic_mean", 4]},
                    "_id": 0,
                }},
            ],
            "display": "table",
            "viz_settings": {
                "table.columns": [
                    {"name": "timestamp", "enabled": True},
                    {"name": "drift_level", "enabled": True},
                    {"name": "global_drift_score", "enabled": True},
                    {"name": "drifted_features_count", "enabled": True},
                    {"name": "total_features", "enabled": True},
                    {"name": "ks_statistic_mean", "enabled": True},
                ],
            },
            "row": 11, "col": 10, "size_x": 8, "size_y": 4,
        },
        {
            "name": "💚 Score Santé ML Global",
            "description": "Score de santé ML (0-100), modèles en production, meilleur F1, total runs",
            "collection": "ml_metrics",
            "pipeline": [
                {"$sort": {"timestamp": -1}},
                {"$limit": 1},
                {"$project": {
                    "timestamp": 1,
                    "health_score": "$health.score",
                    "production_models": "$health.production_models",
                    "best_f1": "$health.best_f1",
                    "total_runs": "$health.total_runs",
                    "registered_models": "$health.registered_models",
                    "mlflow_available": 1,
                    "_id": 0,
                }},
            ],
            "display": "table",
            "viz_settings": {
                "table.columns": [
                    {"name": "timestamp", "enabled": True},
                    {"name": "health_score", "enabled": True},
                    {"name": "production_models", "enabled": True},
                    {"name": "best_f1", "enabled": True},
                    {"name": "total_runs", "enabled": True},
                    {"name": "registered_models", "enabled": True},
                    {"name": "mlflow_available", "enabled": True},
                ],
            },
            "row": 15, "col": 0, "size_x": 18, "size_y": 3,
        },
    ]


# ─────────────────────── HELPERS ─────────────────────────────────────────

def _setup_dashboard(client, db_id, dashboard_name, dashboard_desc, questions_config, label):
    """Crée un dashboard avec ses questions."""
    logger.info("")
    logger.info("─" * 40)
    logger.info(f"❓ QUESTIONS — {label}")
    logger.info("─" * 40)

    card_ids = []
    for q in questions_config:
        card_id = client.create_native_question(
            name=q["name"],
            description=q["description"],
            db_id=db_id,
            collection=q["collection"],
            pipeline=q["pipeline"],
            display=q["display"],
            viz_settings=q.get("viz_settings"),
        )
        card_ids.append({
            "card_id": card_id,
            "row": q["row"],
            "col": q["col"],
            "size_x": q["size_x"],
            "size_y": q["size_y"],
        })

    logger.info("")
    logger.info("─" * 40)
    logger.info(f"📋 DASHBOARD — {label}")
    logger.info("─" * 40)
    dash_id = client.create_dashboard(dashboard_name, dashboard_desc)
    if not dash_id:
        logger.error(f"Impossible de créer le dashboard {label}")
        return None, card_ids

    client.add_cards_to_dashboard(dash_id, card_ids)
    return dash_id, card_ids


# ─────────────────────── MAIN ────────────────────────────────────────────

def main():
    logger.info("=" * 60)
    logger.info("🚀 METABASE DASHBOARD SETUP — P5")
    logger.info("=" * 60)

    client = MetabaseClient(METABASE_URL)

    # 1. Attendre Metabase
    if not client.wait_for_metabase(timeout=120):
        logger.error("Metabase non disponible — abandon")
        sys.exit(1)

    # 2. Setup initial (premier lancement)
    client.setup_initial()

    # 3. Login
    if not client.login():
        logger.error("Login échoué — abandon")
        sys.exit(1)

    # 4. Cleanup duplicates
    logger.info("")
    logger.info("─" * 40)
    logger.info("🧹 NETTOYAGE")
    logger.info("─" * 40)
    client.cleanup_old_resources()

    # 5. Ajouter MongoDB
    logger.info("")
    logger.info("─" * 40)
    logger.info("📦 CONFIGURATION BASE DE DONNÉES")
    logger.info("─" * 40)
    db_id = client.add_mongodb_database()
    if not db_id:
        logger.error("Impossible d'ajouter MongoDB — abandon")
        sys.exit(1)

    # 6. Dashboard 1 — Crypto Market Overview
    dash1_id, cards1 = _setup_dashboard(
        client, db_id,
        DASHBOARD_NAME, DASHBOARD_DESCRIPTION,
        get_questions_config(),
        "Crypto Market",
    )

    # 7. Dashboard 2 — DataOps Monitoring
    dash2_id, cards2 = _setup_dashboard(
        client, db_id,
        DATAOPS_DASHBOARD_NAME, DATAOPS_DASHBOARD_DESCRIPTION,
        get_dataops_questions_config(),
        "DataOps Monitoring",
    )

    # 8. Dashboard 3 — ML Performance
    dash3_id, cards3 = _setup_dashboard(
        client, db_id,
        ML_PERF_DASHBOARD_NAME, ML_PERF_DASHBOARD_DESCRIPTION,
        get_ml_perf_questions_config(),
        "ML Performance",
    )

    # 9. Résumé
    logger.info("")
    logger.info("=" * 60)
    logger.info("✅ DASHBOARDS CONFIGURÉS AVEC SUCCÈS")
    logger.info("=" * 60)
    if dash1_id:
        logger.info(f"📊 Crypto Market:    {METABASE_URL}/dashboard/{dash1_id}")
    if dash2_id:
        logger.info(f"📊 DataOps Monitor:  {METABASE_URL}/dashboard/{dash2_id}")
    if dash3_id:
        logger.info(f"🤖 ML Performance:   {METABASE_URL}/dashboard/{dash3_id}")
    logger.info(f"🔗 Metabase UI: {METABASE_URL}")
    logger.info(f"👤 Login: {METABASE_EMAIL}")
    logger.info(f"🔑 Password: {METABASE_PASSWORD}")
    logger.info(f"📦 Base de données: MongoDB (id={db_id})")
    q_total = len(cards1) + len(cards2) + len(cards3)
    q_ok = sum(1 for c in cards1 + cards2 + cards3 if c["card_id"])
    logger.info(f"❓ Questions créées: {q_ok}/{q_total}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

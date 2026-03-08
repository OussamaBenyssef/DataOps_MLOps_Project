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
METABASE_FIRST_NAME = "Admin"
METABASE_LAST_NAME = "Crypto"

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
    """Client minimal pour l'API REST Metabase."""

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
        # Vérifier si le setup est déjà fait
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
        # Vérifier si la base existe déjà
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
            # Lancer le sync
            self.post(f"/database/{db_id}/sync_schema")
            logger.info("🔄 Sync du schéma lancé — attente 10s...")
            time.sleep(10)
            return db_id
        logger.error(f"❌ Erreur ajout MongoDB: {resp.text[:200]}")
        return None

    # ── Questions (Saved Questions) ──

    def create_native_question(self, name, description, db_id, collection, pipeline, display="table", viz_settings=None):
        """Crée une question MongoDB native (aggregation pipeline)."""
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

    def create_dashboard(self, name, description):
        """Crée un dashboard vide."""
        # Vérifier si le dashboard existe déjà
        resp = self.get("/dashboard")
        if resp.status_code == 200:
            for d in resp.json():
                if d.get("name") == name:
                    logger.info(f"ℹ️  Dashboard '{name}' existe déjà (id={d['id']})")
                    return d["id"]

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

    def add_card_to_dashboard(self, dashboard_id, card_id, row, col, size_x=6, size_y=4):
        """Ajoute une carte (question) à un dashboard."""
        resp = self.post(f"/dashboard/{dashboard_id}/cards", data={
            "cardId": card_id,
        })
        if resp.status_code in (200, 201):
            dashcard = resp.json()
            dashcard_id = dashcard.get("id")
            # Positionner la carte
            self.put(f"/dashboard/{dashboard_id}/cards", data={
                "cards": [{
                    "id": dashcard_id,
                    "card_id": card_id,
                    "row": row,
                    "col": col,
                    "size_x": size_x,
                    "size_y": size_y,
                }],
            })
            return dashcard_id
        return None


# ─────────────────────── QUESTIONS CONFIG ────────────────────────────────

def get_questions_config():
    """Définit les 6 questions (visualisations) du dashboard."""
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
                "graph.series_labels": {"symbol": "Symbole"},
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
            "name": "🚨 Anomalies par Type",
            "description": "Répartition des anomalies détectées par type",
            "collection": "anomalies",
            "pipeline": [
                {"$group": {
                    "_id": "$anomaly_type",
                    "count": {"$sum": 1},
                }},
                {"$project": {
                    "anomaly_type": "$_id",
                    "count": 1,
                    "_id": 0,
                }},
                {"$sort": {"count": -1}},
            ],
            "display": "pie",
            "viz_settings": {
                "pie.dimension": "anomaly_type",
                "pie.metric": "count",
            },
            "row": 5, "col": 9, "size_x": 9, "size_y": 5,
        },
        {
            "name": "⚠️ Anomalies Récentes",
            "description": "Les 50 dernières anomalies détectées (toutes sévérités)",
            "collection": "anomalies",
            "pipeline": [
                {"$sort": {"detected_at": -1}},
                {"$limit": 50},
                {"$project": {
                    "symbol": 1,
                    "anomaly_type": 1,
                    "severity": 1,
                    "value": 1,
                    "description": 1,
                    "detected_at": 1,
                    "_id": 0,
                }},
            ],
            "display": "table",
            "viz_settings": {
                "table.columns": [
                    {"name": "detected_at", "enabled": True},
                    {"name": "symbol", "enabled": True},
                    {"name": "anomaly_type", "enabled": True},
                    {"name": "severity", "enabled": True},
                    {"name": "value", "enabled": True},
                    {"name": "description", "enabled": True},
                ],
            },
            "row": 10, "col": 0, "size_x": 12, "size_y": 5,
        },
        {
            "name": "💰 Prix Moyen Journalier",
            "description": "Prix moyen (close) agrégé par jour et par symbole",
            "collection": "aggregated_metrics",
            "pipeline": [
                {"$match": {"interval": "1d"}},
                {"$sort": {"period_start": -1}},
                {"$limit": 90},
                {"$project": {
                    "symbol": 1,
                    "period_start": 1,
                    "avg_price": 1,
                    "total_volume": 1,
                    "price_range_pct": 1,
                    "_id": 0,
                }},
                {"$sort": {"period_start": 1}},
            ],
            "display": "line",
            "viz_settings": {
                "graph.dimensions": ["period_start"],
                "graph.metrics": ["avg_price"],
            },
            "row": 10, "col": 12, "size_x": 6, "size_y": 5,
        },
    ]


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

    # 4. Ajouter MongoDB
    logger.info("")
    logger.info("─" * 40)
    logger.info("📦 CONFIGURATION BASE DE DONNÉES")
    logger.info("─" * 40)
    db_id = client.add_mongodb_database()
    if not db_id:
        logger.error("Impossible d'ajouter MongoDB — abandon")
        sys.exit(1)

    # 5. Créer les questions
    logger.info("")
    logger.info("─" * 40)
    logger.info("❓ CRÉATION DES QUESTIONS")
    logger.info("─" * 40)
    questions_config = get_questions_config()
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

    # 6. Créer le dashboard
    logger.info("")
    logger.info("─" * 40)
    logger.info("📋 CRÉATION DU DASHBOARD")
    logger.info("─" * 40)
    dash_id = client.create_dashboard(DASHBOARD_NAME, DASHBOARD_DESCRIPTION)
    if not dash_id:
        logger.error("Impossible de créer le dashboard — abandon")
        sys.exit(1)

    # 7. Ajouter les cartes au dashboard
    for card_info in card_ids:
        if card_info["card_id"]:
            client.add_card_to_dashboard(
                dashboard_id=dash_id,
                card_id=card_info["card_id"],
                row=card_info["row"],
                col=card_info["col"],
                size_x=card_info["size_x"],
                size_y=card_info["size_y"],
            )

    # 8. Résumé
    logger.info("")
    logger.info("=" * 60)
    logger.info("✅ DASHBOARD CONFIGURÉ AVEC SUCCÈS")
    logger.info("=" * 60)
    logger.info(f"📊 Dashboard: {METABASE_URL}/dashboard/{dash_id}")
    logger.info(f"🔗 Metabase UI: {METABASE_URL}")
    logger.info(f"👤 Login: {METABASE_EMAIL}")
    logger.info(f"🔑 Password: {METABASE_PASSWORD}")
    logger.info(f"📦 Base de données: MongoDB (id={db_id})")
    logger.info(f"❓ Questions créées: {sum(1 for c in card_ids if c['card_id'])}/{len(card_ids)}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

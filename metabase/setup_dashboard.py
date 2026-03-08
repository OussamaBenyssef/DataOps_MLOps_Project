#!/usr/bin/env python3
"""
Metabase Dashboard Setup — (Visualisation) 
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


# Noms des dashboards 
DATAOPS_DASHBOARD_NAME = "Dashboard Métriques DataOps"
DATAOPS_DASHBOARD_DESCRIPTION = (
    "Suivi de la qualité et de la fraîcheur des données: volumétrie, couverture, "
    "dispersion des prix et répartition des anomalies."
)

# Dashboard dédié au suivi opérationnel des modèles ML.
ML_DASHBOARD_NAME = "Dashboard Performance ML"
ML_DASHBOARD_DESCRIPTION = (
    "Suivi des sorties modèles: distribution des prédictions, score de confiance "
    "et taux d'anomalies par symbole."
)

# Dashboard crypto global demandé dans la tâche 6. 
CRYPTO_DASHBOARD_NAME = "Dashboard Crypto Metabase"
CRYPTO_DASHBOARD_DESCRIPTION = (
    "Vue marché crypto: prix, volumes et anomalies détectées sur les données "
    "ingérées dans MongoDB."
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

def get_dataops_questions_config():
    """Construit les cartes du dashboard de métriques DataOps (tâche 4)."""
    return [
        {
            # Carte de volumétrie des enregistrements agrégés par symbole.
            "name": "DataOps - Volumétrie par symbole",
            "description": "Nombre de fenêtres agrégées calculées par symbole et intervalle",
            "collection": "aggregated_metrics",
            "pipeline": [
                {"$group": {
                    "_id": {"symbol": "$symbol", "interval": "$interval"},
                    "nb_fenetres": {"$sum": 1},
                }},
                {"$project": {
                    "symbol": "$_id.symbol",
                    "interval": "$_id.interval",
                    "nb_fenetres": 1,
                    "_id": 0,
                }},
                {"$sort": {"nb_fenetres": -1}},
            ],
            "display": "bar",
            "viz_settings": {
                "graph.dimensions": ["symbol"],
                "graph.metrics": ["nb_fenetres"],
            },
            "row": 0, "col": 0, "size_x": 8, "size_y": 5,
        },
        {
            # Carte de fraîcheur pour suivre le dernier timestamp ingéré.
            "name": "DataOps - Fraîcheur des données",
            "description": "Dernier timestamp et ancienneté des données par symbole",
            "collection": "ohlcv",
            "pipeline": [
                {"$group": {
                    "_id": "$symbol",
                    "dernier_point": {"$max": "$timestamp"},
                    "lignes": {"$sum": 1},
                }},
                {"$project": {
                    "symbol": "$_id",
                    "dernier_point": 1,
                    "lignes": 1,
                    "_id": 0,
                }},
                {"$sort": {"dernier_point": -1}},
            ],
            "display": "table",
            "viz_settings": {
                "table.columns": [
                    {"name": "symbol", "enabled": True},
                    {"name": "dernier_point", "enabled": True},
                    {"name": "lignes", "enabled": True},
                ],
            },
            "row": 0, "col": 8, "size_x": 10, "size_y": 5,
        },
        {
            # Carte de dispersion prix pour suivre la variabilité de marché.
            "name": "DataOps - Volatilité moyenne",
            "description": "Volatilité moyenne agrégée par symbole",
            "collection": "aggregated_metrics",
            "pipeline": [
                {"$group": {
                    "_id": "$symbol",
                    "volatilite_moyenne": {"$avg": "$price_volatility"},
                    "plage_prix_moyenne": {"$avg": "$price_range_pct"},
                }},
                {"$project": {
                    "symbol": "$_id",
                    "volatilite_moyenne": {"$round": ["$volatilite_moyenne", 6]},
                    "plage_prix_moyenne": {"$round": ["$plage_prix_moyenne", 4]},
                    "_id": 0,
                }},
                {"$sort": {"volatilite_moyenne": -1}},
            ],
            "display": "bar",
            "viz_settings": {
                "graph.dimensions": ["symbol"],
                "graph.metrics": ["volatilite_moyenne"],
            },
            "row": 5, "col": 0, "size_x": 9, "size_y": 5,
        },
        {
            # Carte de qualité orientée incidents/anomalies détectées.
            "name": "DataOps - Taux d'anomalies",
            "description": "Nombre d'anomalies détectées par symbole",
            "collection": "anomalies",
            "pipeline": [
                {"$group": {
                    "_id": "$symbol",
                    "nb_anomalies": {"$sum": 1},
                }},
                {"$project": {
                    "symbol": "$_id",
                    "nb_anomalies": 1,
                    "_id": 0,
                }},
                {"$sort": {"nb_anomalies": -1}},
            ],
            "display": "bar",
            "viz_settings": {
                "graph.dimensions": ["symbol"],
                "graph.metrics": ["nb_anomalies"],
            },
            "row": 5, "col": 9, "size_x": 9, "size_y": 5,
        },
    ]


def get_ml_performance_questions_config():
    """Construit les cartes du dashboard Performance ML (tâche 5)."""
    return [
        {
            # Carte de distribution des classes prédites par modèle.
            "name": "ML - Distribution des prédictions",
            "description": "Nombre de prédictions UP/DOWN par symbole",
            "collection": "predictions",
            "pipeline": [
                {"$group": {
                    "_id": {"symbol": "$symbol", "prediction": "$prediction"},
                    "total": {"$sum": 1},
                }},
                {"$project": {
                    "symbol": "$_id.symbol",
                    "prediction": "$_id.prediction",
                    "total": 1,
                    "_id": 0,
                }},
                {"$sort": {"total": -1}},
            ],
            "display": "bar",
            "viz_settings": {
                "graph.dimensions": ["prediction"],
                "graph.metrics": ["total"],
            },
            "row": 0, "col": 0, "size_x": 8, "size_y": 5,
        },
        {
            # Carte de confiance moyenne des scores produits par le modèle.
            "name": "ML - Confiance moyenne",
            "description": "Moyenne des probabilités de prédiction par symbole",
            "collection": "predictions",
            "pipeline": [
                {"$match": {"prediction_probability": {"$ne": None}}},
                {"$group": {
                    "_id": "$symbol",
                    "confiance_moyenne": {"$avg": "$prediction_probability"},
                    "nb_predictions": {"$sum": 1},
                }},
                {"$project": {
                    "symbol": "$_id",
                    "confiance_moyenne": {"$round": ["$confiance_moyenne", 4]},
                    "nb_predictions": 1,
                    "_id": 0,
                }},
                {"$sort": {"confiance_moyenne": -1}},
            ],
            "display": "bar",
            "viz_settings": {
                "graph.dimensions": ["symbol"],
                "graph.metrics": ["confiance_moyenne"],
            },
            "row": 0, "col": 8, "size_x": 10, "size_y": 5,
        },
        {
            # Carte de rythme de production des prédictions dans le temps.
            "name": "ML - Débit des prédictions",
            "description": "Nombre de prédictions produites par heure",
            "collection": "predictions",
            "pipeline": [
                {"$project": {
                    "prediction_hour": {"$dateTrunc": {"date": "$created_at", "unit": "hour"}},
                    "_id": 0,
                }},
                {"$group": {
                    "_id": "$prediction_hour",
                    "nb_predictions": {"$sum": 1},
                }},
                {"$project": {
                    "prediction_hour": "$_id",
                    "nb_predictions": 1,
                    "_id": 0,
                }},
                {"$sort": {"prediction_hour": 1}},
            ],
            "display": "line",
            "viz_settings": {
                "graph.dimensions": ["prediction_hour"],
                "graph.metrics": ["nb_predictions"],
            },
            "row": 5, "col": 0, "size_x": 9, "size_y": 5,
        },
        {
            # Carte de corrélation simple entre prédictions et anomalies observées.
            "name": "ML - Anomalies par symbole",
            "description": "Nombre d'anomalies détectées pour contextualiser les performances ML",
            "collection": "anomalies",
            "pipeline": [
                {"$group": {
                    "_id": "$symbol",
                    "nb_anomalies": {"$sum": 1},
                }},
                {"$project": {
                    "symbol": "$_id",
                    "nb_anomalies": 1,
                    "_id": 0,
                }},
                {"$sort": {"nb_anomalies": -1}},
            ],
            "display": "table",
            "viz_settings": {
                "table.columns": [
                    {"name": "symbol", "enabled": True},
                    {"name": "nb_anomalies", "enabled": True},
                ],
            },
            "row": 5, "col": 9, "size_x": 9, "size_y": 5,
        },
    ]


def build_dashboard_bundle():
    """Assemble la liste des dashboards à provisionner dans Metabase.""" 
    return [
        {
            "name": DATAOPS_DASHBOARD_NAME,
            "description": DATAOPS_DASHBOARD_DESCRIPTION,
            "questions": get_dataops_questions_config(),
        },
        {
            "name": ML_DASHBOARD_NAME,
            "description": ML_DASHBOARD_DESCRIPTION,
            "questions": get_ml_performance_questions_config(),
        },
        {
            "name": CRYPTO_DASHBOARD_NAME,
            "description": CRYPTO_DASHBOARD_DESCRIPTION,
            "questions": get_questions_config(),
        },
    ]


def create_dashboard_from_config(client, db_id, dashboard_config):
    """Crée un dashboard complet à partir d'une configuration déclarative."""
    logger.info("")
    logger.info("─" * 40)
    logger.info(f"📋 DASHBOARD: {dashboard_config['name']}")
    logger.info("─" * 40)

    # Création des questions du dashboard courant.
    card_ids = []
    for question in dashboard_config["questions"]:
        card_id = client.create_native_question(
            name=question["name"],
            description=question["description"], 
            db_id=db_id,
            collection=question["collection"],
            pipeline=question["pipeline"],
            display=question["display"],
            viz_settings=question.get("viz_settings"),
        )
        card_ids.append({
            "card_id": card_id,
            "row": question["row"],
            "col": question["col"],
            "size_x": question["size_x"],
            "size_y": question["size_y"],
        })

    # Création du dashboard puis association des cartes.
    dash_id = client.create_dashboard(dashboard_config["name"], dashboard_config["description"])
    if not dash_id:
        logger.error(f"Impossible de créer le dashboard: {dashboard_config['name']}")
        return None, card_ids

    client.add_cards_to_dashboard(dash_id, card_ids)
    return dash_id, card_ids


# ─────────────────────── MAIN ────────────────────────────────────────────

def main():
    logger.info("=" * 60)
    logger.info("🚀 METABASE DASHBOARD SETUP") 
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

    # 6. Créer les dashboards P5 (DataOps, Performance ML, Crypto).
    logger.info("")
    logger.info("─" * 40)
    logger.info("📋 CRÉATION DU DASHBOARD")
    logger.info("─" * 40)
     dashboards_created = []
    cards_created = 0
    cards_total = 0
    for dashboard in build_dashboard_bundle():
      dash_id, card_ids = create_dashboard_from_config(client, db_id, dashboard)
      dashboards_created.append({"name": dashboard["name"], "id": dash_id})
      cards_created += sum(1 for card in card_ids if card["card_id"])
      cards_total += len(card_ids)


    # 7. Résumé
    logger.info("")
    logger.info("=" * 60)
    logger.info("✅ DASHBOARD CONFIGURÉ AVEC SUCCÈS")
    logger.info("=" * 60)
    for dashboard_info in dashboards_created:
      logger.info(f"📊 Dashboard {dashboard_info['name']}: {METABASE_URL}/dashboard/{dashboard_info['id']}")
    logger.info(f"🔗 Metabase UI: {METABASE_URL}")
    logger.info(f"👤 Login: {METABASE_EMAIL}")
    logger.info(f"🔑 Password: {METABASE_PASSWORD}")
    logger.info(f"📦 Base de données: MongoDB (id={db_id})")
    logger.info(f"❓ Questions créées: {cards_created}/{cards_total}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

# 📊 Metabase Dashboards — Crypto Market & DataOps

Dashboards de visualisation connectés à MongoDB pour le projet DataOps/MLOps.

## Lancement

```bash
# 1. Démarrer les services Docker
cd docker && docker compose up -d metabase mongodb

# 2. Attendre ~30s que Metabase démarre, puis :

# 3. Collecter les métriques DataOps (optionnel, pour alimenter le dashboard DataOps)
python metabase/collect_dataops_metrics.py

# 4. Configurer les dashboards
python metabase/setup_dashboard.py
```

## Accès

| Service | URL | Credentials |
|---------|-----|-------------|
| **Metabase** | http://localhost:3000 | `admin@crypto.local` / `DataMLOps2024!` |
| **Dashboard Crypto** | http://localhost:3000/dashboard/34 | — |
| **Dashboard DataOps** | http://localhost:3000/dashboard/35 | — |

## Dashboard 1 — 🚀 Crypto Market Overview

| # | Carte | Source | Type |
|---|-------|--------|------|
| 1 | 📈 Prix de Clôture (24h) | `ohlcv` | Ligne |
| 2 | 📊 Volume de Trading (24h) | `ohlcv` | Barres |
| 3 | 📉 RSI-14 par Symbole | `indicators` | Ligne |
| 4 | 🚨 Anomalies Détectées | `anomalies` | Barres |
| 5 | ⚠️ Anomalies Récentes | `anomalies` | Table |
| 6 | 💰 Prix Moyen par Symbole | `ohlcv` | Table |

## Dashboard 2 — 📊 DataOps Monitoring

| # | Carte | Source | Type |
|---|-------|--------|------|
| 1 | 🟢 État des Services | `dataops_metrics` | Table |
| 2 | 📦 Volume par Collection | `dataops_metrics` | Ligne |
| 3 | ⏱️ Fraîcheur des Données | `dataops_metrics` | Table |
| 4 | 🎯 Couverture par Symbole | `dataops_metrics` | Barres |
| 5 | 🧪 Taux de Nulls OHLCV | `dataops_metrics` | Ligne |
| 6 | 🤖 Métriques MLflow | `dataops_metrics` | Table |

### Collecte des métriques

Le script `collect_dataops_metrics.py` collecte les métriques opérationnelles et les persiste
dans MongoDB (`dataops_metrics` collection avec TTL de 30 jours) :

```bash
# Exécution manuelle
python metabase/collect_dataops_metrics.py

# Via cron (toutes les 5 min) — optionnel
*/5 * * * * cd /path/to/project && python metabase/collect_dataops_metrics.py
```

## Ajouter une visualisation

1. Ouvrir Metabase → **New Question** → **Native Query**
2. Sélectionner database `CryptoMarket MongoDB`
3. Écrire un pipeline d'agrégation MongoDB
4. Sauvegarder → Ajouter au dashboard

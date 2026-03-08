# 📊 Metabase Dashboard — Crypto Market

Dashboard de visualisation du marché crypto connecté à MongoDB.

## Lancement

```bash
# 1. Démarrer les services Docker
cd docker && docker compose up -d metabase mongodb

# 2. Attendre ~30s que Metabase démarre, puis :
python metabase/setup_dashboard.py
```

## Accès

| Service | URL | Credentials |
|---------|-----|-------------|
| **Metabase** | http://localhost:3000 | `admin@crypto.local` / `DataMLOps2024!` |
| **Dashboard** | http://localhost:3000/dashboard/1 | — |

## Visualisations

| # | Carte | Source | Type |
|---|-------|--------|------|
| 1 | 📈 Prix de Clôture (24h) | `ohlcv` | Ligne |
| 2 | 📊 Volume de Trading (24h) | `ohlcv` | Barres |
| 3 | 📉 RSI-14 par Symbole | `indicators` | Ligne |
| 4 | 🚨 Anomalies par Type | `anomalies` | Camembert |
| 5 | ⚠️ Anomalies Récentes | `anomalies` | Table |
| 6 | 💰 Prix Moyen Journalier | `aggregated_metrics` | Ligne |

## Ajouter une visualisation

1. Ouvrir Metabase → **New Question** → **Native Query**
2. Sélectionner database `CryptoMarket MongoDB`
3. Écrire un pipeline d'agrégation MongoDB
4. Sauvegarder → Ajouter au dashboard

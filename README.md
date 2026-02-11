# 📊 DataMLOps — Analyse Crypto Binance en Temps Réel

Pipeline DataOps/MLOps complet pour l'analyse de données crypto Binance : streaming temps réel, détection d'anomalies, prédiction de prix et monitoring de drift.

## 🏗️ Architecture

```
Binance API  →  Kafka  →  Spark (ETL)  →  MongoDB
                                              ↓
                              MLflow ← ML Models → FastAPI
                                              ↓
                              Metabase (Dashboards) + DataHub (Catalog)
```

**Orchestration** : Apache Airflow

## 🧑‍💻 Équipe

| Rôle | Membre | Responsabilités |
|------|--------|-----------------|
| P1 Chef de Projet / DevOps | oussama | Infra, Airflow, Tests, CI/CD |
| P2 Kafka Engineer | Mouad | WebSocket Binance, Kafka Producer/Consumer |
| P3 Spark Engineer | issam | ETL Spark, MongoDB, Indicateurs techniques |
| P4 ML Engineer | Abdessamad | Modèles ML, MLflow, FastAPI, Drift |
| P5 Data Analyst | lahoussine | DataHub, Dashboards Metabase |

## 🛠️ Stack Technique

| Composant | Technologie | Port |
|-----------|-------------|------|
| Streaming | Apache Kafka | 9092, 29092 |
| Processing | Apache Spark | 8080 (UI) |
| Storage | MongoDB 7.0 | 27017 |
| ML Tracking | MLflow | 5001 |
| API | FastAPI | 8000 |
| Orchestration | Apache Airflow | 8081 |
| Dashboards | Metabase | 3000 |
| Data Catalog | DataHub | 9002 |
| Metadata DB | PostgreSQL | 5432 |

## 🚀 Setup

### Prérequis
- Docker & Docker Compose
- Python 3.11+
- Git

### Lancement

```bash
# 1. Cloner le repo
git clone https://github.com/OussamaBenyssef/DataOps_MLOps_Project.git
cd DataOps_MLOps_Project

# 2. Installer les dépendances Python
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 3. Lancer l'infrastructure
cd docker
docker-compose up -d

# 4. Vérifier les services
docker-compose ps
```

## 📁 Structure du Projet

```
├── dags/                  # DAGs Airflow
├── docker/
│   ├── docker-compose.yml # Infrastructure complète
│   ├── Dockerfile.api     # Image FastAPI
│   └── start_datahub.sh   # Script démarrage DataHub
├── mongodb/
│   └── init.js            # Init collections crypto
├── src/
│   ├── ingestion/         # Binance WebSocket + Kafka Producer
│   ├── processing/        # Spark ETL + indicateurs techniques
│   ├── ml/                # Modèles ML + API FastAPI
│   └── catalog/           # DataHub ingestion
├── tests/                 # Tests unitaires + intégration
├── docs/                  # Documentation détaillée
├── requirements.txt       # Dépendances Python
└── .gitignore
```

## 🔀 Git Workflow

```
main ← develop ← feature/pX-task-name
```

- **Branches** : `feature/pX-nom-tache` depuis `develop`
- **Commits** : `feat(scope): description` / `fix(scope): description`
- **PR** : Review obligatoire par P1 avant merge

## 📅 Timeline

| Période | Objectif |
|---------|----------|
| S1 (11-21 Fév) | Ingestion Binance + ETL Spark |
| S2 (22-28 Fév) | Orchestration Airflow + ML Prep |
| S3 (1-7 Mars) | Modèles ML + MLflow + API |
| S4 (8-10 Mars) | Dashboards + DataHub + Docs |
| 10-13 Mars | **Soutenance** |

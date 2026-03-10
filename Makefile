# ══════════════════════════════════════════════════════
# Makefile — DataOps/MLOps Pipeline
# Usage: make <target>
# ══════════════════════════════════════════════════════

DOCKER_COMPOSE := docker compose -f docker/docker-compose.yml
# Utilise le venv du projet si disponible, sinon python3 système
PYTHON         := $(shell [ -f .venv/bin/python3 ] && echo ".venv/bin/python3" || echo "python3")

.PHONY: help lint format test test-dags test-integration \
        docker-up docker-down docker-ps docker-logs \
        health status backfill logs clean

# ─────────────────────────────────────────────────────
# Aide
# ─────────────────────────────────────────────────────
help: ## Affiche cette aide
	@echo ""
	@echo "╔══════════════════════════════════════════════════╗"
	@echo "║       DataOps/MLOps Pipeline — Makefile          ║"
	@echo "╚══════════════════════════════════════════════════╝"
	@echo ""
	@echo "  📋 QUALITÉ CODE"
	@grep -E '^(lint|format|test|test-dags|test-integration):.*##' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "    \033[36m%-22s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "  🐳 DOCKER"
	@grep -E '^(docker-up|docker-down|docker-ps|docker-logs):.*##' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "    \033[36m%-22s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "  🩺 SUPERVISION"
	@grep -E '^(health|status|logs):.*##' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "    \033[36m%-22s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "  ⚙️  OPÉRATIONS"
	@grep -E '^(backfill|clean):.*##' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "    \033[36m%-22s\033[0m %s\n", $$1, $$2}'
	@echo ""

# ─────────────────────────────────────────────────────
# Qualité Code
# ─────────────────────────────────────────────────────
lint: ## Vérifie le style du code (flake8 + black --check)
	@echo "🔍 flake8..."
	@flake8 src/ dags/ --max-line-length=120 --extend-ignore=E203,W503,E501 \
		--exclude=src/ml/api/,__pycache__,.venv --statistics
	@echo "🔍 black --check..."
	@black --check --line-length=120 src/ dags/ --exclude='/(\.venv|__pycache__|\.git)/'
	@echo "✅ Lint OK"

format: ## Formate automatiquement le code avec black
	@echo "✏️  black format..."
	@black --line-length=120 src/ dags/ --exclude='/(\.venv|__pycache__|\.git)/'
	@echo "✅ Code formaté"

test: ## Lance les tests unitaires (sans services externes ni PySpark)
	@echo "🧪 pytest unit tests..."
	@pytest tests/ \
		-m "not integration and not integration_pipeline" \
		--ignore=tests/test_aggregated_metrics.py \
		--ignore=tests/test_spark_processing.py \
		--ignore=tests/test_integration_kafka_spark_mongodb.py \
		--ignore=tests/test_real_data_e2e.py \
		-v --tb=short --no-header
	@echo "✅ Tests OK"

test-spark: ## Lance les tests Spark (nécessite PySpark installé ou Docker)
	@echo "⚡ pytest Spark tests (nécessite PySpark)..."
	@pytest tests/test_aggregated_metrics.py \
		     tests/test_spark_processing.py \
		-m "not integration_pipeline" \
		-v --tb=short --no-header
	@echo "✅ Tests Spark OK"

test-dags: ## Valide la structure et les task_ids des 6 DAGs Airflow
	@echo "✈️  Validation des DAGs..."
	@pytest tests/test_dags.py -v --tb=short --no-header
	@echo "✅ DAGs validés"

test-integration: ## Lance les tests d'intégration (nécessite Docker)
	@echo "🔗 pytest integration tests (nécessite Docker up)..."
	@pytest tests/ -m "integration" -v --tb=short
	@echo "✅ Tests intégration OK"

# ─────────────────────────────────────────────────────
# Docker
# ─────────────────────────────────────────────────────
docker-up: ## Démarre toute la stack Docker en arrière-plan
	@echo "🚀 Démarrage de la stack..."
	@$(DOCKER_COMPOSE) up -d
	@echo "✅ Stack démarrée"
	@echo "   Airflow  → http://localhost:8081  (admin/admin)"
	@echo "   MLflow   → http://localhost:5001"
	@echo "   Spark    → http://localhost:8080"
	@echo "   Metabase → http://localhost:3000"
	@echo "   DataHub  → http://localhost:9002"
	@echo "   FastAPI  → http://localhost:8000/docs"

docker-down: ## Arrête toute la stack Docker
	@echo "🛑 Arrêt de la stack..."
	@$(DOCKER_COMPOSE) down
	@echo "✅ Stack arrêtée"

docker-ps: ## Affiche le statut des containers
	@$(DOCKER_COMPOSE) ps

docker-logs: ## Affiche les logs des containers (Ctrl+C pour quitter)
	@$(DOCKER_COMPOSE) logs -f airflow-scheduler spark-master ml-trainer

# ─────────────────────────────────────────────────────
# Supervision
# ─────────────────────────────────────────────────────
health: ## Vérifie la santé de tous les services
	@chmod +x scripts/health_check.sh
	@./scripts/health_check.sh

status: ## Affiche le tableau de bord du pipeline (nécessite Docker up)
	@$(PYTHON) scripts/pipeline_status.py

logs: ## Affiche les logs Airflow scheduler + Spark + ML en temps réel
	@echo "📋 Logs en temps réel (Ctrl+C pour quitter)..."
	@$(DOCKER_COMPOSE) logs -f --tail=50 airflow-scheduler spark-master ml-trainer fastapi

# ─────────────────────────────────────────────────────
# Opérations
# ─────────────────────────────────────────────────────
backfill: ## Lance le DAG backfill via l'API Airflow REST
	@echo "📦 Déclenchement du DAG crypto_backfill..."
	@curl -s -X POST \
		"http://localhost:8081/api/v1/dags/crypto_backfill/dagRuns" \
		-H "Content-Type: application/json" \
		-u admin:admin \
		-d '{"conf": {"start_date": "2025-01-01", "end_date": "2025-03-01"}}' \
		| python3 -m json.tool || echo "❌ Erreur: vérifiez que Airflow est démarré (make docker-up)"

clean: ## Nettoie les caches Python locaux
	@echo "🧹 Nettoyage..."
	@find . -type d -name "__pycache__" ! -path "./.venv/*" -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name ".pytest_cache" ! -path "./.venv/*" -exec rm -rf {} + 2>/dev/null || true
	@find . -name "*.pyc" ! -path "./.venv/*" -delete 2>/dev/null || true
	@echo "✅ Nettoyage terminé"

#!/bin/bash
# Script pour démarrer uniquement la pile DataHub déclarée dans docker/docker-compose.yml.
# Responsable: Manager
# Usage: ./start_datahub.sh

set -e

echo "🚀 Démarrage des services DataHub à partir de docker-compose..."
echo ""

# Accédez au répertoire où se trouve ce script.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

COMPOSE_FILE="docker-compose.yml"
REQUIRED_SERVICES=(
  kafka
  mongodb
  datahub-mysql
  datahub-elasticsearch
  datahub-prerequisites
  datahub-gms
  datahub-frontend
  datahub-actions
)

# Check Docker availability before running compose commands.
if ! command -v docker >/dev/null 2>&1; then
  echo "❌ La CLI Docker n'est pas installée. Veuillez installer Docker et réessayer."
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "❌ Le démon Docker n'est pas en cours d'exécution. Veuillez démarrer Docker et réessayer."
  exit 1
fi

# Privilégier la syntaxe moderne de composition
if docker compose version >/dev/null 2>&1; then
  COMPOSE_CMD="docker compose"
  echo "✅ Utilisation du plugin Docker Compose v2."
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE_CMD="docker-compose"
  echo "⚠️  Utilisation du binaire docker-compose hérité."
else
  echo "❌ Ni 'docker compose' ni 'docker-compose' ne sont disponibles."
  exit 1
fi

# Démarrer les services DataHub
$COMPOSE_CMD -f "$COMPOSE_FILE" up -d "${REQUIRED_SERVICES[@]}"

echo ""
echo "✅ Services DataHub démarrés!"
echo ""
echo "🔗 Accès:"
echo "   - DataHub UI:  http://localhost:9002"
echo "   - Credentials: datahub / datahub"
echo "   - API GMS:     http://localhost:8082"
echo ""
echo "📊 Commandes d'ingestion (une fois les services opérationnels) :"
echo "   docker exec datahub-actions datahub ingest -c /etc/datahub/recipes/mongodb_recipe.yml"
echo "   docker exec datahub-actions datahub ingest -c /etc/datahub/recipes/kafka_recipe.yml"

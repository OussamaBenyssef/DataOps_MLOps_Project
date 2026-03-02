#!/bin/bash
# Script pour démarrer uniquement la pile DataHub déclarée dans docker/docker-compose.yml.
# Usage: ./start_datahub.sh

set -e

echo "Démarrage des services DataHub à partir de docker-compose..."

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
  echo "La CLI Docker n'est pas installée. Veuillez installer Docker et réessayer."
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "Le démon Docker n'est pas en cours d'exécution. Veuillez démarrer Docker et réessayer."
  exit 1
fi

# Privilégier la syntaxe moderne de composition, revenir à l'ancienne syntaxe docker-compose si nécessaire.
if docker compose version >/dev/null 2>&1; then
  COMPOSE_CMD="docker compose"
  echo "Utilisation du plugin Docker Compose v2 (docker compose)."
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE_CMD="docker-compose"
  echo "Utilisation du binaire docker-compose hérité."
  echo "Warning : le plugin Compose v2 est recommandé dans la mesure du possible."
else
  echo "Ni 'docker compose' ni 'docker-compose' ne sont disponibles."
  exit 1
fi

# Vérifiez que les services requis existent dans le fichier compose avant le démarrage.
AVAILABLE_SERVICES=$(awk '
  /^services:/ { in_services=1; next }
  in_services && /^[^[:space:]]/ { in_services=0 }
  in_services && /^  [a-zA-Z0-9_.-]+:/ {
    service=$1
    sub(":$", "", service)
    sub(/^  /, "", service)
    print service
  }
' "$COMPOSE_FILE")

for required_service in "${REQUIRED_SERVICES[@]}"; do
  if ! printf '%s\n' "$AVAILABLE_SERVICES" | grep -qx "$required_service"; then
    echo "Service requis manquant '$required_service' dans $COMPOSE_FILE."
    echo "Veuillez aligner les noms des services dans le fichier compose avant d'exécuter ce script."
    exit 1
  fi
done

# Démarrez les dépendances et les services principaux de DataHub à partir de docker/docker-compose.yml.
$COMPOSE_CMD -f "$COMPOSE_FILE" up -d "${REQUIRED_SERVICES[@]}"

echo ""

echo "Les services DataHub démarrent."
echo "Interface utilisateur DataHub : http://localhost:9002"
echo "API GMS :   http://localhost:8082"

echo ""

echo "Commandes d'ingestion (à exécuter une fois que les services sont opérationnels) :"
echo "docker exec -it datahub-actions datahub ingest -c /etc/datahub/recipes/mongodb_recipe.yml"
echo "docker exec -it datahub-actions datahub ingest -c /etc/datahub/recipes/kafka_recipe.yml"

echo ""

if docker exec datahub-actions sh -lc 'command -v datahub >/dev/null 2>&1'; then
  echo "DataHub CLI est disponible dans datahub-actions."
else
  echo "Avertissement : DataHub CLI introuvable dans le conteneur datahub-actions."
  echo "Installez-le dans le conteneur avant l'ingestion, par exemple :"
  echo "docker exec -it datahub-actions pip install acryl-datahub"
fi
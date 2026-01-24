#!/bin/bash
# Script pour démarrer DataHub séparément avec le quickstart officiel
# Responsable: Manager
# Usage: ./start_datahub.sh

echo "🚀 Démarrage de DataHub avec le quickstart officiel..."
echo ""

# Vérifier que Docker est en cours d'exécution
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker n'est pas en cours d'exécution. Démarrez Docker Desktop d'abord."
    exit 1
fi

# Vérifier que datahub CLI est installé
if ! command -v datahub &> /dev/null; then
    echo "📦 Installation de DataHub CLI..."
    pip install acryl-datahub
fi

# Démarrer DataHub avec le quickstart
echo "📥 Téléchargement et démarrage de DataHub..."
datahub docker quickstart --kafka-setup

echo ""
echo "✅ DataHub démarré!"
echo ""
echo "🔗 Accès:"
echo "   - DataHub UI: http://localhost:9002"
echo "   - Credentials: datahub / datahub"
echo ""
echo "📊 Pour ingérer les métadonnées du projet:"
echo "   cd .. && datahub ingest -c datahub/recipes/mongodb_recipe.yml"
echo "   cd .. && datahub ingest -c datahub/recipes/kafka_recipe.yml"

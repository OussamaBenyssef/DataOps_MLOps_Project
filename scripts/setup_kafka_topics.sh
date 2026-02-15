#!/bin/bash
# ============================================
# Setup Kafka Topics - Projet Crypto Binance
# ============================================
# Usage: ./setup_kafka_topics.sh
# Prérequis: docker-compose up -d (Kafka doit être running)

set -e

KAFKA_CONTAINER="kafka"
BOOTSTRAP_SERVER="localhost:9092"

echo "╔═══════════════════════════════════════════════════╗"
echo "║     Configuration Topics Kafka - Crypto Binance    ║"
echo "╚═══════════════════════════════════════════════════╝"
echo ""

# Vérifier que Kafka est running
echo "🔍 Vérification de Kafka..."
if ! docker exec $KAFKA_CONTAINER kafka-broker-api-versions --bootstrap-server $BOOTSTRAP_SERVER > /dev/null 2>&1; then
    echo "❌ Kafka n'est pas accessible. Lancez d'abord: docker-compose up -d"
    exit 1
fi
echo "✅ Kafka est opérationnel"
echo ""

# ============================================
# CRÉATION DES TOPICS
# ============================================

# Topic 1: raw_trades
# Données brutes des trades WebSocket Binance
# 3 partitions pour parallélisme (BTC, ETH, BNB)
# Rétention: 24h (données brutes temporaires)
echo "📦 Création topic: raw_trades"
docker exec $KAFKA_CONTAINER kafka-topics --create \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --topic raw_trades \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=86400000 \
    --config cleanup.policy=delete \
    --if-not-exists
echo "   ✅ raw_trades créé (3 partitions, rétention 24h)"

# Topic 2: raw_klines
# Candlesticks/OHLCV bruts depuis Binance
# 3 partitions (par paire de trading)
# Rétention: 7 jours (historique court terme)
echo "📦 Création topic: raw_klines"
docker exec $KAFKA_CONTAINER kafka-topics --create \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --topic raw_klines \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=604800000 \
    --config cleanup.policy=delete \
    --if-not-exists
echo "   ✅ raw_klines créé (3 partitions, rétention 7j)"

# Topic 3: processed_data
# Données transformées par Spark (indicateurs techniques, métriques)
# 3 partitions
# Rétention: 7 jours
echo "📦 Création topic: processed_data"
docker exec $KAFKA_CONTAINER kafka-topics --create \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --topic processed_data \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=604800000 \
    --config cleanup.policy=delete \
    --if-not-exists
echo "   ✅ processed_data créé (3 partitions, rétention 7j)"

# Topic 4: anomalies
# Anomalies détectées (alertes prix, volume spikes)
# 1 partition (volume faible, ordre garanti)
# Rétention: 30 jours (garder l'historique des alertes)
echo "📦 Création topic: anomalies"
docker exec $KAFKA_CONTAINER kafka-topics --create \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --topic anomalies \
    --partitions 1 \
    --replication-factor 1 \
    --config retention.ms=2592000000 \
    --config cleanup.policy=delete \
    --if-not-exists
echo "   ✅ anomalies créé (1 partition, rétention 30j)"

echo ""
echo "════════════════════════════════════════════════════"
echo ""

# ============================================
# VÉRIFICATION
# ============================================
echo "📋 Liste des topics créés:"
echo ""
docker exec $KAFKA_CONTAINER kafka-topics --list \
    --bootstrap-server $BOOTSTRAP_SERVER

echo ""
echo "════════════════════════════════════════════════════"
echo ""

# Détails de chaque topic
for topic in raw_trades raw_klines processed_data anomalies; do
    echo "📊 Détails: $topic"
    docker exec $KAFKA_CONTAINER kafka-topics --describe \
        --bootstrap-server $BOOTSTRAP_SERVER \
        --topic $topic
    echo ""
done

echo "════════════════════════════════════════════════════"
echo "✅ Configuration Kafka terminée!"
echo ""
echo "Topics créés:"
echo "  📈 raw_trades     (3 parts, 24h)  ← WebSocket trades"
echo "  📊 raw_klines     (3 parts, 7j)   ← OHLCV candlesticks"
echo "  🔄 processed_data (3 parts, 7j)   ← Spark ETL output"
echo "  🚨 anomalies      (1 part, 30j)   ← Alertes détectées"
echo "════════════════════════════════════════════════════"

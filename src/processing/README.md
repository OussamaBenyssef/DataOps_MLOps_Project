# Spark Processing Module - P3

Module de traitement temps réel avec Apache Spark Structured Streaming pour le projet DataMLOps Crypto Binance.

## 📁 Structure

```
src/processing/
├── config.py                  # Configuration centralisée
├── spark_session.py           # Initialisation SparkSession
├── kafka_consumer.py          # Consumer Kafka Structured Streaming
├── transformations.py         # Transformations de données
├── technical_indicators.py    # Calcul indicateurs techniques
├── mongodb_writer.py          # Connecteur MongoDB
└── main_pipeline.py           # Pipeline principal
```

## 🚀 Quick Start

### Prérequis
- Docker avec Kafka, Zookeeper et MongoDB running
- Python 3.11+
- PySpark 3.5.0

### Installation
```bash
pip install -r requirements.txt
```

### Lancement du pipeline
```bash
# Mode streaming (temps réel)
python src/processing/main_pipeline.py --mode streaming

# Mode batch (historique)
python src/processing/main_pipeline.py --mode batch --start-date 2024-02-01
```

## ⚙️ Configuration

Variables d'environnement (`.env`):
```bash
# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:29092
KAFKA_TOPICS=raw_trades,raw_klines

# MongoDB
MONGODB_URI=mongodb://datamlops:datamlops123@localhost:27017/cryptomarket?authSource=admin

# Spark
SPARK_MASTER=local[*]
SPARK_APP_NAME=crypto_etl
SPARK_CHECKPOINT_DIR=/tmp/spark-checkpoints
```

## 📊 Indicateurs Techniques

Le module calcule automatiquement:
- **RSI (14)**: Relative Strength Index
- **MACD**: Moving Average Convergence Divergence (12, 26, 9)
- **Bollinger Bands**: SMA 20 ± 2σ
- **SMA/EMA**: Moving Averages (20, 50, 200)

## 🔄 Pipeline Flow

```
Kafka Topics → Spark Streaming → Transformations → Indicators → MongoDB
  (raw_trades)      ↓              (parsing)        (RSI/MACD)   (collections)
  (raw_klines)      ↓              (validation)     (Bollinger)
                    ↓
              Checkpointing
```

## 🧪 Tests

```bash
# Tests unitaires
pytest tests/test_spark_processing.py -v

# Test d'intégration
python src/processing/main_pipeline.py --mode test --duration 60
```

## 📝 Logs

Les logs Spark sont disponibles dans:
- Console: niveau INFO
- Fichier: `logs/spark_processing.log`
- Spark UI: http://localhost:4040 (pendant l'exécution)

## 🐛 Troubleshooting

**Erreur: "Kafka broker not available"**
```bash
docker-compose ps kafka  # Vérifier que Kafka est running
```

**Erreur: "MongoDB connection refused"**
```bash
docker-compose ps mongodb  # Vérifier MongoDB
```

**Checkpoints corrompus**
```bash
rm -rf /tmp/spark-checkpoints/*  # Supprimer les checkpoints
```

# 🔥 P3 - ETL Spark: Nettoyage + Calcul Indicateurs Techniques

Module Apache Spark pour le traitement ETL des données crypto OHLCV, incluant:
- **Nettoyage des données**: validation, suppression des nulls, détection outliers
- **Indicateurs techniques**: RSI, MACD, Bollinger Bands, SMA, EMA
- **Streaming temps réel**: Kafka → Spark Structured Streaming → MongoDB

## 📁 Structure du Module

```
src/processing/
├── __init__.py              # Package marker
├── config.py                # Configuration centralisée (Kafka, MongoDB, Spark, Cleaning, Indicators)
├── spark_session.py         # Création et gestion de la SparkSession
├── data_cleaning.py         # ⭐ Pipeline nettoyage des données OHLCV
├── technical_indicators.py  # ⭐ Calcul RSI, MACD, Bollinger Bands, SMA, EMA
├── kafka_consumer.py        # Consumer Kafka Structured Streaming
├── mongodb_writer.py        # Persistance MongoDB (batch + streaming)
└── main_pipeline.py         # Orchestrateur principal du pipeline

tests/
└── test_etl_spark.py        # Tests unitaires (pytest)
```

## 🚀 Démarrage Rapide

### Prérequis

```bash
pip install pyspark==3.4.0 pymongo pytest
```

### Mode Test (sans Kafka/MongoDB)

```bash
cd DataOps_MLOps_Project
python src/processing/main_pipeline.py --mode test
```

### Mode Streaming (production)

```bash
# 1. Lancer l'infrastructure
docker-compose -f docker/docker-compose.yml up -d kafka zookeeper mongodb

# 2. Créer les topics Kafka
bash scripts/setup_kafka_topics.sh   # Linux/Mac
scripts\setup_kafka_topics.bat       # Windows

# 3. Lancer le pipeline streaming
python src/processing/main_pipeline.py --mode streaming
```

### Mode Debug (console)

```bash
python src/processing/main_pipeline.py --mode streaming --debug
```

## 🧹 Nettoyage des Données (`data_cleaning.py`)

| Étape | Description |
|-------|-------------|
| 1. Normalisation | Symbol → MAJUSCULES, timestamp → event_time |
| 2. Null Handling | Suppression lignes avec nulls dans colonnes critiques |
| 3. Validation OHLCV | `high >= low`, prix > 0, volume ≥ 0 |
| 4. Déduplication | Suppression doublons `(symbol, interval, timestamp)` |
| 5. Outlier Detection | Z-score sur `close` (seuil: 3.0σ) |
| 6. Price Spike Detection | Variation % entre bougies consécutives (seuil: 5%) |

## 📊 Indicateurs Techniques (`technical_indicators.py`)

### RSI (Relative Strength Index) - 14 périodes

```
delta = close_t - close_{t-1}
avg_gain = mean(gains, 14)
avg_loss = mean(losses, 14)
RSI = 100 - (100 / (1 + avg_gain/avg_loss))
```

**Signaux**:
- `overbought` → RSI > 70 (potentiel retournement baissier)
- `oversold`   → RSI < 30 (potentiel retournement haussier)
- `neutral`    → 30 ≤ RSI ≤ 70

### MACD (12, 26, 9)

```
MACD Line   = EMA(12) - EMA(26)
Signal Line = EMA(MACD, 9)
Histogram   = MACD Line - Signal Line
```

**Signaux de croisement**:
- `bullish_cross` → Histogram passe de négatif à positif
- `bearish_cross` → Histogram passe de positif à négatif

### Bollinger Bands (20 périodes, 2σ)

```
Middle Band = SMA(20)
Upper Band  = SMA(20) + 2 × σ
Lower Band  = SMA(20) - 2 × σ
%B          = (Close - Lower) / (Upper - Lower)
```

## 🧪 Tests

```bash
cd DataOps_MLOps_Project
pytest tests/test_etl_spark.py -v

# Résultats attendus:
# TestDataCleaning::test_normalize_symbol_uppercase     PASSED
# TestDataCleaning::test_validate_ohlcv_rejects_*       PASSED
# TestTechnicalIndicators::test_rsi_range_0_to_100      PASSED
# TestTechnicalIndicators::test_bollinger_upper_ge_*    PASSED
# TestIntegration::test_full_etl_pipeline               PASSED
```

## ⚙️ Variables d'Environnement

| Variable | Défaut | Description |
|----------|--------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Adresse du broker Kafka |
| `MONGODB_URI` | `mongodb://admin:password@localhost:27017/crypto_data?authSource=admin` | URI MongoDB |

## 🔧 Configuration Python

```python
from src.processing.config import ProcessingConfig

config = ProcessingConfig.from_env()
config.indicators.rsi_period = 14  # Modifier si besoin
config.cleaning.price_spike_threshold = 0.03  # 3%
```

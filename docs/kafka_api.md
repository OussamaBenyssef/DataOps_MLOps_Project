# Kafka API & Schemas Documentation

Cette documentation décrit l'architecture, la configuration et les schémas de données utilisés pour l'intégration de Kafka dans le projet `DataOps_MLOps_Project`.

---

## 1. Vue d'ensemble de l'Architecture

Les données en temps réel sont streamées depuis Binance (WebSocket), produites vers Kafka de manière partitionnée, et consommées par Apache Spark (Structured Streaming) pour enrichissement et agrégation.

1. **Source** : API Binance WebSocket. L'ingestion est gérée par le `BinanceKafkaProducer`.
2. **Streaming Engine** : Kafka, agissant comme le hub de messages central à très haut débit.
3. **Consumer** : Les jobs Spark en streaming s'abonnent aux topics Kafka (via `pyspark.sql.functions`), transforment le JSON en DataFrames structurés, et appliquent des agrégations temporelles (Watermarks, tumbling windows).

> **Voir le code source** : 
> - **Producer** : `src/streaming/kafka_producer.py`
> - **Consumer** : `src/processing/kafka_consumer.py`
> - **Configuration** : `src/streaming/config.py`

---

## 2. Configuration Kafka (`kafka_config`)

La configuration Kafka est centralisée dans la classe `KafkaConfig` (`src/streaming/config.py`).

### Bootstrap Servers
- Détection automatique : Le producer et le consumer tentent d'utiliser la variable d'environnement `KAFKA_BOOTSTRAP_SERVERS`.
- Valeur par défaut : `localhost:29092` (idéal pour le développement local hors Docker).

### Configuration du Producer
Le `BinanceKafkaProducer` est optimisé pour le streaming temps réel et la robustesse :
- **`acks`** : `"all"` (Garantit que tous les répliquas In-Sync ont bien accusé réception du message).
- **`compression_type`** : `"gzip"` (Réduit significativement la bande passante utilisée par les messages JSON).
- **`retries`** : `3` (Nombre de tentatives en cas d'échec transitoire).
- **`linger_ms`** : `10` (Le producer attend jusqu'à 10ms pour grouper les messages).
- **`batch_size`** : `16384` (Taille maximale d'un batch à envoyer, soit 16 Ko).
- **`max_in_flight_requests_per_connection`** : `5`.

### Configuration du Consumer
Les paramètres recommandés pour consommer ces flux :
- **`group_id`** : `"binance_consumer_group"`.
- **`auto_offset_reset`** : `"latest"` (Par défaut, le consumer démarre sur les derniers messages afin de privilégier le temps réel).
- **`enable_auto_commit`** : `True`.

---

## 3. Topics et Flux de données

Le producteur intercepte les messages du WebSocket de Binance, examine le champ `stream`, et les achemine vers les topics Kafka correspondants:

| Flux WebSocket Binance (`stream`) | Topic Kafka Destination | Contenu                                |
|-----------------------------------|-------------------------|----------------------------------------|
| `*@aggTrade` ou `*@trade`         | `raw_trades`            | Trades individuels (instantanés).      |
| `*@kline_*`                       | `raw_klines`            | Chandeliers OHLCV (agrégés par temps). |
| `*@ticker`                        | `raw_ticker`            | Mini-ticker sur 24h.                   |
| `*@depth*`                        | `raw_depth`             | Order book / Profondeur de marché.     |

> 💡 **Remarque sur le partitionnement** : Lors de l'envoi vers un topic, le `BinanceKafkaProducer` utilise la **paire de trading** (ex: `btcusdt`) comme **Key** Kafka. Cela garantit que tous les messages concernant un même actif atterriront dans la même partition Kafka, préservant ainsi l'ordre strict des événements et optimisant les lectures parallèles.

---

## 4. Schémas de Données (Payloads JSON)

Les messages transités dans Kafka sont au format JSON. Dans Spark Structured Streaming (`src/processing/kafka_consumer.py`), ils sont désérialisés selon les schémas PySpark `StructType` suivants.

### 4.1. Topic : `raw_trades`

Ce topic reçoit les informations sur chaque transaction effectuée sur le marché.

**Schéma PySpark :**
```python
StructType([
    StructField("symbol", StringType(), False),
    StructField("price", DoubleType(), False),
    StructField("quantity", DoubleType(), False),
    StructField("timestamp", LongType(), False),  # UNIX timestamp (en ms)
    StructField("trade_id", LongType(), True),
    StructField("is_buyer_maker", BooleanType(), True)
])
```

**Exemple de message JSON en sortie de Kafka (champs imbriqués 'data') :**
```json
{
  "symbol": "BTCUSDT",
  "price": 50000.00,
  "quantity": 0.001,
  "timestamp": 1639584000000,
  "trade_id": 123456789,
  "is_buyer_maker": true
}
```
*(Le payload Kafka complet inclut les métadonnées WebSocket, mais le consumer Spark extrait le sous-objet `data`)*

---

### 4.2. Topic : `raw_klines`

Ce topic reçoit des bougies (candlesticks / OHLCV) mis à jour régulièrement par Binance.

**Schéma PySpark :**
```python
StructType([
    StructField("symbol", StringType(), False),
    StructField("interval", StringType(), False),    # ex: "1m", "5m"
    StructField("open_time", LongType(), False),     # UNIX timestamp begin (en ms)
    StructField("close_time", LongType(), False),    # UNIX timestamp end (en ms)
    StructField("open", DoubleType(), False),
    StructField("high", DoubleType(), False),
    StructField("low", DoubleType(), False),
    StructField("close", DoubleType(), False),
    StructField("volume", DoubleType(), False),
    StructField("quote_volume", DoubleType(), True),
    StructField("trades_count", LongType(), True)
])
```

**Exemple de payload JSON (champs extraits du corps 'k' du stream Binance) :**
```json
{
  "symbol": "ETHUSDT",
  "interval": "1m",
  "open_time": 1639584000000,
  "close_time": 1639584059999,
  "open": 4000.00,
  "high": 4010.00,
  "low": 3995.00,
  "close": 4005.00,
  "volume": 100.0,
  "quote_volume": 400500.0,
  "trades_count": 550
}
```

> **Note - Traitement PySpark** : Lors de la lecture de `raw_klines`, le consumer Spark convertit les timestamps UNIX millisecondes en type Timestamp natif et renomme `open_time` en `timestamp` pour faciliter les `groupBy(window())`.

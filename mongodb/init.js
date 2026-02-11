// MongoDB init script - Crypto Market Database
// Creates collections with validators for the Binance crypto project

db = db.getSiblingDB('cryptomarket');

// Create user
db.createUser({
    user: 'datamlops',
    pwd: 'datamlops123',
    roles: [{ role: 'readWrite', db: 'cryptomarket' }]
});

// Collection: raw_trades (données brutes WebSocket)
db.createCollection('raw_trades', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['symbol', 'price', 'quantity', 'timestamp'],
            properties: {
                symbol: { bsonType: 'string', description: 'Paire trading (ex: BTCUSDT)' },
                price: { bsonType: 'double', description: 'Prix du trade' },
                quantity: { bsonType: 'double', description: 'Volume du trade' },
                timestamp: { bsonType: 'date', description: 'Horodatage du trade' },
                trade_id: { bsonType: 'long', description: 'ID unique Binance' },
                is_buyer_maker: { bsonType: 'bool' }
            }
        }
    }
});

// Collection: ohlcv (candlesticks / klines)
db.createCollection('ohlcv', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['symbol', 'interval', 'open', 'high', 'low', 'close', 'volume', 'timestamp'],
            properties: {
                symbol: { bsonType: 'string' },
                interval: { bsonType: 'string', description: '1m, 5m, 15m, 1h, 4h, 1d' },
                open: { bsonType: 'double' },
                high: { bsonType: 'double' },
                low: { bsonType: 'double' },
                close: { bsonType: 'double' },
                volume: { bsonType: 'double' },
                timestamp: { bsonType: 'date' }
            }
        }
    }
});

// Collection: indicators (indicateurs techniques calculés par Spark)
db.createCollection('indicators', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['symbol', 'timestamp'],
            properties: {
                symbol: { bsonType: 'string' },
                timestamp: { bsonType: 'date' },
                rsi_14: { bsonType: 'double' },
                macd: { bsonType: 'double' },
                macd_signal: { bsonType: 'double' },
                bollinger_upper: { bsonType: 'double' },
                bollinger_lower: { bsonType: 'double' },
                sma_20: { bsonType: 'double' },
                ema_12: { bsonType: 'double' }
            }
        }
    }
});

// Collection: anomalies (détections anomalies)
db.createCollection('anomalies');

// Collection: predictions (prédictions ML)
db.createCollection('predictions');

// Collection: daily_metrics (métriques agrégées)
db.createCollection('daily_metrics');

// Index pour performance
db.raw_trades.createIndex({ symbol: 1, timestamp: -1 });
db.ohlcv.createIndex({ symbol: 1, interval: 1, timestamp: -1 });
db.indicators.createIndex({ symbol: 1, timestamp: -1 });
db.anomalies.createIndex({ symbol: 1, detected_at: -1 });
db.predictions.createIndex({ symbol: 1, predicted_at: -1 });
db.daily_metrics.createIndex({ date: -1 });

print('✅ Database cryptomarket initialized with collections and indexes');

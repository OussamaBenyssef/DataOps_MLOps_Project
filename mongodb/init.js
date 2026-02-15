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
// Structure: { symbol, anomaly_type, severity, detected_at, value, threshold, metadata }
db.createCollection('anomalies', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['symbol', 'anomaly_type', 'severity', 'detected_at', 'value'],
            properties: {
                symbol: {
                    bsonType: 'string',
                    description: 'Paire trading (ex: BTCUSDT)'
                },
                anomaly_type: {
                    bsonType: 'string',
                    enum: ['price_spike', 'price_drop', 'volume_spike', 'volatility_spike', 'unusual_pattern'],
                    description: 'Type d\'anomalie détectée'
                },
                severity: {
                    bsonType: 'string',
                    enum: ['low', 'medium', 'high', 'critical'],
                    description: 'Niveau de sévérité de l\'anomalie'
                },
                detected_at: {
                    bsonType: 'date',
                    description: 'Timestamp de détection'
                },
                value: {
                    bsonType: 'double',
                    description: 'Valeur observée (prix, volume, etc.)'
                },
                threshold: {
                    bsonType: 'double',
                    description: 'Seuil de détection dépassé'
                },
                deviation_percent: {
                    bsonType: 'double',
                    description: 'Pourcentage de déviation par rapport à la normale'
                },
                metadata: {
                    bsonType: 'object',
                    description: 'Données contextuelles additionnelles'
                }
            }
        }
    }
});

// Collection: predictions (prédictions ML)
// Structure: { symbol, model_name, predicted_price, confidence, predicted_at, prediction_horizon, features }
db.createCollection('predictions', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['symbol', 'model_name', 'predicted_price', 'confidence', 'predicted_at', 'prediction_horizon'],
            properties: {
                symbol: {
                    bsonType: 'string',
                    description: 'Paire trading (ex: BTCUSDT)'
                },
                model_name: {
                    bsonType: 'string',
                    description: 'Nom du modèle ML utilisé (ex: xgboost_v1, lstm_v2)'
                },
                model_version: {
                    bsonType: 'string',
                    description: 'Version du modèle'
                },
                predicted_price: {
                    bsonType: 'double',
                    description: 'Prix prédit'
                },
                confidence: {
                    bsonType: 'double',
                    minimum: 0.0,
                    maximum: 1.0,
                    description: 'Score de confiance de la prédiction (0-1)'
                },
                predicted_at: {
                    bsonType: 'date',
                    description: 'Timestamp de la prédiction'
                },
                prediction_horizon: {
                    bsonType: 'string',
                    enum: ['5m', '15m', '30m', '1h', '4h', '1d'],
                    description: 'Horizon temporel de la prédiction'
                },
                current_price: {
                    bsonType: 'double',
                    description: 'Prix actuel au moment de la prédiction'
                },
                predicted_change_percent: {
                    bsonType: 'double',
                    description: 'Variation prédite en pourcentage'
                },
                features: {
                    bsonType: 'object',
                    description: 'Features utilisées pour la prédiction'
                },
                mlflow_run_id: {
                    bsonType: 'string',
                    description: 'ID du run MLflow associé'
                }
            }
        }
    }
});

// Collection: daily_metrics (métriques agrégées)
db.createCollection('daily_metrics');

// ============================================
// INDEX OPTIMIZATION FOR TIME-SERIES QUERIES
// ============================================

// raw_trades: Index composé pour requêtes time-series
db.raw_trades.createIndex({ symbol: 1, timestamp: -1 });
db.raw_trades.createIndex({ timestamp: -1 }); // Pour requêtes globales par temps
db.raw_trades.createIndex({ trade_id: 1 }, { unique: true }); // Éviter doublons

// ohlcv: Index composé pour requêtes multi-dimensions
db.ohlcv.createIndex({ symbol: 1, interval: 1, timestamp: -1 });
db.ohlcv.createIndex({ timestamp: -1 }); // Pour agrégations temporelles
db.ohlcv.createIndex({ symbol: 1, timestamp: -1 }); // Requêtes par symbole

// indicators: Index pour accès rapide aux indicateurs techniques
db.indicators.createIndex({ symbol: 1, timestamp: -1 });
db.indicators.createIndex({ symbol: 1, interval: 1, timestamp: -1 }); // Avec interval
db.indicators.createIndex({ timestamp: -1 }); // Pour analyses cross-symbol

// anomalies: Index pour détection et alerting
db.anomalies.createIndex({ symbol: 1, detected_at: -1 });
db.anomalies.createIndex({ anomaly_type: 1, detected_at: -1 }); // Par type
db.anomalies.createIndex({ severity: 1, detected_at: -1 }); // Par sévérité
db.anomalies.createIndex({ detected_at: -1 }); // Toutes les anomalies récentes

// predictions: Index pour requêtes ML
db.predictions.createIndex({ symbol: 1, predicted_at: -1 });
db.predictions.createIndex({ model_name: 1, predicted_at: -1 }); // Par modèle
db.predictions.createIndex({ prediction_horizon: 1, predicted_at: -1 }); // Par horizon
db.predictions.createIndex({ mlflow_run_id: 1 }); // Traçabilité MLflow

// daily_metrics: Index pour dashboards et reporting
db.daily_metrics.createIndex({ date: -1 });
db.daily_metrics.createIndex({ symbol: 1, date: -1 });

// ============================================
// TTL INDEX - AUTOMATIC DATA RETENTION
// ============================================
// Les données brutes sont automatiquement supprimées après 30 jours
db.raw_trades.createIndex(
    { timestamp: 1 },
    { expireAfterSeconds: 2592000 } // 30 jours
);

// Les anomalies sont conservées 90 jours
db.anomalies.createIndex(
    { detected_at: 1 },
    { expireAfterSeconds: 7776000 } // 90 jours
);

// Les prédictions sont conservées 60 jours
db.predictions.createIndex(
    { predicted_at: 1 },
    { expireAfterSeconds: 5184000 } // 60 jours
);

print('✅ Database cryptomarket initialized with collections, validators, and optimized indexes');
print('📊 Collections created: raw_trades, ohlcv, indicators, anomalies, predictions, daily_metrics');
print('🔍 Compound indexes created for time-series optimization');
print('⏰ TTL indexes configured for automatic data retention');


// ============================================
// MongoDB Init Script - Crypto Market Database
// P3: Schema & Collections
// ============================================
// Ce script est exécuté automatiquement au premier lancement de MongoDB
// via docker-entrypoint-initdb.d/

db = db.getSiblingDB('cryptomarket');

// Création de l'utilisateur applicatif
db.createUser({
    user: 'datamlops',
    pwd: 'datamlops123',
    roles: [{ role: 'readWrite', db: 'cryptomarket' }]
});

// ============================================
// COLLECTION 1: raw_trades
// Données brutes des trades WebSocket Binance
// TTL: 7 jours (données temporaires avant agrégation)
// ============================================
db.createCollection('raw_trades', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['symbol', 'price', 'quantity', 'timestamp'],
            properties: {
                symbol: { bsonType: 'string', description: 'Paire de trading (ex: BTCUSDT)' },
                price: { bsonType: 'double', description: 'Prix du trade' },
                quantity: { bsonType: 'double', description: 'Volume du trade' },
                timestamp: { bsonType: 'date', description: 'Horodatage du trade' },
                trade_id: { bsonType: 'long', description: 'ID unique Binance' },
                is_buyer_maker: { bsonType: 'bool', description: 'True si acheteur est le maker' },
                received_at: { bsonType: 'date', description: 'Date de réception par le système' },
                ingested_at: { bsonType: 'date', description: 'Date d\'insertion en base' }
            }
        }
    },
    validationLevel: 'moderate',
    validationAction: 'warn'
});

// Indexes raw_trades
db.raw_trades.createIndex({ symbol: 1, timestamp: -1 }, { name: 'idx_trades_symbol_ts' });
db.raw_trades.createIndex({ trade_id: 1 }, { name: 'idx_trades_id', unique: true, sparse: true });
db.raw_trades.createIndex({ ingested_at: 1 }, { name: 'idx_trades_ttl', expireAfterSeconds: 604800 }); // TTL 7 jours

// ============================================
// COLLECTION 2: ohlcv
// Candlesticks / Klines agrégées par Spark
// Pas de TTL (données historiques à conserver)
// ============================================
db.createCollection('ohlcv', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['symbol', 'interval', 'open', 'high', 'low', 'close', 'volume', 'timestamp'],
            properties: {
                symbol: { bsonType: 'string', description: 'Paire de trading' },
                interval: { bsonType: 'string', enum: ['1m', '5m', '15m', '1h', '4h', '1d'], description: 'Intervalle temporel' },
                open: { bsonType: 'double', description: 'Prix d\'ouverture' },
                high: { bsonType: 'double', description: 'Prix le plus haut' },
                low: { bsonType: 'double', description: 'Prix le plus bas' },
                close: { bsonType: 'double', description: 'Prix de clôture' },
                volume: { bsonType: 'double', description: 'Volume total' },
                timestamp: { bsonType: 'date', description: 'Début de la bougie' },
                num_trades: { bsonType: 'int', description: 'Nombre de trades dans la bougie' },
                price_change: { bsonType: 'double', description: 'Variation de prix (close - open)' },
                price_change_pct: { bsonType: 'double', description: 'Variation en pourcentage' }
            }
        }
    },
    validationLevel: 'moderate',
    validationAction: 'warn'
});

// Index unique pour éviter les doublons de bougies
db.ohlcv.createIndex(
    { symbol: 1, interval: 1, timestamp: -1 },
    { name: 'idx_ohlcv_symbol_interval_ts', unique: true }
);

// ============================================
// COLLECTION 3: indicators
// Indicateurs techniques calculés par Spark
// (RSI, MACD, Bollinger Bands, SMA, EMA)
// ============================================
db.createCollection('indicators', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['symbol', 'interval', 'timestamp'],
            properties: {
                symbol: { bsonType: 'string', description: 'Paire de trading' },
                interval: { bsonType: 'string', description: 'Intervalle temporel' },
                timestamp: { bsonType: 'date', description: 'Horodatage du calcul' },
                // Prix de référence
                close: { bsonType: 'double' },
                price_change: { bsonType: 'double' },
                price_change_pct: { bsonType: 'double' },
                // Moyennes mobiles
                sma_20: { bsonType: 'double', description: 'SMA période 20' },
                sma_50: { bsonType: 'double', description: 'SMA période 50' },
                ema_12: { bsonType: 'double', description: 'EMA période 12' },
                ema_26: { bsonType: 'double', description: 'EMA période 26' },
                // RSI
                rsi_14: { bsonType: 'double', description: 'RSI période 14 (0-100)' },
                // MACD
                macd: { bsonType: 'double', description: 'MACD line' },
                macd_signal: { bsonType: 'double', description: 'Signal line' },
                macd_histogram: { bsonType: 'double', description: 'Histogramme MACD' },
                // Bollinger Bands
                bollinger_upper: { bsonType: 'double', description: 'Bande supérieure' },
                bollinger_middle: { bsonType: 'double', description: 'Bande médiane (SMA)' },
                bollinger_lower: { bsonType: 'double', description: 'Bande inférieure' },
                // Metadata
                calculated_at: { bsonType: 'date', description: 'Date de calcul' }
            }
        }
    },
    validationLevel: 'moderate',
    validationAction: 'warn'
});

// Indexes indicators
db.indicators.createIndex(
    { symbol: 1, interval: 1, timestamp: -1 },
    { name: 'idx_indicators_symbol_interval_ts', unique: true }
);

// ============================================
// COLLECTION 4: anomalies
// Anomalies détectées (price spikes, volume spikes)
// TTL: 90 jours
// ============================================
db.createCollection('anomalies', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['symbol', 'anomaly_type', 'severity', 'detected_at'],
            properties: {
                symbol: { bsonType: 'string', description: 'Paire de trading' },
                anomaly_type: { bsonType: 'string', enum: ['price_spike', 'volume_spike', 'rsi_extreme', 'bollinger_breakout', 'macd_divergence'], description: 'Type d\'anomalie' },
                severity: { bsonType: 'string', enum: ['low', 'medium', 'high', 'critical'], description: 'Niveau de sévérité' },
                detected_at: { bsonType: 'date', description: 'Date de détection' },
                value: { bsonType: 'double', description: 'Valeur observée' },
                threshold: { bsonType: 'double', description: 'Seuil dépassé' },
                description: { bsonType: 'string', description: 'Description lisible' },
                interval: { bsonType: 'string', description: 'Intervalle temporel' },
                metadata: { bsonType: 'object', description: 'Données supplémentaires' }
            }
        }
    },
    validationLevel: 'moderate',
    validationAction: 'warn'
});

// Indexes anomalies
db.anomalies.createIndex({ symbol: 1, detected_at: -1 }, { name: 'idx_anomalies_symbol_date' });
db.anomalies.createIndex({ anomaly_type: 1, severity: 1 }, { name: 'idx_anomalies_type_severity' });
db.anomalies.createIndex({ detected_at: 1 }, { name: 'idx_anomalies_ttl', expireAfterSeconds: 7776000 }); // TTL 90 jours

// ============================================
// COLLECTION 5: predictions
// Prédictions ML (prix, direction, etc.)
// TTL: 30 jours
// ============================================
db.createCollection('predictions', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['symbol', 'model_name', 'predicted_at'],
            properties: {
                symbol: { bsonType: 'string', description: 'Paire de trading' },
                model_name: { bsonType: 'string', description: 'Nom du modèle (ex: xgboost_v2)' },
                model_version: { bsonType: 'string', description: 'Version MLflow du modèle' },
                predicted_at: { bsonType: 'date', description: 'Date de la prédiction' },
                target_time: { bsonType: 'date', description: 'Date cible de la prédiction' },
                prediction_type: { bsonType: 'string', enum: ['price', 'direction', 'volatility'], description: 'Type de prédiction' },
                predicted_value: { bsonType: 'double', description: 'Valeur prédite' },
                actual_value: { bsonType: 'double', description: 'Valeur réelle (rempli après)' },
                confidence: { bsonType: 'double', description: 'Score de confiance (0-1)' },
                features_used: { bsonType: 'array', description: 'Features utilisées pour la prédiction' },
                error: { bsonType: 'double', description: 'Erreur de prédiction (rempli après)' }
            }
        }
    },
    validationLevel: 'moderate',
    validationAction: 'warn'
});

// Indexes predictions
db.predictions.createIndex({ symbol: 1, predicted_at: -1 }, { name: 'idx_predictions_symbol_date' });
db.predictions.createIndex({ model_name: 1, symbol: 1, predicted_at: -1 }, { name: 'idx_predictions_model_symbol_date' });
db.predictions.createIndex({ predicted_at: 1 }, { name: 'idx_predictions_ttl', expireAfterSeconds: 2592000 }); // TTL 30 jours

// ============================================
// COLLECTION 6: aggregated_metrics
// Métriques agrégées calculées par Spark (daily, hourly)
// TTL: 180 jours
// ============================================
db.createCollection('aggregated_metrics', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['symbol', 'interval', 'period_start', 'period_end', 'avg_price', 'total_volume'],
            properties: {
                symbol: { bsonType: 'string', description: 'Paire de trading (ex: BTCUSDT)' },
                interval: { bsonType: 'string', enum: ['1h', '4h', '1d', '1w'], description: 'Période d\'agrégation' },
                period_start: { bsonType: 'date', description: 'Début de la période' },
                period_end: { bsonType: 'date', description: 'Fin de la période' },
                // Prix
                avg_price: { bsonType: 'double', description: 'Prix moyen (close)' },
                min_price: { bsonType: 'double', description: 'Prix minimum' },
                max_price: { bsonType: 'double', description: 'Prix maximum' },
                open_price: { bsonType: 'double', description: 'Prix d\'ouverture (premier close)' },
                close_price: { bsonType: 'double', description: 'Prix de clôture (dernier close)' },
                price_volatility: { bsonType: 'double', description: 'Écart-type des prix close' },
                price_range_pct: { bsonType: 'double', description: '(max - min) / min * 100' },
                period_return_pct: { bsonType: 'double', description: '(close - open) / open * 100' },
                // Volume
                total_volume: { bsonType: 'double', description: 'Volume total sur la période' },
                avg_volume: { bsonType: 'double', description: 'Volume moyen par bougie' },
                total_trades: { bsonType: 'int', description: 'Nombre total de trades' },
                // VWAP
                vwap: { bsonType: 'double', description: 'Volume-Weighted Average Price' },
                // Metadata
                num_candles: { bsonType: 'int', description: 'Nombre de bougies agrégées' },
                calculated_at: { bsonType: 'date', description: 'Date de calcul' }
            }
        }
    },
    validationLevel: 'moderate',
    validationAction: 'warn'
});

// Index unique pour éviter les doublons de métriques agrégées
db.aggregated_metrics.createIndex(
    { symbol: 1, interval: 1, period_start: -1 },
    { name: 'idx_agg_metrics_symbol_interval_period', unique: true }
);
db.aggregated_metrics.createIndex({ calculated_at: 1 }, { name: 'idx_agg_metrics_ttl', expireAfterSeconds: 15552000 }); // TTL 180 jours

// ============================================
// RÉSUMÉ
// ============================================
print('');
print('✅ Database cryptomarket initialized');
print('📦 Collections: raw_trades, ohlcv, indicators, anomalies, predictions, aggregated_metrics');
print('🔑 Indexes: compound, unique, TTL');
print('');

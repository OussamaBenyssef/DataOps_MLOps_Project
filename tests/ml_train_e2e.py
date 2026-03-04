"""
ML Training E2E — Reads data from MongoDB, trains XGBoost + LSTM models
"""
import sys
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

sys.path.insert(0, '/app/src')

import pymongo
import pandas as pd
import numpy as np

# -- Step 1: Read from MongoDB --------------------------------------
logger.info("=" * 60)
logger.info("STEP 1: Reading OHLCV data from MongoDB")
logger.info("=" * 60)

MONGO_URI = "mongodb://datamlops:datamlops123@mongodb:27017/cryptomarket?authSource=admin"
client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
db = client["cryptomarket"]

cursor = db.ohlcv.find({"symbol": "BTCUSDT"}).sort("timestamp", 1)
docs = list(cursor)
client.close()

logger.info(f"✅ Read {len(docs)} BTCUSDT documents from MongoDB")

if len(docs) < 50:
    logger.error(f"❌ Not enough data ({len(docs)} rows). Need at least 50.")
    sys.exit(1)

# -- Step 2: Create DataFrame & Feature Engineering ------------------
logger.info("=" * 60)
logger.info("STEP 2: Feature Engineering")
logger.info("=" * 60)

raw_df = pd.DataFrame(docs)

# Select only the core OHLCV columns needed for feature engineering
core_cols = ['symbol', 'interval', 'timestamp', 'open', 'high', 'low', 'close', 'volume']
df = raw_df[core_cols].copy()
for col in ['open', 'high', 'low', 'close', 'volume']:
    df[col] = df[col].astype(float)

from ml.feature_engineering import CryptoFeatureEngineer

df = CryptoFeatureEngineer.add_price_features(df)
df = CryptoFeatureEngineer.add_volume_features(df)
df = CryptoFeatureEngineer.add_candle_features(df)
df = CryptoFeatureEngineer.add_target_variables(df)

# Drop NaN only in feature columns (initial rows lack rolling window data)
feature_cols = [c for c in df.columns if c not in {'symbol', 'interval', 'timestamp', '_id'}]
df = df.replace([np.inf, -np.inf], np.nan)
df = df.dropna(subset=feature_cols)

logger.info(f"✅ Feature matrix: {df.shape[0]} rows × {df.shape[1]} columns")

# -- Step 3: Train XGBoost ------------------------------------------
logger.info("=" * 60)
logger.info("STEP 3: Training XGBoost on MongoDB data")
logger.info("=" * 60)

from ml.model_training import CryptoPricePredictor

predictor = CryptoPricePredictor(test_size=0.2, sequence_length=10)

exclude = {"_id", "symbol", "interval", "timestamp", "open_time", "close_time",
           "quote_volume", "taker_buy_base", "taker_buy_quote", "ignore",
           "target_direction", "target_return_pct", "anomaly_label",
           "processed_at", "candle_type", "price_change", "price_change_percent"}
feature_names = [c for c in df.columns if c not in exclude and df[c].dtype in ['float64', 'int64']]

X_train, X_test, y_train, y_test = predictor.prepare_data(
    df, feature_names, target_col="target_direction"
)

model_xgb, metrics_xgb = predictor.train_xgboost(X_train, y_train, X_test, y_test)
preds_xgb = predictor.predict(model_xgb, X_test, model_type="xgboost")

logger.info(f"✅ XGBoost trained — Accuracy: {metrics_xgb['accuracy']:.2%}")
logger.info(f"   Predictions: {len(preds_xgb)} samples")

# -- Step 4: Train LSTM ---------------------------------------------
logger.info("=" * 60)
logger.info("STEP 4: Training LSTM on MongoDB data")
logger.info("=" * 60)

model_lstm, metrics_lstm = predictor.train_lstm(
    X_train, y_train, X_test, y_test, epochs=5, batch_size=32
)
preds_lstm = predictor.predict(model_lstm, X_test, model_type="lstm")

logger.info(f"✅ LSTM trained — Accuracy: {metrics_lstm['accuracy']:.2%}")
logger.info(f"   Predictions: {len(preds_lstm)} samples")

# -- Summary ---------------------------------------------------------
logger.info("")
logger.info("=" * 60)
logger.info("✅ ML TRAINING E2E COMPLETE — MongoDB → Features → XGBoost + LSTM")
logger.info(f"   Data source: MongoDB cryptomarket.ohlcv ({len(docs)} BTCUSDT docs)")
logger.info(f"   XGBoost Accuracy: {metrics_xgb['accuracy']:.2%}")
logger.info(f"   LSTM Accuracy:    {metrics_lstm['accuracy']:.2%}")
logger.info("=" * 60)

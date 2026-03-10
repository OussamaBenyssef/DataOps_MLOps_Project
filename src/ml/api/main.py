"""
FastAPI - Crypto Binance ML API - P4 (Abdessamad)
Endpoints: /predict, /anomalies, /health, /models

Usage (Docker):
    uvicorn main:app --host 0.0.0.0 --port 8000

Usage (local):
    cd src/ml/api && uvicorn main:app --reload
"""

import os
import logging
import sys
from datetime import datetime, timezone
from typing import List, Optional, Dict

import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

# Add src to path for local imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

logger = logging.getLogger(__name__)

# ============================================
# PYDANTIC SCHEMAS
# ============================================


class PredictRequest(BaseModel):
    """Request body for /predict endpoint."""

    symbol: str = Field(default="BTCUSDT", description="Trading pair")
    interval: str = Field(default="1m", description="Candle interval")
    limit: int = Field(default=200, ge=50, le=5000, description="Number of candles")


class PredictResponse(BaseModel):
    """Response for /predict endpoint."""

    symbol: str
    interval: str
    direction: str  # "UP" or "DOWN"
    confidence: float
    model_type: str
    features_used: int
    samples_analyzed: int
    timestamp: str


class AnomalyRequest(BaseModel):
    """Request body for /anomalies endpoint."""

    symbol: str = Field(default="BTCUSDT", description="Trading pair")
    interval: str = Field(default="1m", description="Candle interval")
    limit: int = Field(default=300, ge=50, le=5000, description="Number of candles")


class AnomalyItem(BaseModel):
    """Single anomaly detection result."""

    index: int
    is_anomaly: bool
    anomaly_score: float


class AnomalyResponse(BaseModel):
    """Response for /anomalies endpoint."""

    symbol: str
    interval: str
    total_points: int
    anomalies_detected: int
    anomaly_ratio: float
    model_type: str
    timestamp: str
    results: List[AnomalyItem]


class HealthResponse(BaseModel):
    """Response for /health endpoint."""

    status: str
    service: str
    version: str
    timestamp: str
    checks: Dict[str, str]


class ModelInfo(BaseModel):
    """Model information for /models endpoint."""

    name: str
    latest_version: Optional[str] = None
    description: str = ""


class ModelsResponse(BaseModel):
    """Response for /models endpoint."""

    models: List[ModelInfo]
    timestamp: str


# ============================================
# APP INITIALIZATION
# ============================================

app = FastAPI(
    title="Crypto Binance ML API",
    description="API de prédiction et détection d'anomalies pour données crypto",
    version="1.0.0",
)


def _get_mongodb_client():
    """Lazy MongoDB connection (only when endpoint is called)."""
    try:
        from pymongo import MongoClient

        uri = os.getenv("MONGODB_URI", "mongodb://datamlops:datamlops123@localhost:27017/cryptomarket?authSource=admin")
        client = MongoClient(uri, serverSelectionTimeoutMS=3000)
        return client
    except Exception:
        return None


def _get_mlflow_client():
    """Lazy MLflow client."""
    try:
        from mlflow.tracking import MlflowClient
        import mlflow

        tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5001")
        mlflow.set_tracking_uri(tracking_uri)
        return MlflowClient(tracking_uri)
    except Exception:
        return None


def _fetch_ohlcv_from_mongo(symbol: str, interval: str, limit: int) -> Optional[pd.DataFrame]:
    """Fetches OHLCV data from MongoDB (core columns only)."""
    client = _get_mongodb_client()
    if client is None:
        return None

    try:
        db = client["cryptomarket"]
        collection = db["ohlcv"]

        # Project only core OHLCV columns to avoid NaN contamination
        projection = {
            "_id": 0,
            "symbol": 1,
            "interval": 1,
            "timestamp": 1,
            "open": 1,
            "high": 1,
            "low": 1,
            "close": 1,
            "volume": 1,
        }
        cursor = collection.find(
            {"symbol": symbol.upper(), "interval": interval},
            projection,
            sort=[("timestamp", -1)],
        ).limit(limit)

        records = list(cursor)
        if not records:
            return None

        df = pd.DataFrame(records)
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.sort_values("timestamp").reset_index(drop=True)
        return df
    except Exception as e:
        logger.error(f"MongoDB fetch error: {e}")
        return None
    finally:
        client.close()


def _generate_synthetic_ohlcv(symbol: str, n: int = 500) -> pd.DataFrame:
    """Generates synthetic OHLCV data for demo/testing when MongoDB is unavailable."""
    np.random.seed(42)
    timestamps = pd.date_range("2024-02-01", periods=n, freq="1min")
    base_price = 45000.0
    returns = np.random.normal(0.0002, 0.005, n)
    close = base_price * np.cumprod(1 + returns)
    noise = np.random.uniform(0.001, 0.005, n)

    return pd.DataFrame(
        {
            "symbol": symbol,
            "interval": "1m",
            "timestamp": timestamps,
            "open": close * (1 - noise),
            "high": close * (1 + np.random.uniform(0, 0.003, n)),
            "low": close * (1 - np.random.uniform(0, 0.003, n)),
            "close": close,
            "volume": np.random.uniform(50, 500, n),
        }
    )


# ============================================
# ENDPOINTS
# ============================================


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """
    Vérifie que l'API est opérationnelle.
    Checks connectivity to MongoDB and MLflow.
    """
    checks = {}

    # MongoDB check
    mongo_client = _get_mongodb_client()
    if mongo_client:
        try:
            mongo_client.admin.command("ping")
            checks["mongodb"] = "connected"
        except Exception:
            checks["mongodb"] = "disconnected"
        finally:
            mongo_client.close()
    else:
        checks["mongodb"] = "unavailable"

    # MLflow check
    mlflow_client = _get_mlflow_client()
    if mlflow_client:
        try:
            mlflow_client.search_experiments(max_results=1)
            checks["mlflow"] = "connected"
        except Exception:
            checks["mlflow"] = "disconnected"
    else:
        checks["mlflow"] = "unavailable"

    overall = "healthy" if all(v == "connected" for v in checks.values()) else "degraded"

    return HealthResponse(
        status=overall,
        service="crypto-ml-api",
        version="1.0.0",
        timestamp=datetime.now(timezone.utc).isoformat(),
        checks=checks,
    )


@app.post("/predict", response_model=PredictResponse)
async def predict(request: PredictRequest):
    """
    Prédit la direction du prix (UP/DOWN) en utilisant XGBoost.
    Fetches data from MongoDB, runs feature engineering, returns prediction.
    """
    from ml.feature_engineering import CryptoFeatureEngineer
    from ml.model_training import CryptoPricePredictor

    # Fetch data
    df = _fetch_ohlcv_from_mongo(request.symbol, request.interval, request.limit)
    if df is None:
        df = _generate_synthetic_ohlcv(request.symbol, request.limit)
        logger.info("Using synthetic data (MongoDB unavailable)")

    # Feature engineering
    try:
        fe = CryptoFeatureEngineer()
        df = fe.build_feature_matrix_from_dataframe(df, dropna=True)
        # Replace inf values that can appear in ratios/returns
        df = df.replace([np.inf, -np.inf], np.nan).dropna().reset_index(drop=True)
        feature_names = fe.get_feature_names(df)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Feature engineering failed: {e}")

    if len(df) < 50:
        raise HTTPException(status_code=400, detail="Not enough data for prediction")

    # Train and predict
    try:
        predictor = CryptoPricePredictor(sequence_length=5)
        X_train, X_test, y_train, y_test = predictor.prepare_data(df, feature_names)

        model, metrics = predictor.train_xgboost(
            X_train, y_train, X_test, y_test, params={"n_estimators": 50, "max_depth": 4}
        )

        # Predict on the last data point
        last_point = X_test[-1:] if len(X_test) > 0 else X_train[-1:]
        prediction = int(model.predict(last_point)[0])
        proba = model.predict_proba(last_point)[0]
        confidence = float(max(proba))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Prediction failed: {e}")

    return PredictResponse(
        symbol=request.symbol,
        interval=request.interval,
        direction="UP" if prediction == 1 else "DOWN",
        confidence=round(confidence, 4),
        model_type="xgboost",
        features_used=len(feature_names),
        samples_analyzed=len(df),
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


@app.post("/anomalies", response_model=AnomalyResponse)
async def detect_anomalies(request: AnomalyRequest):
    """
    Détecte les anomalies de marché en utilisant Isolation Forest.
    Returns anomaly flags and scores for each data point.
    """
    from ml.feature_engineering import CryptoFeatureEngineer
    from ml.anomaly_detection import CryptoAnomalyDetector

    # Fetch data
    df = _fetch_ohlcv_from_mongo(request.symbol, request.interval, request.limit)
    if df is None:
        df = _generate_synthetic_ohlcv(request.symbol, request.limit)
        logger.info("Using synthetic data (MongoDB unavailable)")

    # Feature engineering
    try:
        fe = CryptoFeatureEngineer()
        df = fe.build_feature_matrix_from_dataframe(df, dropna=True)
        # Replace inf values that can appear in ratios/returns
        df = df.replace([np.inf, -np.inf], np.nan).dropna().reset_index(drop=True)
        feature_names = fe.get_feature_names(df)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Feature engineering failed: {e}")

    if len(df) < 50:
        raise HTTPException(status_code=400, detail="Not enough data for anomaly detection")

    # Train and detect
    try:
        detector = CryptoAnomalyDetector(contamination=0.05)
        X_train, X_test, _, y_test = detector.prepare_data(df, feature_names)

        model, metrics = detector.train_isolation_forest(X_train, X_test, y_test)

        # Predict on test set
        preds = detector.predict_isolation_forest(model, X_test)
        scores = model.decision_function(X_test)

        # Build results
        results = []
        for i in range(len(preds)):
            results.append(
                AnomalyItem(
                    index=i,
                    is_anomaly=bool(preds[i] == 1),
                    anomaly_score=round(float(scores[i]), 6),
                )
            )

        anomalies_count = int(preds.sum())
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Anomaly detection failed: {e}")

    return AnomalyResponse(
        symbol=request.symbol,
        interval=request.interval,
        total_points=len(preds),
        anomalies_detected=anomalies_count,
        anomaly_ratio=round(anomalies_count / len(preds), 4) if len(preds) > 0 else 0,
        model_type="isolation_forest",
        timestamp=datetime.now(timezone.utc).isoformat(),
        results=results,
    )


@app.get("/models", response_model=ModelsResponse)
async def list_models():
    """
    Liste les modèles enregistrés dans MLflow Model Registry.
    """
    mlflow_client = _get_mlflow_client()
    models = []

    if mlflow_client:
        try:
            registered_models = mlflow_client.search_registered_models()
            for rm in registered_models:
                latest_version = None
                if rm.latest_versions:
                    latest_version = rm.latest_versions[0].version
                models.append(
                    ModelInfo(
                        name=rm.name,
                        latest_version=str(latest_version) if latest_version else None,
                        description=rm.description or "",
                    )
                )
        except Exception as e:
            logger.warning(f"MLflow registry query failed: {e}")

    return ModelsResponse(
        models=models,
        timestamp=datetime.now(timezone.utc).isoformat(),
    )

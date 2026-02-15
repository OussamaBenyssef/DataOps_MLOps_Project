"""
FastAPI - Crypto Binance ML API
Endpoints: /predict, /anomalies, /health
"""

from fastapi import FastAPI
from datetime import datetime

app = FastAPI(
    title="Crypto Binance ML API",
    description="API de prédiction et détection d'anomalies pour données crypto",
    version="0.1.0"
)


@app.get("/health")
async def health_check():
    """Vérifie que l'API est opérationnelle."""
    return {
        "status": "healthy",
        "service": "crypto-ml-api",
        "timestamp": datetime.utcnow().isoformat()
    }


@app.get("/predict")
async def predict():
    """Endpoint de prédiction (à implémenter par P4)."""
    return {
        "status": "not_implemented",
        "message": "Prédiction sera implémentée en Sprint 3.2"
    }


@app.get("/anomalies")
async def anomalies():
    """Endpoint de détection d'anomalies (à implémenter par P4)."""
    return {
        "status": "not_implemented",
        "message": "Détection anomalies sera implémentée en Sprint 3.2"
    }

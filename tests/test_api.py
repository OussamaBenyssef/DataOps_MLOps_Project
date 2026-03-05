"""
Unit Tests for FastAPI Crypto ML API - P4 (Abdessamad)
Tests use FastAPI TestClient with synthetic data — no Docker, MongoDB, or MLflow required.
"""

import pytest
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from fastapi.testclient import TestClient
from ml.api.main import app


client = TestClient(app)


# ============================================
# HEALTH ENDPOINT
# ============================================

def test_health_endpoint():
    """Test /health returns valid response."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["service"] == "crypto-ml-api"
    assert data["version"] == "1.0.0"
    assert "timestamp" in data
    assert "checks" in data
    assert data["status"] in ("healthy", "degraded")


def test_health_has_checks():
    """Test /health includes MongoDB and MLflow checks."""
    response = client.get("/health")
    data = response.json()
    assert "mongodb" in data["checks"]
    assert "mlflow" in data["checks"]


# ============================================
# PREDICT ENDPOINT
# ============================================

def test_predict_endpoint():
    """Test /predict returns valid prediction (uses synthetic data fallback)."""
    response = client.post("/predict", json={
        "symbol": "BTCUSDT",
        "interval": "1m",
        "limit": 200,
    })
    assert response.status_code == 200
    data = response.json()
    assert data["symbol"] == "BTCUSDT"
    assert data["direction"] in ("UP", "DOWN")
    assert 0 <= data["confidence"] <= 1
    assert data["model_type"] == "xgboost"
    assert data["features_used"] > 0
    assert data["samples_analyzed"] > 0
    assert "timestamp" in data


def test_predict_default_values():
    """Test /predict works with default request values."""
    response = client.post("/predict", json={})
    assert response.status_code == 200
    data = response.json()
    assert data["symbol"] == "BTCUSDT"
    assert data["direction"] in ("UP", "DOWN")


def test_predict_invalid_limit():
    """Test /predict rejects invalid limit."""
    response = client.post("/predict", json={"limit": 10})
    assert response.status_code == 422  # Pydantic validation error


# ============================================
# ANOMALIES ENDPOINT
# ============================================

def test_anomalies_endpoint():
    """Test /anomalies returns valid anomaly detection results."""
    response = client.post("/anomalies", json={
        "symbol": "BTCUSDT",
        "interval": "1m",
        "limit": 200,
    })
    assert response.status_code == 200
    data = response.json()
    assert data["symbol"] == "BTCUSDT"
    assert data["model_type"] == "isolation_forest"
    assert data["total_points"] > 0
    assert data["anomalies_detected"] >= 0
    assert 0 <= data["anomaly_ratio"] <= 1
    assert "results" in data
    assert len(data["results"]) == data["total_points"]
    assert "timestamp" in data


def test_anomalies_result_structure():
    """Test each anomaly result has the correct structure."""
    response = client.post("/anomalies", json={"limit": 200})
    data = response.json()
    if data["results"]:
        item = data["results"][0]
        assert "index" in item
        assert "is_anomaly" in item
        assert "anomaly_score" in item
        assert isinstance(item["is_anomaly"], bool)


def test_anomalies_default_values():
    """Test /anomalies works with default request values."""
    response = client.post("/anomalies", json={})
    assert response.status_code == 200


# ============================================
# MODELS ENDPOINT
# ============================================

def test_models_endpoint():
    """Test /models returns valid response."""
    response = client.get("/models")
    assert response.status_code == 200
    data = response.json()
    assert "models" in data
    assert "timestamp" in data
    assert isinstance(data["models"], list)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

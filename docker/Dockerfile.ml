# ─────────────────────────────────────────────────────────────
# Dockerfile.ml — Image pour l'entraînement des modèles ML
# Rôle: Expérimentation → Modèles entraînés (via MLflow)
# Contient: TensorFlow, scikit-learn, XGBoost, MLflow
# ─────────────────────────────────────────────────────────────
FROM python:3.11-slim

# Dépendances système pour TensorFlow / compilation
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    python3-dev \
    pkg-config \
    libhdf5-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Installer les dépendances Python ML
COPY docker/requirements-ml.txt /tmp/requirements-ml.txt
RUN pip install --no-cache-dir -r /tmp/requirements-ml.txt \
    && rm /tmp/requirements-ml.txt

# Copier le code source ML + processing (pour feature_engineering)
COPY src/ /app/src/

# PYTHONPATH: pour les imports src.ml.* et src.processing.*
ENV PYTHONPATH="/app:/app/src"

CMD ["python", "-c", "print('ML trainer ready. Use: python -m pytest tests/')"]

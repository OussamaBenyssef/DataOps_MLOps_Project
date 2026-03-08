"""
Anomaly Detection Module - P4 (Abdessamad)
Market anomaly detection using Isolation Forest and Autoencoder.
Integrates with MLflow for experiment tracking.

Usage:
    from ml.feature_engineering import CryptoFeatureEngineer
    from ml.anomaly_detection import CryptoAnomalyDetector

    fe = CryptoFeatureEngineer()
    df = fe.build_feature_matrix("BTCUSDT", "1m", limit=5000)

    detector = CryptoAnomalyDetector()
    results = detector.detect_and_compare(df, fe.get_feature_names(df))
"""

import logging
import os
import json
import joblib
import numpy as np
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import precision_score, recall_score, f1_score
from pymongo import MongoClient

from .config import anomaly_config, mongodb_config, model_config

logger = logging.getLogger(__name__)

# Suppress TensorFlow INFO logs
os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "2")


def _import_tensorflow():
    """Lazy import TensorFlow to avoid slow startup when not needed."""
    import tensorflow as tf
    return tf


class CryptoAnomalyDetector:
    """
    Detects market anomalies (flash crashes, volume spikes, unusual price moves)
    using unsupervised learning: Isolation Forest and Autoencoder.

    Both models are trained on feature matrices from CryptoFeatureEngineer.
    The `anomaly_label` column (produced by feature engineering) is used
    for evaluation only — the models are trained unsupervised.

    Features:
        - Isolation Forest: tree-based unsupervised outlier detection
        - Autoencoder: reconstruction error-based anomaly detection
        - MLflow: automatic experiment logging
        - Comparison: train both, pick the best by F1 score
    """

    def __init__(
        self,
        test_size: float = 0.2,
        random_state: int = 42,
        contamination: float = None,
    ):
        """
        Args:
            test_size:      Fraction for test split (default 0.2)
            random_state:   Reproducibility seed
            contamination:  Expected anomaly ratio (default from config)
        """
        self.test_size = test_size
        self.random_state = random_state
        self.contamination = contamination or anomaly_config.contamination
        self.scaler = StandardScaler()

    # ------------------------------------------------------------------
    # DATA PREPARATION
    # ------------------------------------------------------------------

    def prepare_data(
        self,
        df: pd.DataFrame,
        feature_names: List[str],
        target_col: str = "anomaly_label"
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """
        Splits and scales data for anomaly detection.

        Args:
            df:            Feature matrix (from CryptoFeatureEngineer)
            feature_names: List of feature column names
            target_col:    Target column name (for evaluation only)

        Returns:
            (X_train, X_test, y_train, y_test) — scaled numpy arrays
        """
        logger.info("Preparing data for anomaly detection...")

        X = df[feature_names].values
        y = df[target_col].values if target_col in df.columns else np.zeros(len(df))

        X_train, X_test, y_train, y_test = train_test_split(
            X, y,
            test_size=self.test_size,
            random_state=self.random_state,
            shuffle=False  # time-series: preserve order
        )

        # Scale features
        X_train = self.scaler.fit_transform(X_train)
        X_test = self.scaler.transform(X_test)

        logger.info(f"  Train: {X_train.shape[0]} samples, Test: {X_test.shape[0]} samples")
        logger.info(f"  Features: {X_train.shape[1]}")
        anomaly_count = int(y_test.sum()) if target_col in df.columns else 0
        logger.info(f"  Anomalies in test set (labels): {anomaly_count}")

        return X_train, X_test, y_train, y_test

    # ------------------------------------------------------------------
    # ISOLATION FOREST
    # ------------------------------------------------------------------

    def train_isolation_forest(
        self,
        X_train: np.ndarray,
        X_test: np.ndarray,
        y_test: np.ndarray,
        params: Optional[Dict] = None
    ) -> Tuple[IsolationForest, Dict[str, float]]:
        """
        Trains an Isolation Forest for unsupervised anomaly detection.

        Args:
            X_train:  Training features (scaled)
            X_test:   Test features (scaled)
            y_test:   Test labels (for evaluation only)
            params:   Optional hyperparameters override

        Returns:
            (model, metrics_dict)
        """
        logger.info("=" * 50)
        logger.info("Training Isolation Forest...")
        logger.info("=" * 50)

        default_params = {
            "contamination": self.contamination,
            "n_estimators": anomaly_config.n_estimators,
            "random_state": self.random_state,
            "n_jobs": -1,
        }
        if params:
            default_params.update(params)

        model = IsolationForest(**default_params)
        model.fit(X_train)

        # Predict: IsolationForest returns -1 for anomalies, 1 for normal
        raw_preds = model.predict(X_test)
        y_pred = (raw_preds == -1).astype(int)  # Convert to 0/1

        metrics = self._compute_metrics(y_test, y_pred)
        metrics["anomalies_detected"] = int(y_pred.sum())

        logger.info(f"  Precision: {metrics['precision']:.4f}")
        logger.info(f"  Recall:    {metrics['recall']:.4f}")
        logger.info(f"  F1 Score:  {metrics['f1']:.4f}")
        logger.info(f"  Anomalies detected: {metrics['anomalies_detected']}")

        return model, metrics

    # ------------------------------------------------------------------
    # AUTOENCODER
    # ------------------------------------------------------------------

    def train_autoencoder(
        self,
        X_train: np.ndarray,
        X_test: np.ndarray,
        y_test: np.ndarray,
        epochs: int = None,
        batch_size: int = None,
        latent_dim: int = None,
    ) -> Tuple[Any, float, Dict[str, float]]:
        """
        Trains an Autoencoder for reconstruction error-based anomaly detection.

        Architecture:
            Encoder: Dense(32, relu) -> Dense(latent_dim, relu)
            Decoder: Dense(32, relu) -> Dense(n_features, sigmoid)

        Args:
            X_train:    Training features (scaled)
            X_test:     Test features (scaled)
            y_test:     Test labels (for evaluation only)
            epochs:     Training epochs (default from config)
            batch_size: Batch size (default from config)
            latent_dim: Bottleneck dimension (default from config)

        Returns:
            (model, threshold, metrics_dict)
        """
        logger.info("=" * 50)
        logger.info("Training Autoencoder...")
        logger.info("=" * 50)

        tf = _import_tensorflow()

        epochs = epochs or anomaly_config.autoencoder_epochs
        batch_size = batch_size or anomaly_config.autoencoder_batch_size
        latent_dim = latent_dim or anomaly_config.autoencoder_latent_dim
        n_features = X_train.shape[1]

        # Build autoencoder
        model = tf.keras.Sequential([
            # Encoder
            tf.keras.layers.Input(shape=(n_features,)),
            tf.keras.layers.Dense(32, activation="relu"),
            tf.keras.layers.Dense(latent_dim, activation="relu"),
            # Decoder
            tf.keras.layers.Dense(32, activation="relu"),
            tf.keras.layers.Dense(n_features, activation="sigmoid"),
        ])

        model.compile(
            optimizer=tf.keras.optimizers.Adam(learning_rate=0.001),
            loss="mse",
        )

        # Train (reconstruct the input)
        model.fit(
            X_train, X_train,
            epochs=epochs,
            batch_size=batch_size,
            validation_data=(X_test, X_test),
            verbose=0,
        )

        # Compute reconstruction errors on training set to determine threshold
        train_recon = model.predict(X_train, verbose=0)
        train_errors = np.mean(np.square(X_train - train_recon), axis=1)
        threshold = float(np.percentile(
            train_errors, anomaly_config.reconstruction_threshold_percentile
        ))

        # Evaluate on test set
        test_recon = model.predict(X_test, verbose=0)
        test_errors = np.mean(np.square(X_test - test_recon), axis=1)
        y_pred = (test_errors > threshold).astype(int)

        metrics = self._compute_metrics(y_test, y_pred)
        metrics["anomalies_detected"] = int(y_pred.sum())
        metrics["threshold"] = threshold

        logger.info(f"  Precision:  {metrics['precision']:.4f}")
        logger.info(f"  Recall:     {metrics['recall']:.4f}")
        logger.info(f"  F1 Score:   {metrics['f1']:.4f}")
        logger.info(f"  Threshold:  {threshold:.6f}")
        logger.info(f"  Anomalies detected: {metrics['anomalies_detected']}")

        return model, threshold, metrics

    # ------------------------------------------------------------------
    # PREDICTION
    # ------------------------------------------------------------------

    def predict_isolation_forest(
        self,
        model: IsolationForest,
        X: np.ndarray,
    ) -> np.ndarray:
        """
        Generates anomaly predictions using a trained Isolation Forest.

        Returns:
            Binary array: 1 = anomaly, 0 = normal
        """
        raw_preds = model.predict(X)
        return (raw_preds == -1).astype(int)

    def predict_autoencoder(
        self,
        model: Any,
        X: np.ndarray,
        threshold: float,
    ) -> np.ndarray:
        """
        Generates anomaly predictions using a trained Autoencoder.

        Returns:
            Binary array: 1 = anomaly, 0 = normal
        """
        recon = model.predict(X, verbose=0)
        errors = np.mean(np.square(X - recon), axis=1)
        return (errors > threshold).astype(int)

    # ------------------------------------------------------------------
    # METRICS
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> Dict[str, float]:
        """Computes anomaly detection metrics."""
        return {
            "precision": float(precision_score(y_true, y_pred, zero_division=0)),
            "recall": float(recall_score(y_true, y_pred, zero_division=0)),
            "f1": float(f1_score(y_true, y_pred, zero_division=0)),
        }

    # ------------------------------------------------------------------
    # MLFLOW LOGGING
    # ------------------------------------------------------------------

    @staticmethod
    def log_to_mlflow(
        model: Any,
        model_type: str,
        params: Dict[str, Any],
        metrics: Dict[str, float],
        feature_names: Optional[List[str]] = None,
    ) -> Optional[str]:
        """
        Logs model, parameters, and metrics to MLflow.

        Args:
            model:         Trained model
            model_type:    'isolation_forest' or 'autoencoder'
            params:        Hyperparameters dict
            metrics:       Metrics dict
            feature_names: Optional list of feature names

        Returns:
            MLflow run_id or None if MLflow is not available
        """
        try:
            import mlflow
            import mlflow.sklearn
            import mlflow.keras
        except ImportError:
            logger.warning("MLflow not available, skipping logging")
            return None

        try:
            mlflow.set_tracking_uri(model_config.mlflow_tracking_uri)
            mlflow.set_experiment(anomaly_config.experiment_name)

            run_name = f"{model_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            with mlflow.start_run(run_name=run_name):
                mlflow.log_params(params)

                # Filter out non-numeric metrics for MLflow
                numeric_metrics = {
                    k: v for k, v in metrics.items() if isinstance(v, (int, float))
                }
                mlflow.log_metrics(numeric_metrics)

                if model_type == "isolation_forest":
                    try:
                        mlflow.sklearn.log_model(model, "model")
                    except Exception as model_err:
                        logger.warning(f"Native log_model failed ({model_err}), using joblib fallback")
                        import tempfile, joblib as _joblib
                        with tempfile.TemporaryDirectory() as tmpdir:
                            path = f"{tmpdir}/{model_type}_model.joblib"
                            _joblib.dump(model, path)
                            mlflow.log_artifact(path, "model")
                elif model_type == "autoencoder":
                    try:
                        mlflow.keras.log_model(model, "model")
                    except Exception as model_err:
                        logger.warning(f"Native log_model failed ({model_err}), using joblib fallback")
                        import tempfile, joblib as _joblib
                        with tempfile.TemporaryDirectory() as tmpdir:
                            path = f"{tmpdir}/{model_type}_model.joblib"
                            _joblib.dump(model, path)
                            mlflow.log_artifact(path, "model")

                if feature_names:
                    mlflow.log_text(
                        json.dumps(feature_names, indent=2),
                        "feature_names.json"
                    )

                run_id = mlflow.active_run().info.run_id
                logger.info(f"  Logged to MLflow (run_id: {run_id})")
                return run_id

        except Exception as e:
            logger.warning(f"MLflow logging failed: {e}")
            return None

    # ------------------------------------------------------------------
    # SAVE / LOAD
    # ------------------------------------------------------------------

    @staticmethod
    def save_model(model: Any, path: str, model_type: str = "isolation_forest") -> str:
        """
        Saves a trained model to disk.

        Args:
            model:      Trained model
            path:       File path (without extension)
            model_type: 'isolation_forest' or 'autoencoder'

        Returns:
            Full path of saved model
        """
        os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)

        if model_type == "isolation_forest":
            full_path = f"{path}.joblib"
            joblib.dump(model, full_path)
        elif model_type == "autoencoder":
            full_path = f"{path}.keras"
            model.save(full_path)
        else:
            raise ValueError(f"Unknown model type: {model_type}")

        logger.info(f"  Model saved to {full_path}")
        return full_path

    @staticmethod
    def load_model(path: str, model_type: str = "isolation_forest") -> Any:
        """
        Loads a trained model from disk.

        Args:
            path:       Full file path
            model_type: 'isolation_forest' or 'autoencoder'

        Returns:
            Loaded model
        """
        if model_type == "isolation_forest":
            return joblib.load(path)
        elif model_type == "autoencoder":
            tf = _import_tensorflow()
            return tf.keras.models.load_model(path)
        else:
            raise ValueError(f"Unknown model type: {model_type}")

    # ------------------------------------------------------------------
    # SAVE / LOAD SCALER & THRESHOLD
    # ------------------------------------------------------------------

    def save_scaler(self, path: str) -> str:
        """Saves the fitted StandardScaler to disk."""
        full_path = f"{path}_scaler.joblib"
        os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)
        joblib.dump(self.scaler, full_path)
        logger.info(f"  Scaler saved to {full_path}")
        return full_path

    def load_scaler(self, path: str):
        """Loads a saved StandardScaler."""
        self.scaler = joblib.load(path)
        logger.info(f"  Scaler loaded from {path}")

    @staticmethod
    def save_threshold(threshold: float, path: str) -> str:
        """Saves the autoencoder threshold to disk."""
        full_path = f"{path}_threshold.json"
        os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)
        with open(full_path, "w") as f:
            json.dump({"threshold": threshold}, f)
        logger.info(f"  Threshold saved to {full_path}")
        return full_path

    @staticmethod
    def load_threshold(path: str) -> float:
        """Loads a saved autoencoder threshold."""
        with open(path, "r") as f:
            data = json.load(f)
        return data["threshold"]

    # ------------------------------------------------------------------
    # MONGODB PERSISTENCE
    # ------------------------------------------------------------------

    @staticmethod
    def save_anomalies_to_mongodb(
        df: pd.DataFrame,
        anomaly_mask: np.ndarray,
        symbol: str,
        interval: str,
        model_type: str = "isolation_forest",
        mongo_uri: Optional[str] = None,
        database: Optional[str] = None,
    ) -> int:
        """
        Saves detected anomalies to the MongoDB 'anomalies' collection.

        Args:
            df:           Original DataFrame (with timestamps)
            anomaly_mask: Boolean/int array (1 = anomaly)
            symbol:       Trading pair
            interval:     Candle interval
            model_type:   Model that detected the anomalies
            mongo_uri:    MongoDB URI (default from config)
            database:     Database name (default from config)

        Returns:
            Number of anomalies inserted
        """
        uri = mongo_uri or mongodb_config.uri
        db_name = database or mongodb_config.database

        anomaly_indices = np.where(anomaly_mask == 1)[0]
        if len(anomaly_indices) == 0:
            logger.info("  No anomalies to save")
            return 0

        anomaly_rows = df.iloc[anomaly_indices].copy()

        records = []
        for _, row in anomaly_rows.iterrows():
            record = {
                "symbol": symbol,
                "interval": interval,
                "timestamp": row.get("timestamp", datetime.now()),
                "model_type": model_type,
                "detected_at": datetime.now(),
            }
            # Include key columns if present
            for col in ["close", "volume", "return_1", "volatility_5"]:
                if col in row.index:
                    record[col] = float(row[col])
            records.append(record)

        client = MongoClient(uri)
        try:
            db = client[db_name]
            collection = db[mongodb_config.anomalies_collection]
            result = collection.insert_many(records)
            count = len(result.inserted_ids)
            logger.info(f"  Saved {count} anomalies to MongoDB ({mongodb_config.anomalies_collection})")
            return count
        finally:
            client.close()

    # ------------------------------------------------------------------
    # FULL PIPELINE: DETECT & COMPARE
    # ------------------------------------------------------------------

    def detect_and_compare(
        self,
        df: pd.DataFrame,
        feature_names: List[str],
        target_col: str = "anomaly_label",
        if_params: Optional[Dict] = None,
        ae_epochs: int = None,
        ae_batch_size: int = None,
        log_mlflow: bool = False,
    ) -> Dict[str, Any]:
        """
        Trains both Isolation Forest and Autoencoder, compares them.

        Args:
            df:              Feature matrix
            feature_names:   Feature columns
            target_col:      Target column (for evaluation)
            if_params:       Optional Isolation Forest hyperparameters
            ae_epochs:       Autoencoder epochs
            ae_batch_size:   Autoencoder batch size
            log_mlflow:      Whether to log to MLflow

        Returns:
            Dict with: best_model_name, if_metrics, ae_metrics,
                       if_model, ae_model, ae_threshold
        """
        logger.info("=" * 60)
        logger.info("DETECTING & COMPARING ANOMALY MODELS")
        logger.info("=" * 60)

        # Prepare data
        X_train, X_test, y_train, y_test = self.prepare_data(
            df, feature_names, target_col
        )

        # Train Isolation Forest
        if_model, if_metrics = self.train_isolation_forest(
            X_train, X_test, y_test, if_params
        )

        # Train Autoencoder
        ae_model, ae_threshold, ae_metrics = self.train_autoencoder(
            X_train, X_test, y_test,
            epochs=ae_epochs, batch_size=ae_batch_size
        )

        # Compare by F1 score
        best_name = (
            "isolation_forest" if if_metrics["f1"] >= ae_metrics["f1"]
            else "autoencoder"
        )

        logger.info("")
        logger.info("=" * 60)
        logger.info("COMPARISON RESULTS")
        logger.info("=" * 60)
        logger.info(f"  Isolation Forest F1: {if_metrics['f1']:.4f}")
        logger.info(f"  Autoencoder      F1: {ae_metrics['f1']:.4f}")
        logger.info(f"  Best model: {best_name.upper()}")
        logger.info("=" * 60)

        # Log to MLflow
        if log_mlflow:
            if_log_params = if_params or {
                "contamination": self.contamination,
                "n_estimators": anomaly_config.n_estimators,
                "model_type": "isolation_forest",
            }
            self.log_to_mlflow(
                if_model, "isolation_forest", if_log_params,
                if_metrics, feature_names
            )

            ae_log_params = {
                "epochs": ae_epochs or anomaly_config.autoencoder_epochs,
                "batch_size": ae_batch_size or anomaly_config.autoencoder_batch_size,
                "latent_dim": anomaly_config.autoencoder_latent_dim,
                "threshold": ae_threshold,
                "model_type": "autoencoder",
            }
            self.log_to_mlflow(
                ae_model, "autoencoder", ae_log_params,
                ae_metrics, feature_names
            )

        return {
            "best_model_name": best_name,
            "if_metrics": if_metrics,
            "ae_metrics": ae_metrics,
            "if_model": if_model,
            "ae_model": ae_model,
            "ae_threshold": ae_threshold,
        }

"""
Model Training Module - P4 (Abdessamad)
Short-term price prediction using XGBoost and LSTM.
Integrates with MLflow for experiment tracking and model versioning.

Usage:
    from ml.feature_engineering import CryptoFeatureEngineer
    from ml.model_training import CryptoPricePredictor

    fe = CryptoFeatureEngineer()
    df = fe.build_feature_matrix("BTCUSDT", "1m", limit=5000)

    predictor = CryptoPricePredictor()
    results = predictor.train_and_compare(df, fe.get_feature_names(df))
"""

import logging
import os
import json
import joblib
import numpy as np
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    classification_report
)
from xgboost import XGBClassifier

from .config import model_config, feature_config

logger = logging.getLogger(__name__)

# Suppress TensorFlow INFO logs
os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "2")


def _import_tensorflow():
    """Lazy import TensorFlow to avoid slow startup when not needed."""
    import tensorflow as tf
    return tf


class CryptoPricePredictor:
    """
    Trains and evaluates XGBoost and LSTM models for short-term crypto
    price direction prediction (binary: up=1 / down=0).

    Features:
        - XGBoost: tabular classifier on the flat feature matrix
        - LSTM: sequence-based classifier using a sliding window
        - MLflow: automatic experiment logging
        - Comparison: train both, pick the best by F1 score
    """

    def __init__(
        self,
        test_size: float = 0.2,
        random_state: int = 42,
        sequence_length: int = 10
    ):
        """
        Args:
            test_size:       Fraction for test split (default 0.2)
            random_state:    Reproducibility seed
            sequence_length: Number of past time-steps for LSTM input
        """
        self.test_size = test_size
        self.random_state = random_state
        self.sequence_length = sequence_length
        self.scaler = StandardScaler()

    # ------------------------------------------------------------------
    # DATA PREPARATION
    # ------------------------------------------------------------------

    def prepare_data(
        self,
        df: pd.DataFrame,
        feature_names: List[str],
        target_col: str = "target_direction"
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """
        Splits and scales data for model training.

        Args:
            df:            Feature matrix (from CryptoFeatureEngineer)
            feature_names: List of feature column names
            target_col:    Target column name

        Returns:
            (X_train, X_test, y_train, y_test) — scaled numpy arrays
        """
        logger.info("Preparing data for training...")

        X = df[feature_names].values
        y = df[target_col].values

        X_train, X_test, y_train, y_test = train_test_split(
            X, y,
            test_size=self.test_size,
            random_state=self.random_state,
            shuffle=False  # time-series: no shuffle to preserve order
        )

        # Scale features
        X_train = self.scaler.fit_transform(X_train)
        X_test = self.scaler.transform(X_test)

        logger.info(f"  Train: {X_train.shape[0]} samples, Test: {X_test.shape[0]} samples")
        logger.info(f"  Features: {X_train.shape[1]}")
        logger.info(f"  Target distribution (train): {np.bincount(y_train.astype(int))}")

        return X_train, X_test, y_train, y_test

    @staticmethod
    def create_sequences(
        X: np.ndarray,
        y: np.ndarray,
        seq_length: int = 10
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        Creates sliding-window sequences for LSTM input.

        Args:
            X: Feature array (n_samples, n_features)
            y: Target array (n_samples,)
            seq_length: Number of past time-steps per sample

        Returns:
            (X_seq, y_seq) where X_seq has shape (n_samples, seq_length, n_features)
        """
        X_seq, y_seq = [], []
        for i in range(seq_length, len(X)):
            X_seq.append(X[i - seq_length:i])
            y_seq.append(y[i])

        return np.array(X_seq), np.array(y_seq)

    # ------------------------------------------------------------------
    # XGBOOST
    # ------------------------------------------------------------------

    def train_xgboost(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_test: np.ndarray,
        y_test: np.ndarray,
        params: Optional[Dict] = None
    ) -> Tuple[XGBClassifier, Dict[str, float]]:
        """
        Trains an XGBoost classifier for price direction.

        Args:
            X_train, y_train: Training data
            X_test, y_test:   Test data
            params:           Optional hyperparameters override

        Returns:
            (model, metrics_dict)
        """
        logger.info("=" * 50)
        logger.info("Training XGBoost model...")
        logger.info("=" * 50)

        default_params = {
            "max_depth": 6,
            "n_estimators": 100,
            "learning_rate": 0.1,
            "objective": "binary:logistic",
            "eval_metric": "logloss",
            "use_label_encoder": False,
            "random_state": self.random_state,
            "n_jobs": -1,
        }
        if params:
            default_params.update(params)

        model = XGBClassifier(**default_params)
        model.fit(
            X_train, y_train,
            eval_set=[(X_test, y_test)],
            verbose=False,
        )

        # Evaluate
        y_pred = model.predict(X_test)
        metrics = self._compute_metrics(y_test, y_pred)

        logger.info(f"  Accuracy:  {metrics['accuracy']:.4f}")
        logger.info(f"  Precision: {metrics['precision']:.4f}")
        logger.info(f"  Recall:    {metrics['recall']:.4f}")
        logger.info(f"  F1 Score:  {metrics['f1']:.4f}")

        return model, metrics

    # ------------------------------------------------------------------
    # LSTM
    # ------------------------------------------------------------------

    def train_lstm(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_test: np.ndarray,
        y_test: np.ndarray,
        epochs: int = 50,
        batch_size: int = 32,
    ) -> Tuple[Any, Dict[str, float]]:
        """
        Trains an LSTM classifier for price direction.

        Architecture:
            LSTM(64) → Dropout(0.2) → LSTM(32) → Dropout(0.2) → Dense(1, sigmoid)

        Args:
            X_train, y_train: Training data (flat, will be sequenced)
            X_test, y_test:   Test data (flat, will be sequenced)
            epochs:           Training epochs
            batch_size:       Batch size

        Returns:
            (model, metrics_dict)
        """
        logger.info("=" * 50)
        logger.info("Training LSTM model...")
        logger.info("=" * 50)

        tf = _import_tensorflow()

        # Create sequences
        X_train_seq, y_train_seq = self.create_sequences(
            X_train, y_train, self.sequence_length
        )
        X_test_seq, y_test_seq = self.create_sequences(
            X_test, y_test, self.sequence_length
        )

        if len(X_train_seq) == 0 or len(X_test_seq) == 0:
            logger.error("Not enough data to create sequences")
            return None, {"accuracy": 0, "precision": 0, "recall": 0, "f1": 0}

        n_features = X_train_seq.shape[2]
        logger.info(f"  Sequence shape: {X_train_seq.shape}")

        # Build model
        model = tf.keras.Sequential([
            tf.keras.layers.Input(shape=(self.sequence_length, n_features)),
            tf.keras.layers.LSTM(64, return_sequences=True),
            tf.keras.layers.Dropout(0.2),
            tf.keras.layers.LSTM(32, return_sequences=False),
            tf.keras.layers.Dropout(0.2),
            tf.keras.layers.Dense(1, activation="sigmoid"),
        ])

        model.compile(
            optimizer=tf.keras.optimizers.Adam(learning_rate=0.001),
            loss="binary_crossentropy",
            metrics=["accuracy"],
        )

        # Train
        model.fit(
            X_train_seq, y_train_seq,
            epochs=epochs,
            batch_size=batch_size,
            validation_data=(X_test_seq, y_test_seq),
            verbose=0,
        )

        # Evaluate
        y_pred_proba = model.predict(X_test_seq, verbose=0).flatten()
        y_pred = (y_pred_proba >= 0.5).astype(int)
        metrics = self._compute_metrics(y_test_seq, y_pred)

        logger.info(f"  Accuracy:  {metrics['accuracy']:.4f}")
        logger.info(f"  Precision: {metrics['precision']:.4f}")
        logger.info(f"  Recall:    {metrics['recall']:.4f}")
        logger.info(f"  F1 Score:  {metrics['f1']:.4f}")

        return model, metrics

    # ------------------------------------------------------------------
    # METRICS
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> Dict[str, float]:
        """Computes classification metrics."""
        return {
            "accuracy": float(accuracy_score(y_true, y_pred)),
            "precision": float(precision_score(y_true, y_pred, zero_division=0)),
            "recall": float(recall_score(y_true, y_pred, zero_division=0)),
            "f1": float(f1_score(y_true, y_pred, zero_division=0)),
        }

    # ------------------------------------------------------------------
    # PREDICTION
    # ------------------------------------------------------------------

    def predict(
        self,
        model: Any,
        X: np.ndarray,
        model_type: str = "xgboost"
    ) -> np.ndarray:
        """
        Generates predictions using a trained model.

        Args:
            model:      Trained model (XGBClassifier or Keras model)
            X:          Feature array (scaled)
            model_type: 'xgboost' or 'lstm'

        Returns:
            Binary prediction array (0 or 1)
        """
        if model_type == "xgboost":
            return model.predict(X)
        elif model_type == "lstm":
            X_seq, _ = self.create_sequences(
                X, np.zeros(len(X)), self.sequence_length
            )
            if len(X_seq) == 0:
                return np.array([])
            proba = model.predict(X_seq, verbose=0).flatten()
            return (proba >= 0.5).astype(int)
        else:
            raise ValueError(f"Unknown model type: {model_type}")

    # ------------------------------------------------------------------
    # MLFLOW LOGGING
    # ------------------------------------------------------------------

    @staticmethod
    def log_to_mlflow(
        model: Any,
        model_type: str,
        params: Dict[str, Any],
        metrics: Dict[str, float],
        feature_names: Optional[List[str]] = None
    ) -> Optional[str]:
        """
        Logs model, parameters, and metrics to MLflow.

        Args:
            model:         Trained model
            model_type:    'xgboost' or 'lstm'
            params:        Hyperparameters dict
            metrics:       Metrics dict
            feature_names: Optional list of feature names

        Returns:
            MLflow run_id or None if MLflow is not available
        """
        try:
            import mlflow
            import mlflow.xgboost
            import mlflow.keras
        except ImportError:
            logger.warning("MLflow not available, skipping logging")
            return None

        try:
            mlflow.set_tracking_uri(model_config.mlflow_tracking_uri)
            mlflow.set_experiment(model_config.experiment_name)

            with mlflow.start_run(run_name=f"{model_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
                # Log parameters
                mlflow.log_params(params)

                # Log metrics
                mlflow.log_metrics(metrics)

                # Log model
                if model_type == "xgboost":
                    mlflow.xgboost.log_model(model, "model")
                elif model_type == "lstm":
                    mlflow.keras.log_model(model, "model")

                # Log feature names
                if feature_names:
                    mlflow.log_text(
                        json.dumps(feature_names, indent=2),
                        "feature_names.json"
                    )

                run_id = mlflow.active_run().info.run_id
                logger.info(f"  ✅ Logged to MLflow (run_id: {run_id})")
                return run_id

        except Exception as e:
            logger.warning(f"MLflow logging failed: {e}")
            return None

    # ------------------------------------------------------------------
    # SAVE / LOAD
    # ------------------------------------------------------------------

    @staticmethod
    def save_model(model: Any, path: str, model_type: str = "xgboost") -> str:
        """
        Saves a trained model to disk.

        Args:
            model:      Trained model
            path:       File path (without extension)
            model_type: 'xgboost' or 'lstm'

        Returns:
            Full path of saved model
        """
        os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)

        if model_type == "xgboost":
            full_path = f"{path}.joblib"
            joblib.dump(model, full_path)
        elif model_type == "lstm":
            full_path = f"{path}.keras"
            model.save(full_path)
        else:
            raise ValueError(f"Unknown model type: {model_type}")

        logger.info(f"  Model saved to {full_path}")
        return full_path

    @staticmethod
    def load_model(path: str, model_type: str = "xgboost") -> Any:
        """
        Loads a trained model from disk.

        Args:
            path:       Full file path
            model_type: 'xgboost' or 'lstm'

        Returns:
            Loaded model
        """
        if model_type == "xgboost":
            return joblib.load(path)
        elif model_type == "lstm":
            tf = _import_tensorflow()
            return tf.keras.models.load_model(path)
        else:
            raise ValueError(f"Unknown model type: {model_type}")

    # ------------------------------------------------------------------
    # SAVE / LOAD SCALER
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

    # ------------------------------------------------------------------
    # FULL PIPELINE: TRAIN & COMPARE
    # ------------------------------------------------------------------

    def train_and_compare(
        self,
        df: pd.DataFrame,
        feature_names: List[str],
        target_col: str = "target_direction",
        xgboost_params: Optional[Dict] = None,
        lstm_epochs: int = 50,
        lstm_batch_size: int = 32,
        log_mlflow: bool = False
    ) -> Dict[str, Any]:
        """
        Trains both XGBoost and LSTM, compares them, returns the results.

        Args:
            df:              Feature matrix
            feature_names:   Feature columns
            target_col:      Target column
            xgboost_params:  Optional XGBoost hyperparameters
            lstm_epochs:     LSTM epochs
            lstm_batch_size: LSTM batch size
            log_mlflow:      Whether to log to MLflow

        Returns:
            Dict with keys: best_model_name, xgboost_metrics, lstm_metrics,
                            xgboost_model, lstm_model
        """
        logger.info("=" * 60)
        logger.info("TRAINING & COMPARING MODELS")
        logger.info("=" * 60)

        # Prepare data
        X_train, X_test, y_train, y_test = self.prepare_data(
            df, feature_names, target_col
        )

        # Train XGBoost
        xgb_model, xgb_metrics = self.train_xgboost(
            X_train, y_train, X_test, y_test, xgboost_params
        )

        # Train LSTM
        lstm_model, lstm_metrics = self.train_lstm(
            X_train, y_train, X_test, y_test,
            epochs=lstm_epochs, batch_size=lstm_batch_size
        )

        # Compare by F1 score
        best_name = "xgboost" if xgb_metrics["f1"] >= lstm_metrics["f1"] else "lstm"

        logger.info("")
        logger.info("=" * 60)
        logger.info("COMPARISON RESULTS")
        logger.info("=" * 60)
        logger.info(f"  XGBoost F1: {xgb_metrics['f1']:.4f}")
        logger.info(f"  LSTM    F1: {lstm_metrics['f1']:.4f}")
        logger.info(f"  🏆 Best model: {best_name.upper()}")
        logger.info("=" * 60)

        # Log to MLflow
        if log_mlflow:
            xgb_params = xgboost_params or {
                "max_depth": 6, "n_estimators": 100,
                "learning_rate": 0.1, "model_type": "xgboost"
            }
            self.log_to_mlflow(xgb_model, "xgboost", xgb_params, xgb_metrics, feature_names)

            lstm_params = {
                "sequence_length": self.sequence_length,
                "epochs": lstm_epochs, "batch_size": lstm_batch_size,
                "lstm_units_1": 64, "lstm_units_2": 32,
                "dropout": 0.2, "model_type": "lstm"
            }
            self.log_to_mlflow(lstm_model, "lstm", lstm_params, lstm_metrics, feature_names)

        return {
            "best_model_name": best_name,
            "xgboost_metrics": xgb_metrics,
            "lstm_metrics": lstm_metrics,
            "xgboost_model": xgb_model,
            "lstm_model": lstm_model,
        }

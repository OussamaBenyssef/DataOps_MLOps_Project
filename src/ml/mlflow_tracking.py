"""
MLflow Tracking & Experiments Module - P4 (Abdessamad)
Centralized experiment management: run training pipelines, log to MLflow,
register models, compare experiments.

Usage:
    from ml.mlflow_tracking import MLflowExperimentTracker

    tracker = MLflowExperimentTracker()
    results = tracker.run_prediction_experiment(df, feature_names)
    tracker.register_best_model("crypto-price-prediction", "f1")
"""

import logging
import os
import json
import numpy as np
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Any

import mlflow
import mlflow.xgboost
import mlflow.sklearn
from mlflow.tracking import MlflowClient

from .config import model_config, anomaly_config
from .model_training import CryptoPricePredictor
from .anomaly_detection import CryptoAnomalyDetector

logger = logging.getLogger(__name__)


def _import_mlflow_keras():
    """Lazy import mlflow.keras to avoid TensorFlow startup."""
    import mlflow.keras
    return mlflow.keras


class MLflowExperimentTracker:
    """
    Orchestrates ML experiment runs with full MLflow tracking.

    Ties together CryptoPricePredictor and CryptoAnomalyDetector with
    MLflow experiment logging, model registry, and comparison utilities.
    """

    def __init__(
        self,
        tracking_uri: Optional[str] = None,
    ):
        """
        Args:
            tracking_uri: MLflow tracking server URI (default from config)
        """
        self.tracking_uri = tracking_uri or model_config.mlflow_tracking_uri
        mlflow.set_tracking_uri(self.tracking_uri)
        self.client = MlflowClient(self.tracking_uri)

    # ------------------------------------------------------------------
    # EXPERIMENT RUNNERS
    # ------------------------------------------------------------------

    def run_prediction_experiment(
        self,
        df: pd.DataFrame,
        feature_names: List[str],
        experiment_name: Optional[str] = None,
        xgboost_params: Optional[Dict] = None,
        lstm_epochs: int = 50,
        lstm_batch_size: int = 32,
        symbol: str = "BTCUSDT",
        interval: str = "1m",
    ) -> Dict[str, Any]:
        """
        Runs a full price prediction experiment: trains XGBoost + LSTM,
        logs everything to MLflow under a single experiment.

        Args:
            df:              Feature matrix
            feature_names:   Feature columns
            experiment_name: MLflow experiment name (default from config)
            xgboost_params:  Optional XGBoost hyperparameters
            lstm_epochs:     LSTM training epochs
            lstm_batch_size: LSTM batch size
            symbol:          Trading pair (logged as tag)
            interval:        Candle interval (logged as tag)

        Returns:
            Dict with run_ids and metrics for both models
        """
        exp_name = experiment_name or model_config.experiment_name
        mlflow.set_experiment(exp_name)

        logger.info("=" * 60)
        logger.info(f"PREDICTION EXPERIMENT: {exp_name}")
        logger.info("=" * 60)

        predictor = CryptoPricePredictor()
        X_train, X_test, y_train, y_test = predictor.prepare_data(
            df, feature_names
        )

        results = {}

        # --- XGBoost Run ---
        xgb_params = xgboost_params or {
            "max_depth": 6, "n_estimators": 100, "learning_rate": 0.1,
        }
        with mlflow.start_run(run_name=f"xgboost_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
            mlflow.set_tags({
                "model_type": "xgboost",
                "symbol": symbol,
                "interval": interval,
                "task": "price_prediction",
            })
            mlflow.log_params({
                "max_depth": xgb_params.get("max_depth", 6),
                "n_estimators": xgb_params.get("n_estimators", 100),
                "learning_rate": xgb_params.get("learning_rate", 0.1),
                "test_size": predictor.test_size,
                "n_features": len(feature_names),
                "n_train_samples": X_train.shape[0],
            })

            xgb_model, xgb_metrics = predictor.train_xgboost(
                X_train, y_train, X_test, y_test, xgb_params
            )

            mlflow.log_metrics(xgb_metrics)
            mlflow.xgboost.log_model(xgb_model, "model")
            mlflow.log_text(
                json.dumps(feature_names, indent=2), "feature_names.json"
            )

            results["xgboost_run_id"] = mlflow.active_run().info.run_id
            results["xgboost_metrics"] = xgb_metrics

        logger.info(f"  XGBoost run: {results['xgboost_run_id']}")

        # --- LSTM Run ---
        mlflow_keras = _import_mlflow_keras()
        with mlflow.start_run(run_name=f"lstm_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
            mlflow.set_tags({
                "model_type": "lstm",
                "symbol": symbol,
                "interval": interval,
                "task": "price_prediction",
            })
            mlflow.log_params({
                "sequence_length": predictor.sequence_length,
                "epochs": lstm_epochs,
                "batch_size": lstm_batch_size,
                "lstm_units_1": 64,
                "lstm_units_2": 32,
                "dropout": 0.2,
                "test_size": predictor.test_size,
                "n_features": len(feature_names),
            })

            lstm_model, lstm_metrics = predictor.train_lstm(
                X_train, y_train, X_test, y_test,
                epochs=lstm_epochs, batch_size=lstm_batch_size,
            )

            mlflow.log_metrics(lstm_metrics)
            if lstm_model is not None:
                mlflow_keras.log_model(lstm_model, "model")

            results["lstm_run_id"] = mlflow.active_run().info.run_id
            results["lstm_metrics"] = lstm_metrics

        logger.info(f"  LSTM run: {results['lstm_run_id']}")

        # Best model
        results["best_model"] = (
            "xgboost" if xgb_metrics["f1"] >= lstm_metrics["f1"] else "lstm"
        )
        logger.info(f"  Best: {results['best_model'].upper()}")

        return results

    def run_anomaly_experiment(
        self,
        df: pd.DataFrame,
        feature_names: List[str],
        experiment_name: Optional[str] = None,
        if_params: Optional[Dict] = None,
        ae_epochs: int = 50,
        ae_batch_size: int = 32,
        symbol: str = "BTCUSDT",
        interval: str = "1m",
    ) -> Dict[str, Any]:
        """
        Runs a full anomaly detection experiment: trains Isolation Forest + Autoencoder,
        logs everything to MLflow.

        Returns:
            Dict with run_ids and metrics for both models
        """
        exp_name = experiment_name or anomaly_config.experiment_name
        mlflow.set_experiment(exp_name)

        logger.info("=" * 60)
        logger.info(f"ANOMALY EXPERIMENT: {exp_name}")
        logger.info("=" * 60)

        detector = CryptoAnomalyDetector()
        X_train, X_test, y_train, y_test = detector.prepare_data(
            df, feature_names
        )

        results = {}

        # --- Isolation Forest Run ---
        with mlflow.start_run(run_name=f"isolation_forest_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
            mlflow.set_tags({
                "model_type": "isolation_forest",
                "symbol": symbol,
                "interval": interval,
                "task": "anomaly_detection",
            })
            mlflow.log_params({
                "contamination": detector.contamination,
                "n_estimators": anomaly_config.n_estimators,
                "n_features": len(feature_names),
                "n_train_samples": X_train.shape[0],
            })

            if_model, if_metrics = detector.train_isolation_forest(
                X_train, X_test, y_test, if_params
            )

            numeric_metrics = {
                k: v for k, v in if_metrics.items() if isinstance(v, (int, float))
            }
            mlflow.log_metrics(numeric_metrics)
            mlflow.sklearn.log_model(if_model, "model")

            results["if_run_id"] = mlflow.active_run().info.run_id
            results["if_metrics"] = if_metrics

        logger.info(f"  IF run: {results['if_run_id']}")

        # --- Autoencoder Run ---
        mlflow_keras = _import_mlflow_keras()
        with mlflow.start_run(run_name=f"autoencoder_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
            mlflow.set_tags({
                "model_type": "autoencoder",
                "symbol": symbol,
                "interval": interval,
                "task": "anomaly_detection",
            })
            mlflow.log_params({
                "latent_dim": anomaly_config.autoencoder_latent_dim,
                "epochs": ae_epochs,
                "batch_size": ae_batch_size,
                "threshold_percentile": anomaly_config.reconstruction_threshold_percentile,
                "n_features": len(feature_names),
            })

            ae_model, ae_threshold, ae_metrics = detector.train_autoencoder(
                X_train, X_test, y_test,
                epochs=ae_epochs, batch_size=ae_batch_size,
            )

            numeric_metrics = {
                k: v for k, v in ae_metrics.items() if isinstance(v, (int, float))
            }
            mlflow.log_metrics(numeric_metrics)
            if ae_model is not None:
                mlflow_keras.log_model(ae_model, "model")
            mlflow.log_params({"ae_threshold": ae_threshold})

            results["ae_run_id"] = mlflow.active_run().info.run_id
            results["ae_metrics"] = ae_metrics
            results["ae_threshold"] = ae_threshold

        logger.info(f"  AE run: {results['ae_run_id']}")

        results["best_model"] = (
            "isolation_forest" if if_metrics["f1"] >= ae_metrics["f1"]
            else "autoencoder"
        )
        logger.info(f"  Best: {results['best_model'].upper()}")

        return results

    # ------------------------------------------------------------------
    # MODEL REGISTRY
    # ------------------------------------------------------------------

    def register_best_model(
        self,
        experiment_name: str,
        metric: str = "f1",
        model_name: Optional[str] = None,
    ) -> Optional[str]:
        """
        Finds the best run in an experiment and registers its model.

        Args:
            experiment_name: MLflow experiment name
            metric:          Metric to optimize (default: f1)
            model_name:      Registry name (default: experiment_name)

        Returns:
            Model version string, or None if failed
        """
        model_name = model_name or experiment_name.replace(" ", "-")
        best_run = self.get_best_run(experiment_name, metric)

        if best_run is None:
            logger.warning(f"No runs found in experiment '{experiment_name}'")
            return None

        run_id = best_run.info.run_id
        model_uri = f"runs:/{run_id}/model"

        try:
            result = mlflow.register_model(model_uri, model_name)
            version = result.version
            logger.info(
                f"  Registered model '{model_name}' v{version} "
                f"(run: {run_id}, {metric}: {best_run.data.metrics.get(metric, 'N/A')})"
            )
            return version
        except Exception as e:
            logger.error(f"Failed to register model: {e}")
            return None

    def promote_model(
        self,
        model_name: str,
        version: str,
        stage: str = "Production",
    ) -> bool:
        """
        Promotes a model version by setting an alias.

        Uses the modern alias-based API instead of the deprecated
        stage-based transition_model_version_stage.

        Args:
            model_name: Registry model name
            version:    Model version number
            stage:      Alias name (e.g. 'Staging', 'Production')

        Returns:
            True if successful
        """
        try:
            alias = stage.lower().replace(" ", "-")
            self.client.set_registered_model_alias(
                name=model_name,
                alias=alias,
                version=version,
            )
            logger.info(f"  Model '{model_name}' v{version} -> @{alias}")
            return True
        except Exception as e:
            logger.error(f"Failed to promote model: {e}")
            return False

    def load_production_model(self, model_name: str) -> Optional[Any]:
        """
        Loads the model with the 'production' alias from the registry.

        Args:
            model_name: Registry model name

        Returns:
            Loaded model or None
        """
        try:
            model_uri = f"models:/{model_name}@production"
            model = mlflow.pyfunc.load_model(model_uri)
            logger.info(f"  Loaded production model '{model_name}'")
            return model
        except Exception as e:
            logger.warning(f"No production model found for '{model_name}': {e}")
            return None

    # ------------------------------------------------------------------
    # QUERYING & COMPARISON
    # ------------------------------------------------------------------

    def get_best_run(
        self,
        experiment_name: str,
        metric: str = "f1",
        ascending: bool = False,
    ) -> Optional[Any]:
        """
        Returns the best run from an experiment by a given metric.

        Args:
            experiment_name: Experiment name
            metric:          Metric name to sort by
            ascending:       If True, lower is better (e.g. loss)

        Returns:
            mlflow.entities.Run or None
        """
        experiment = self.client.get_experiment_by_name(experiment_name)
        if experiment is None:
            logger.warning(f"Experiment '{experiment_name}' not found")
            return None

        order = "ASC" if ascending else "DESC"
        runs = self.client.search_runs(
            experiment_ids=[experiment.experiment_id],
            order_by=[f"metrics.{metric} {order}"],
            max_results=1,
        )

        if not runs:
            logger.warning(f"No runs found in '{experiment_name}'")
            return None

        return runs[0]

    def compare_runs(
        self,
        experiment_name: str,
        metric: str = "f1",
        top_n: int = 10,
    ) -> pd.DataFrame:
        """
        Returns a DataFrame comparing top N runs in an experiment.

        Args:
            experiment_name: Experiment name
            metric:          Metric to sort by
            top_n:           Number of runs to include

        Returns:
            DataFrame with run_id, model_type, params, and metrics
        """
        experiment = self.client.get_experiment_by_name(experiment_name)
        if experiment is None:
            logger.warning(f"Experiment '{experiment_name}' not found")
            return pd.DataFrame()

        runs = self.client.search_runs(
            experiment_ids=[experiment.experiment_id],
            order_by=[f"metrics.{metric} DESC"],
            max_results=top_n,
        )

        if not runs:
            return pd.DataFrame()

        rows = []
        for run in runs:
            row = {
                "run_id": run.info.run_id,
                "run_name": run.info.run_name or "",
                "model_type": run.data.tags.get("model_type", "unknown"),
                "status": run.info.status,
            }
            row.update(run.data.metrics)
            rows.append(row)

        df = pd.DataFrame(rows)
        logger.info(f"  Compared {len(df)} runs from '{experiment_name}'")
        return df

    def list_experiments(self) -> pd.DataFrame:
        """
        Lists all MLflow experiments with run counts.

        Returns:
            DataFrame with experiment_id, name, and run count
        """
        experiments = self.client.search_experiments()
        rows = []
        for exp in experiments:
            runs = self.client.search_runs(
                experiment_ids=[exp.experiment_id], max_results=1000
            )
            rows.append({
                "experiment_id": exp.experiment_id,
                "name": exp.name,
                "run_count": len(runs),
                "lifecycle_stage": exp.lifecycle_stage,
            })

        return pd.DataFrame(rows)

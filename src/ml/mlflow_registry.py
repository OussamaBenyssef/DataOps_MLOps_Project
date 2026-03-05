"""
MLflow Model Registry Module - P4 (Abdessamad)
Full model lifecycle management: register, stage, promote, validate, rollback.

Uses the modern alias-based API (not deprecated stages).

Usage:
    from ml.mlflow_registry import MLflowModelRegistry

    registry = MLflowModelRegistry()
    version = registry.register_model(run_id, "crypto-predictor")
    registry.promote_to_staging("crypto-predictor", version)
    registry.promote_to_production("crypto-predictor", version, min_f1=0.6)
"""

import logging
import os
import pandas as pd
from typing import Dict, List, Optional, Any

import mlflow
from mlflow.tracking import MlflowClient

from .config import model_config

logger = logging.getLogger(__name__)


class MLflowModelRegistry:
    """
    Manages the full model lifecycle in MLflow Model Registry.

    Workflow:
        1. register_model()         — Register a run's model artifact
        2. promote_to_staging()     — Set @staging alias for validation
        3. validate_before_promotion() — Check metric thresholds
        4. promote_to_production()  — Set @production alias (with validation)
        5. rollback_production()    — Revert to previous production version
        6. archive_model()          — Mark a version as @archived

    All stage transitions use the alias-based API (set_registered_model_alias).
    """

    def __init__(self, tracking_uri: Optional[str] = None):
        """
        Args:
            tracking_uri: MLflow tracking server URI (default from config)
        """
        self.tracking_uri = tracking_uri or model_config.mlflow_tracking_uri
        mlflow.set_tracking_uri(self.tracking_uri)
        self.client = MlflowClient(self.tracking_uri)

    # ------------------------------------------------------------------
    # REGISTRATION
    # ------------------------------------------------------------------

    def register_model(
        self,
        run_id: str,
        model_name: str,
        artifact_path: str = "model",
    ) -> Optional[str]:
        """
        Registers a model artifact from a specific MLflow run.

        Args:
            run_id:        MLflow run ID
            model_name:    Name in the model registry
            artifact_path: Artifact path within the run (default: "model")

        Returns:
            Version string, or None if failed
        """
        model_uri = f"runs:/{run_id}/{artifact_path}"
        try:
            result = mlflow.register_model(model_uri, model_name)
            version = result.version
            logger.info(
                f"Registered model '{model_name}' v{version} from run {run_id}"
            )
            return version
        except Exception as e:
            logger.error(f"Failed to register model: {e}")
            return None

    # ------------------------------------------------------------------
    # PROMOTION (Staging → Production)
    # ------------------------------------------------------------------

    def promote_to_staging(
        self,
        model_name: str,
        version: str,
    ) -> bool:
        """
        Promotes a model version to Staging by setting the @staging alias.

        Args:
            model_name: Registry model name
            version:    Model version number

        Returns:
            True if successful
        """
        try:
            self.client.set_registered_model_alias(
                name=model_name, alias="staging", version=version
            )
            logger.info(f"Model '{model_name}' v{version} -> @staging")
            return True
        except Exception as e:
            logger.error(f"Failed to promote to staging: {e}")
            return False

    def promote_to_production(
        self,
        model_name: str,
        version: str,
        min_f1: Optional[float] = None,
        min_accuracy: Optional[float] = None,
    ) -> bool:
        """
        Promotes a model version to Production by setting the @production alias.

        Optionally validates metric thresholds before promotion.

        Args:
            model_name:   Registry model name
            version:      Model version number
            min_f1:       Minimum F1 score required (None = skip check)
            min_accuracy: Minimum accuracy required (None = skip check)

        Returns:
            True if promoted, False if validation failed or error
        """
        # Validate if thresholds provided
        if min_f1 is not None or min_accuracy is not None:
            is_valid = self.validate_before_promotion(
                model_name, version,
                min_f1=min_f1,
                min_accuracy=min_accuracy,
            )
            if not is_valid:
                logger.warning(
                    f"Model '{model_name}' v{version} failed validation — NOT promoted"
                )
                return False

        try:
            self.client.set_registered_model_alias(
                name=model_name, alias="production", version=version
            )
            logger.info(f"Model '{model_name}' v{version} -> @production")
            return True
        except Exception as e:
            logger.error(f"Failed to promote to production: {e}")
            return False

    def validate_before_promotion(
        self,
        model_name: str,
        version: str,
        min_f1: Optional[float] = None,
        min_accuracy: Optional[float] = None,
    ) -> bool:
        """
        Validates a model version's metrics before promotion.

        Checks the run that produced this version against metric thresholds.

        Args:
            model_name:   Registry model name
            version:      Version to validate
            min_f1:       Minimum F1 score (None = skip)
            min_accuracy: Minimum accuracy (None = skip)

        Returns:
            True if all thresholds met, False otherwise
        """
        try:
            mv = self.client.get_model_version(model_name, version)
            run_id = mv.run_id
            run = self.client.get_run(run_id)
            metrics = run.data.metrics

            logger.info(f"Validating '{model_name}' v{version}:")

            if min_f1 is not None:
                actual_f1 = metrics.get("f1", 0.0)
                if actual_f1 < min_f1:
                    logger.warning(f"  F1 {actual_f1:.4f} < threshold {min_f1}")
                    return False
                logger.info(f"  F1 {actual_f1:.4f} >= {min_f1} ✓")

            if min_accuracy is not None:
                actual_acc = metrics.get("accuracy", 0.0)
                if actual_acc < min_accuracy:
                    logger.warning(
                        f"  Accuracy {actual_acc:.4f} < threshold {min_accuracy}"
                    )
                    return False
                logger.info(f"  Accuracy {actual_acc:.4f} >= {min_accuracy} ✓")

            return True
        except Exception as e:
            logger.error(f"Validation failed: {e}")
            return False

    # ------------------------------------------------------------------
    # MODEL LOADING
    # ------------------------------------------------------------------

    def load_model_by_alias(
        self,
        model_name: str,
        alias: str = "production",
    ) -> Optional[Any]:
        """
        Loads a model by its alias from the registry.

        Args:
            model_name: Registry model name
            alias:      Alias name (e.g. 'staging', 'production')

        Returns:
            Loaded model or None
        """
        try:
            model_uri = f"models:/{model_name}@{alias}"
            model = mlflow.pyfunc.load_model(model_uri)
            logger.info(f"Loaded model '{model_name}' @{alias}")
            return model
        except Exception as e:
            logger.warning(f"No model found for '{model_name}' @{alias}: {e}")
            return None

    def load_latest_model(self, model_name: str) -> Optional[Any]:
        """
        Loads the latest registered version of a model.

        Args:
            model_name: Registry model name

        Returns:
            Loaded model or None
        """
        try:
            versions = self.client.search_model_versions(
                f"name='{model_name}'",
            )
            if not versions:
                logger.warning(f"No versions found for '{model_name}'")
                return None

            latest = max(versions, key=lambda v: int(v.version))
            model_uri = f"models:/{model_name}/{latest.version}"
            model = mlflow.pyfunc.load_model(model_uri)
            logger.info(
                f"Loaded latest model '{model_name}' v{latest.version}"
            )
            return model
        except Exception as e:
            logger.warning(f"Failed to load latest model '{model_name}': {e}")
            return None

    # ------------------------------------------------------------------
    # QUERYING & COMPARISON
    # ------------------------------------------------------------------

    def list_model_versions(self, model_name: str) -> pd.DataFrame:
        """
        Lists all versions of a registered model with metadata.

        Args:
            model_name: Registry model name

        Returns:
            DataFrame with version, run_id, status, aliases, and creation_timestamp
        """
        try:
            versions = self.client.search_model_versions(
                f"name='{model_name}'"
            )
            if not versions:
                return pd.DataFrame()

            rows = []
            for v in versions:
                rows.append({
                    "version": v.version,
                    "run_id": v.run_id,
                    "status": v.status,
                    "aliases": ", ".join(v.aliases) if v.aliases else "",
                    "creation_timestamp": v.creation_timestamp,
                    "description": v.description or "",
                })

            df = pd.DataFrame(rows)
            df = df.sort_values("version", key=lambda x: x.astype(int), ascending=False)
            return df.reset_index(drop=True)
        except Exception as e:
            logger.error(f"Failed to list versions: {e}")
            return pd.DataFrame()

    def get_model_version_info(
        self,
        model_name: str,
        version: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Gets detailed info for a specific model version, including run metrics.

        Args:
            model_name: Registry model name
            version:    Version number

        Returns:
            Dict with version metadata and metrics, or None
        """
        try:
            mv = self.client.get_model_version(model_name, version)
            run = self.client.get_run(mv.run_id)

            return {
                "model_name": model_name,
                "version": mv.version,
                "run_id": mv.run_id,
                "status": mv.status,
                "aliases": list(mv.aliases) if mv.aliases else [],
                "creation_timestamp": mv.creation_timestamp,
                "description": mv.description or "",
                "metrics": dict(run.data.metrics),
                "params": dict(run.data.params),
                "tags": dict(run.data.tags),
            }
        except Exception as e:
            logger.error(f"Failed to get version info: {e}")
            return None

    def compare_model_versions(self, model_name: str) -> pd.DataFrame:
        """
        Compares all versions of a model by their run metrics.

        Args:
            model_name: Registry model name

        Returns:
            DataFrame with version, aliases, and all logged metrics
        """
        try:
            versions = self.client.search_model_versions(
                f"name='{model_name}'"
            )
            if not versions:
                return pd.DataFrame()

            rows = []
            for v in versions:
                try:
                    run = self.client.get_run(v.run_id)
                    row = {
                        "version": v.version,
                        "aliases": ", ".join(v.aliases) if v.aliases else "",
                        "run_id": v.run_id,
                        "model_type": run.data.tags.get("model_type", "unknown"),
                    }
                    row.update(run.data.metrics)
                    rows.append(row)
                except Exception:
                    rows.append({
                        "version": v.version,
                        "aliases": ", ".join(v.aliases) if v.aliases else "",
                        "run_id": v.run_id,
                    })

            df = pd.DataFrame(rows)
            if "f1" in df.columns:
                df = df.sort_values("f1", ascending=False)
            return df.reset_index(drop=True)
        except Exception as e:
            logger.error(f"Failed to compare versions: {e}")
            return pd.DataFrame()

    def get_production_version(self, model_name: str) -> Optional[Dict[str, Any]]:
        """
        Gets the current production version info.

        Args:
            model_name: Registry model name

        Returns:
            Dict with version info and metrics, or None
        """
        try:
            mv = self.client.get_model_version_by_alias(model_name, "production")
            return self.get_model_version_info(model_name, mv.version)
        except Exception as e:
            logger.warning(f"No production version for '{model_name}': {e}")
            return None

    # ------------------------------------------------------------------
    # LIFECYCLE (Rollback, Delete, Archive)
    # ------------------------------------------------------------------

    def rollback_production(self, model_name: str) -> bool:
        """
        Rolls back the production alias to the previous version.

        Finds the current production version, then sets @production
        to the highest version below it.

        Args:
            model_name: Registry model name

        Returns:
            True if rollback succeeded, False if no previous version
        """
        try:
            # Get current production version
            prod_mv = self.client.get_model_version_by_alias(
                model_name, "production"
            )
            current_version = int(prod_mv.version)

            # Find all versions
            all_versions = self.client.search_model_versions(
                f"name='{model_name}'"
            )
            previous_versions = [
                v for v in all_versions
                if int(v.version) < current_version
            ]

            if not previous_versions:
                logger.warning(f"No previous version to rollback to for '{model_name}'")
                return False

            # Pick the highest version below current
            prev = max(previous_versions, key=lambda v: int(v.version))

            self.client.set_registered_model_alias(
                name=model_name, alias="production", version=prev.version
            )
            logger.info(
                f"Rolled back '{model_name}' production: "
                f"v{current_version} -> v{prev.version}"
            )
            return True
        except Exception as e:
            logger.error(f"Rollback failed: {e}")
            return False

    def delete_model_version(self, model_name: str, version: str) -> bool:
        """
        Deletes a specific model version from the registry.

        Args:
            model_name: Registry model name
            version:    Version to delete

        Returns:
            True if deleted
        """
        try:
            self.client.delete_model_version(model_name, version)
            logger.info(f"Deleted model '{model_name}' v{version}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete version: {e}")
            return False

    def archive_model(self, model_name: str, version: str) -> bool:
        """
        Marks a model version as archived by setting the @archived alias.

        Note: MLflow aliases are 1:1 (one alias → one version), so only
        the latest archived version will have @archived. For full audit
        trail, add a tag instead.

        Args:
            model_name: Registry model name
            version:    Version to archive

        Returns:
            True if successful
        """
        try:
            # Add a tag for permanent audit trail
            self.client.set_model_version_tag(
                model_name, version, "archived", "true"
            )
            logger.info(f"Archived model '{model_name}' v{version}")
            return True
        except Exception as e:
            logger.error(f"Failed to archive: {e}")
            return False

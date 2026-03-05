"""
Data Drift Detection Module - P4 (Abdessamad)
Monitors data drift using PSI, KS test, and feature-level comparison.
Logs drift metrics to MLflow.

Usage:
    from ml.drift_detection import DataDriftDetector

    detector = DataDriftDetector()
    report = detector.generate_drift_report(ref_df, curr_df, feature_names)
"""

import logging
import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Tuple, Any
from scipy import stats
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class DataDriftDetector:
    """
    Monitors data drift between a reference (training) dataset and
    current (production) data.

    Methods:
        - PSI (Population Stability Index): Measures distribution shift
        - KS Test (Kolmogorov-Smirnov): Statistical test for distribution change
        - Feature drift: Per-feature analysis with configurable thresholds
        - Prediction drift: Distribution shift in model outputs

    Thresholds:
        PSI < 0.1  → No drift
        PSI 0.1–0.2 → Moderate drift
        PSI > 0.2  → Significant drift
    """

    PSI_THRESHOLD_MODERATE = 0.1
    PSI_THRESHOLD_SIGNIFICANT = 0.2
    KS_PVALUE_THRESHOLD = 0.05

    def __init__(
        self,
        psi_bins: int = 10,
        psi_threshold: float = 0.2,
        ks_pvalue_threshold: float = 0.05,
    ):
        """
        Args:
            psi_bins:           Number of bins for PSI calculation
            psi_threshold:      PSI threshold for significant drift
            ks_pvalue_threshold: p-value below which KS test signals drift
        """
        self.psi_bins = psi_bins
        self.psi_threshold = psi_threshold
        self.ks_pvalue_threshold = ks_pvalue_threshold

    # ------------------------------------------------------------------
    # PSI (Population Stability Index)
    # ------------------------------------------------------------------

    def compute_psi(
        self,
        reference: np.ndarray,
        current: np.ndarray,
        bins: Optional[int] = None,
    ) -> float:
        """
        Computes Population Stability Index between two distributions.

        PSI = Σ (p_i - q_i) * ln(p_i / q_i)

        Args:
            reference: Reference distribution (training data)
            current:   Current distribution (production data)
            bins:      Number of bins (default: self.psi_bins)

        Returns:
            PSI value (0 = identical, higher = more drift)
        """
        bins = bins or self.psi_bins

        # Create bins from reference distribution
        breakpoints = np.linspace(
            min(reference.min(), current.min()),
            max(reference.max(), current.max()),
            bins + 1,
        )

        ref_counts = np.histogram(reference, bins=breakpoints)[0]
        curr_counts = np.histogram(current, bins=breakpoints)[0]

        # Convert to proportions, avoid division by zero
        ref_pct = ref_counts / len(reference)
        curr_pct = curr_counts / len(current)

        # Replace zeros with small epsilon
        eps = 1e-6
        ref_pct = np.clip(ref_pct, eps, None)
        curr_pct = np.clip(curr_pct, eps, None)

        psi = np.sum((curr_pct - ref_pct) * np.log(curr_pct / ref_pct))
        return float(psi)

    # ------------------------------------------------------------------
    # KS TEST
    # ------------------------------------------------------------------

    def compute_ks_test(
        self,
        reference: np.ndarray,
        current: np.ndarray,
    ) -> Dict[str, float]:
        """
        Performs two-sample Kolmogorov-Smirnov test.

        Args:
            reference: Reference distribution
            current:   Current distribution

        Returns:
            Dict with 'statistic', 'p_value', and 'is_drifted' flag
        """
        statistic, p_value = stats.ks_2samp(reference, current)
        return {
            "statistic": float(statistic),
            "p_value": float(p_value),
            "is_drifted": bool(p_value < self.ks_pvalue_threshold),
        }

    # ------------------------------------------------------------------
    # FEATURE DRIFT DETECTION
    # ------------------------------------------------------------------

    def detect_feature_drift(
        self,
        ref_df: pd.DataFrame,
        curr_df: pd.DataFrame,
        features: List[str],
    ) -> Dict[str, Dict[str, Any]]:
        """
        Runs PSI + KS test across all features.

        Args:
            ref_df:   Reference DataFrame
            curr_df:  Current DataFrame
            features: List of feature column names

        Returns:
            Dict mapping feature_name -> {psi, ks_statistic, ks_pvalue, is_drifted, drift_level}
        """
        results = {}

        for feat in features:
            if feat not in ref_df.columns or feat not in curr_df.columns:
                logger.warning(f"Feature '{feat}' missing from one of the datasets")
                continue

            ref_vals = ref_df[feat].dropna().values.astype(float)
            curr_vals = curr_df[feat].dropna().values.astype(float)

            if len(ref_vals) == 0 or len(curr_vals) == 0:
                continue

            # PSI
            psi = self.compute_psi(ref_vals, curr_vals)

            # KS Test
            ks_result = self.compute_ks_test(ref_vals, curr_vals)

            # Drift level
            if psi < self.PSI_THRESHOLD_MODERATE:
                drift_level = "none"
            elif psi < self.PSI_THRESHOLD_SIGNIFICANT:
                drift_level = "moderate"
            else:
                drift_level = "significant"

            is_drifted = psi >= self.psi_threshold or ks_result["is_drifted"]

            results[feat] = {
                "psi": round(psi, 6),
                "ks_statistic": round(ks_result["statistic"], 6),
                "ks_pvalue": round(ks_result["p_value"], 6),
                "is_drifted": is_drifted,
                "drift_level": drift_level,
            }

        return results

    # ------------------------------------------------------------------
    # PREDICTION DRIFT
    # ------------------------------------------------------------------

    def detect_prediction_drift(
        self,
        ref_preds: np.ndarray,
        curr_preds: np.ndarray,
    ) -> Dict[str, Any]:
        """
        Checks if prediction distribution has shifted.

        Args:
            ref_preds:  Reference predictions (training/validation)
            curr_preds: Current predictions (production)

        Returns:
            Dict with PSI, KS test results, and drift flag
        """
        psi = self.compute_psi(ref_preds, curr_preds)
        ks_result = self.compute_ks_test(ref_preds, curr_preds)

        if psi < self.PSI_THRESHOLD_MODERATE:
            drift_level = "none"
        elif psi < self.PSI_THRESHOLD_SIGNIFICANT:
            drift_level = "moderate"
        else:
            drift_level = "significant"

        return {
            "psi": round(psi, 6),
            "ks_statistic": round(ks_result["statistic"], 6),
            "ks_pvalue": round(ks_result["p_value"], 6),
            "is_drifted": psi >= self.psi_threshold or ks_result["is_drifted"],
            "drift_level": drift_level,
        }

    # ------------------------------------------------------------------
    # DRIFT REPORT
    # ------------------------------------------------------------------

    def generate_drift_report(
        self,
        ref_df: pd.DataFrame,
        curr_df: pd.DataFrame,
        features: List[str],
        ref_preds: Optional[np.ndarray] = None,
        curr_preds: Optional[np.ndarray] = None,
    ) -> Dict[str, Any]:
        """
        Generates a full drift report for all features.

        Args:
            ref_df:     Reference DataFrame
            curr_df:    Current DataFrame
            features:   Feature column names
            ref_preds:  Optional reference predictions
            curr_preds: Optional current predictions

        Returns:
            Dict with per-feature drift, overall drift score, and summary
        """
        # Feature drift
        feature_drift = self.detect_feature_drift(ref_df, curr_df, features)

        # Overall metrics
        psi_values = [v["psi"] for v in feature_drift.values()]
        drifted_features = [
            k for k, v in feature_drift.items() if v["is_drifted"]
        ]

        overall_psi = float(np.mean(psi_values)) if psi_values else 0.0
        drift_ratio = len(drifted_features) / len(features) if features else 0.0

        if overall_psi < self.PSI_THRESHOLD_MODERATE:
            overall_level = "none"
        elif overall_psi < self.PSI_THRESHOLD_SIGNIFICANT:
            overall_level = "moderate"
        else:
            overall_level = "significant"

        report = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "reference_samples": len(ref_df),
            "current_samples": len(curr_df),
            "total_features": len(features),
            "drifted_features_count": len(drifted_features),
            "drift_ratio": round(drift_ratio, 4),
            "overall_psi": round(overall_psi, 6),
            "overall_drift_level": overall_level,
            "drifted_features": drifted_features,
            "feature_details": feature_drift,
        }

        # Prediction drift (optional)
        if ref_preds is not None and curr_preds is not None:
            report["prediction_drift"] = self.detect_prediction_drift(
                ref_preds, curr_preds
            )

        logger.info(
            f"Drift report: {len(drifted_features)}/{len(features)} features drifted, "
            f"overall PSI={overall_psi:.4f} ({overall_level})"
        )

        return report

    # ------------------------------------------------------------------
    # MLFLOW LOGGING
    # ------------------------------------------------------------------

    def log_drift_to_mlflow(
        self,
        report: Dict[str, Any],
        experiment_name: str = "data-drift-monitoring",
    ) -> Optional[str]:
        """
        Logs drift report metrics to MLflow.

        Args:
            report:          Drift report from generate_drift_report()
            experiment_name: MLflow experiment name

        Returns:
            MLflow run_id or None
        """
        try:
            import mlflow
            import os

            tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5001")
            mlflow.set_tracking_uri(tracking_uri)
            mlflow.set_experiment(experiment_name)

            with mlflow.start_run(
                run_name=f"drift_check_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            ):
                # Overall metrics
                mlflow.log_metrics({
                    "overall_psi": report["overall_psi"],
                    "drift_ratio": report["drift_ratio"],
                    "drifted_features_count": report["drifted_features_count"],
                    "total_features": report["total_features"],
                    "reference_samples": report["reference_samples"],
                    "current_samples": report["current_samples"],
                })

                # Per-feature PSI (top drifted)
                feature_details = report.get("feature_details", {})
                for feat, details in feature_details.items():
                    mlflow.log_metric(f"psi_{feat}", details["psi"])

                # Prediction drift if available
                pred_drift = report.get("prediction_drift")
                if pred_drift:
                    mlflow.log_metrics({
                        "prediction_psi": pred_drift["psi"],
                        "prediction_ks_statistic": pred_drift["ks_statistic"],
                    })

                # Tags
                mlflow.set_tags({
                    "drift_level": report["overall_drift_level"],
                    "task": "drift_monitoring",
                })

                run_id = mlflow.active_run().info.run_id
                logger.info(f"Drift report logged to MLflow (run: {run_id})")
                return run_id

        except Exception as e:
            logger.warning(f"MLflow drift logging failed: {e}")
            return None

"""
Feature Engineering Module - P4 (Abdessamad)
Transforms OHLCV data and technical indicators into ML-ready features
for anomaly detection and price prediction.

Input:  MongoDB collections 'ohlcv' and 'indicators' (produced by P3)
Output: pandas DataFrame with engineered features, ready for model training
"""

import logging
import numpy as np
import pandas as pd
from pymongo import MongoClient
from typing import Optional, List

from .config import mongodb_config, feature_config

logger = logging.getLogger(__name__)


class CryptoFeatureEngineer:
    """
    Builds an ML-ready feature matrix from raw OHLCV data and technical indicators.

    Pipeline:
        1. Load OHLCV + indicators from MongoDB
        2. Merge on (symbol, interval, timestamp)
        3. Engineer features (price, volume, candle, indicator, lag, rolling)
        4. Create target variables (direction, anomaly label)
        5. Drop NaN rows and return clean DataFrame

    Usage:
        fe = CryptoFeatureEngineer()
        df = fe.build_feature_matrix("BTCUSDT", "1m", limit=5000)
        X = df[fe.get_feature_names(df)]
        y = df["target_direction"]
    """

    def __init__(self, mongo_uri: Optional[str] = None, database: Optional[str] = None):
        """
        Initialize feature engineer.

        Args:
            mongo_uri:  MongoDB URI (default from config)
            database:   Database name (default from config)
        """
        self.mongo_uri = mongo_uri or mongodb_config.uri
        self.database = database or mongodb_config.database
        self.config = feature_config

    # ------------------------------------------------------------------
    # DATA LOADING
    # ------------------------------------------------------------------

    def _get_mongo_client(self) -> MongoClient:
        """Creates a MongoDB client connection."""
        return MongoClient(self.mongo_uri)

    def load_ohlcv_data(self, symbol: str, interval: str = "1m", limit: int = 5000) -> pd.DataFrame:
        """
        Loads OHLCV data from MongoDB.

        Args:
            symbol:   Trading pair (e.g. 'BTCUSDT')
            interval: Candle interval (e.g. '1m', '5m', '1h')
            limit:    Max rows to load (most recent)

        Returns:
            DataFrame sorted by timestamp ascending
        """
        logger.info(f"Loading OHLCV data: {symbol}/{interval} (limit={limit})")

        client = self._get_mongo_client()
        try:
            db = client[self.database]
            collection = db[mongodb_config.ohlcv_collection]

            cursor = (
                collection.find({"symbol": symbol, "interval": interval}, {"_id": 0}).sort("timestamp", -1).limit(limit)
            )

            df = pd.DataFrame(list(cursor))

            if df.empty:
                logger.warning(f"No OHLCV data found for {symbol}/{interval}")
                return df

            df = df.sort_values("timestamp").reset_index(drop=True)
            logger.info(f"  Loaded {len(df)} OHLCV rows")
            return df

        finally:
            client.close()

    def load_indicators_data(self, symbol: str, interval: str = "1m", limit: int = 5000) -> pd.DataFrame:
        """
        Loads technical indicators from MongoDB.

        Args:
            symbol:   Trading pair
            interval: Candle interval
            limit:    Max rows

        Returns:
            DataFrame sorted by timestamp ascending
        """
        logger.info(f"Loading indicators data: {symbol}/{interval} (limit={limit})")

        client = self._get_mongo_client()
        try:
            db = client[self.database]
            collection = db[mongodb_config.indicators_collection]

            cursor = (
                collection.find({"symbol": symbol, "interval": interval}, {"_id": 0}).sort("timestamp", -1).limit(limit)
            )

            df = pd.DataFrame(list(cursor))

            if df.empty:
                logger.warning(f"No indicators data found for {symbol}/{interval}")
                return df

            df = df.sort_values("timestamp").reset_index(drop=True)
            logger.info(f"  Loaded {len(df)} indicator rows")
            return df

        finally:
            client.close()

    @staticmethod
    def merge_ohlcv_indicators(ohlcv_df: pd.DataFrame, indicators_df: pd.DataFrame) -> pd.DataFrame:
        """
        Merges OHLCV and indicators on (symbol, interval, timestamp).

        If indicators_df is empty, returns ohlcv_df unchanged.
        Duplicate columns from the indicators side are dropped to avoid
        collisions (e.g. 'close' already present in OHLCV).
        """
        if indicators_df.empty:
            logger.info("  No indicators to merge, using OHLCV only")
            return ohlcv_df

        # Drop columns already present in OHLCV (except join keys)
        join_keys = ["symbol", "interval", "timestamp"]
        duplicate_cols = [c for c in indicators_df.columns if c in ohlcv_df.columns and c not in join_keys]
        if duplicate_cols:
            indicators_df = indicators_df.drop(columns=duplicate_cols)

        merged = pd.merge(ohlcv_df, indicators_df, on=join_keys, how="left")
        logger.info(f"  Merged result: {len(merged)} rows, {len(merged.columns)} columns")
        return merged

    # ------------------------------------------------------------------
    # FEATURE ENGINEERING — PRICE
    # ------------------------------------------------------------------

    @staticmethod
    def add_price_features(df: pd.DataFrame) -> pd.DataFrame:
        """
        Adds price-based features:
        - Simple returns over multiple periods
        - Log return (1-period)
        - Realized volatility over multiple windows
        - Average True Range (ATR)
        """
        logger.info("Adding price features...")

        # Returns over different periods
        for p in feature_config.return_periods:
            df[f"return_{p}"] = df["close"].pct_change(periods=p)

        # Log return (1 period)
        df["log_return_1"] = np.log(df["close"] / df["close"].shift(1))

        # Realized volatility (rolling std of returns)
        if "return_1" not in df.columns:
            df["return_1"] = df["close"].pct_change(periods=1)

        for w in feature_config.rolling_windows:
            df[f"volatility_{w}"] = df["return_1"].rolling(window=w).std()

        # ATR (Average True Range)
        high_low = df["high"] - df["low"]
        high_close_prev = (df["high"] - df["close"].shift(1)).abs()
        low_close_prev = (df["low"] - df["close"].shift(1)).abs()
        true_range = pd.concat([high_low, high_close_prev, low_close_prev], axis=1).max(axis=1)
        df["atr"] = true_range.rolling(window=14).mean()

        logger.info("  ✅ Price features added")
        return df

    # ------------------------------------------------------------------
    # FEATURE ENGINEERING — VOLUME
    # ------------------------------------------------------------------

    @staticmethod
    def add_volume_features(df: pd.DataFrame) -> pd.DataFrame:
        """
        Adds volume-based features:
        - Volume ratio to N-period moving average
        - Volume 1-period change
        """
        logger.info("Adding volume features...")

        for w in feature_config.rolling_windows:
            vol_ma = df["volume"].rolling(window=w).mean()
            df[f"volume_ratio_{w}"] = df["volume"] / vol_ma

        df["volume_change_1"] = df["volume"].pct_change(periods=1)

        logger.info("  ✅ Volume features added")
        return df

    # ------------------------------------------------------------------
    # FEATURE ENGINEERING — CANDLE STRUCTURE
    # ------------------------------------------------------------------

    @staticmethod
    def add_candle_features(df: pd.DataFrame) -> pd.DataFrame:
        """
        Adds candle micro-structure features:
        - body_ratio: |close-open| / (high-low)
        - upper_shadow / lower_shadow ratios
        - hl_range: (high-low) / close (normalized range)
        """
        logger.info("Adding candle features...")

        hl = df["high"] - df["low"]
        # Guard against zero range
        hl_safe = hl.replace(0, np.nan)

        df["body_ratio"] = (df["close"] - df["open"]).abs() / hl_safe
        df["upper_shadow"] = (df["high"] - df[["open", "close"]].max(axis=1)) / hl_safe
        df["lower_shadow"] = (df[["open", "close"]].min(axis=1) - df["low"]) / hl_safe
        df["hl_range"] = hl / df["close"]

        logger.info("  ✅ Candle features added")
        return df

    # ------------------------------------------------------------------
    # FEATURE ENGINEERING — INDICATOR-DERIVED
    # ------------------------------------------------------------------

    @staticmethod
    def add_indicator_features(df: pd.DataFrame) -> pd.DataFrame:
        """
        Adds discretized/derived features from technical indicators:
        - rsi_zone (0=oversold, 1=neutral, 2=overbought)
        - macd_cross_signal (+1 bullish, -1 bearish, 0 neutral)
        - bollinger_pband: (close - lower) / (upper - lower)
        - bollinger_width: (upper - lower) / middle
        - sma_cross_20_50: 1 if SMA20 > SMA50, else 0
        """
        logger.info("Adding indicator-derived features...")

        # RSI zone
        if "rsi_14" in df.columns:
            df["rsi_zone"] = 1  # neutral
            df.loc[df["rsi_14"] <= feature_config.rsi_oversold, "rsi_zone"] = 0
            df.loc[df["rsi_14"] >= feature_config.rsi_overbought, "rsi_zone"] = 2
        else:
            logger.warning("  rsi_14 not found, skipping rsi_zone")

        # MACD cross signal
        if "macd" in df.columns and "macd_signal" in df.columns:
            macd_diff = df["macd"] - df["macd_signal"]
            macd_diff_prev = macd_diff.shift(1)
            df["macd_cross_signal"] = 0
            df.loc[(macd_diff > 0) & (macd_diff_prev <= 0), "macd_cross_signal"] = 1  # bullish
            df.loc[(macd_diff < 0) & (macd_diff_prev >= 0), "macd_cross_signal"] = -1  # bearish
        else:
            logger.warning("  macd/macd_signal not found, skipping macd_cross_signal")

        # Bollinger %B and width
        if all(c in df.columns for c in ["bollinger_upper", "bollinger_lower", "bollinger_middle"]):
            band_range = df["bollinger_upper"] - df["bollinger_lower"]
            band_range_safe = band_range.replace(0, np.nan)
            df["bollinger_pband"] = (df["close"] - df["bollinger_lower"]) / band_range_safe
            df["bollinger_width"] = band_range / df["bollinger_middle"]
        else:
            logger.warning("  Bollinger columns not found, skipping bollinger features")

        # SMA cross
        if "sma_20" in df.columns and "sma_50" in df.columns:
            df["sma_cross_20_50"] = (df["sma_20"] > df["sma_50"]).astype(int)
        else:
            logger.warning("  sma_20/sma_50 not found, skipping sma_cross_20_50")

        logger.info("  ✅ Indicator-derived features added")
        return df

    # ------------------------------------------------------------------
    # FEATURE ENGINEERING — LAG FEATURES
    # ------------------------------------------------------------------

    @staticmethod
    def add_lag_features(df: pd.DataFrame) -> pd.DataFrame:
        """
        Adds lagged values for key columns:
        - close, volume lags for all configured lag periods
        - rsi_14 lags for first few periods (if present)
        """
        logger.info("Adding lag features...")

        for lag in feature_config.lag_periods:
            df[f"close_lag_{lag}"] = df["close"].shift(lag)
            df[f"volume_lag_{lag}"] = df["volume"].shift(lag)

        # RSI lags (shorter list — only first 3 lag periods)
        if "rsi_14" in df.columns:
            for lag in feature_config.lag_periods[:3]:
                df[f"rsi_14_lag_{lag}"] = df["rsi_14"].shift(lag)

        logger.info("  ✅ Lag features added")
        return df

    # ------------------------------------------------------------------
    # FEATURE ENGINEERING — ROLLING STATISTICS
    # ------------------------------------------------------------------

    @staticmethod
    def add_rolling_features(df: pd.DataFrame) -> pd.DataFrame:
        """
        Adds rolling statistical features:
        - rolling mean, std of close
        - rolling min, max of close (for 20-period window)
        """
        logger.info("Adding rolling features...")

        for w in feature_config.rolling_windows:
            df[f"rolling_mean_{w}"] = df["close"].rolling(window=w).mean()
            df[f"rolling_std_{w}"] = df["close"].rolling(window=w).std()

        # Min/max for the largest window
        max_w = max(feature_config.rolling_windows)
        df[f"rolling_min_{max_w}"] = df["close"].rolling(window=max_w).min()
        df[f"rolling_max_{max_w}"] = df["close"].rolling(window=max_w).max()

        logger.info("  ✅ Rolling features added")
        return df

    # ------------------------------------------------------------------
    # TARGET VARIABLES
    # ------------------------------------------------------------------

    @staticmethod
    def add_target_variables(df: pd.DataFrame) -> pd.DataFrame:
        """
        Adds supervised learning target variables:
        - target_direction:  1 if close increases over prediction_horizon, else 0
        - target_return_pct: % return over prediction_horizon
        - anomaly_label:     1 if |return_1| exceeds anomaly threshold, else 0
        """
        logger.info("Adding target variables...")

        horizon = feature_config.prediction_horizon
        threshold = feature_config.anomaly_return_threshold

        # Future return
        future_close = df["close"].shift(-horizon)
        df["target_return_pct"] = (future_close - df["close"]) / df["close"]

        # Direction: 1 = price goes up, 0 = price stays or goes down
        df["target_direction"] = (df["target_return_pct"] > 0).astype(int)

        # Anomaly label (based on 1-period absolute return)
        if "return_1" not in df.columns:
            df["return_1"] = df["close"].pct_change(periods=1)

        df["anomaly_label"] = (df["return_1"].abs() > threshold).astype(int)

        logger.info("  ✅ Target variables added")
        return df

    # ------------------------------------------------------------------
    # FEATURE NAME HELPERS
    # ------------------------------------------------------------------

    def get_feature_names(self, df: pd.DataFrame) -> List[str]:
        """
        Returns the list of feature column names, excluding metadata and targets.

        Args:
            df: The feature DataFrame

        Returns:
            Sorted list of feature column names
        """
        exclude = set(self.config.metadata_columns + self.config.target_columns)
        # Also exclude any extra processing metadata
        exclude.update(
            [
                "calculated_at",
                "processed_at",
                "processing_timestamp",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "num_trades",
                "price_change",
                "price_change_pct",
            ]
        )
        features = [c for c in df.columns if c not in exclude]
        return sorted(features)

    # ------------------------------------------------------------------
    # FULL PIPELINE
    # ------------------------------------------------------------------

    def build_feature_matrix(
        self, symbol: str, interval: str = "1m", limit: int = 5000, dropna: bool = True
    ) -> pd.DataFrame:
        """
        End-to-end pipeline: load data → engineer features → clean → return.

        Args:
            symbol:   Trading pair (e.g. 'BTCUSDT')
            interval: Candle interval
            limit:    Max rows to load from MongoDB
            dropna:   If True, drop rows with NaN values

        Returns:
            Clean pandas DataFrame with all features and target variables
        """
        logger.info(f"========== Building feature matrix: {symbol}/{interval} ==========")

        # 1. Load data
        ohlcv_df = self.load_ohlcv_data(symbol, interval, limit)
        if ohlcv_df.empty:
            logger.error("No OHLCV data available — cannot build features")
            return pd.DataFrame()

        indicators_df = self.load_indicators_data(symbol, interval, limit)

        # 2. Merge
        df = self.merge_ohlcv_indicators(ohlcv_df, indicators_df)

        # 3. Engineer features
        df = self.add_price_features(df)
        df = self.add_volume_features(df)
        df = self.add_candle_features(df)
        df = self.add_indicator_features(df)
        df = self.add_lag_features(df)
        df = self.add_rolling_features(df)

        # 4. Add targets
        df = self.add_target_variables(df)

        # 5. Clean NaN rows
        rows_before = len(df)
        if dropna:
            df = df.dropna().reset_index(drop=True)
        rows_after = len(df)
        logger.info(f"  Rows: {rows_before} → {rows_after} after NaN drop")

        feature_names = self.get_feature_names(df)
        logger.info(f"  Features: {len(feature_names)} columns")
        logger.info("========== Feature matrix ready ==========")

        return df

    def build_feature_matrix_from_dataframe(self, df: pd.DataFrame, dropna: bool = True) -> pd.DataFrame:
        """
        Builds features from an already-loaded DataFrame (useful for testing
        or when data is supplied externally instead of from MongoDB).

        Args:
            df:     DataFrame with at minimum: open, high, low, close, volume columns
            dropna: If True, drop rows with NaN values

        Returns:
            DataFrame with all engineered features and target variables
        """
        logger.info("Building feature matrix from provided DataFrame...")

        df = self.add_price_features(df)
        df = self.add_volume_features(df)
        df = self.add_candle_features(df)
        df = self.add_indicator_features(df)
        df = self.add_lag_features(df)
        df = self.add_rolling_features(df)
        df = self.add_target_variables(df)

        if dropna:
            df = df.dropna().reset_index(drop=True)

        return df

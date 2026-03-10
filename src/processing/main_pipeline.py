"""
Main Pipeline - P3
Orchestrates the complete Spark Structured Streaming pipeline for crypto data processing
"""

import argparse
import logging
import sys
from pyspark.sql import SparkSession

# Import local modules
from spark_session import create_spark_session, stop_spark_session
from kafka_consumer import consume_trades_stream, consume_klines_stream, aggregate_trades_to_ohlcv, read_klines_batch
from transformations import (
    validate_trades_data,
    validate_ohlcv_data,
    add_metadata_columns,
    add_interval_column,
    calculate_price_change,
    calculate_volume_metrics,
    add_candle_pattern,
)
from technical_indicators import calculate_all_indicators
from aggregated_metrics import calculate_daily_metrics
from data_cleaning import clean_ohlcv_data
from config import cleaning_config
from mongodb_writer import (
    write_trades_to_mongodb,
    write_ohlcv_to_mongodb,
    write_indicators_to_mongodb,
    write_aggregated_metrics_to_mongodb,
    write_to_console,
    await_termination,
    stop_all_queries,
)

# Configure logging
import os

_log_handlers = [logging.StreamHandler(sys.stdout)]
_log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
try:
    os.makedirs(_log_dir, exist_ok=True)
    _log_handlers.append(logging.FileHandler(os.path.join(_log_dir, "spark_processing.log")))
except OSError:
    pass  # Fallback to stdout only (e.g. inside Docker container)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=_log_handlers,
)
logger = logging.getLogger(__name__)


def run_streaming_pipeline(spark: SparkSession, debug: bool = False):
    """
    Runs the complete streaming pipeline

    Args:
        spark: SparkSession instance
        debug: If True, outputs to console instead of MongoDB
    """
    logger.info("=" * 60)
    logger.info("STARTING STREAMING PIPELINE - P3")
    logger.info("=" * 60)

    queries = []

    try:
        # ============================================
        # STEP 1: Consume Trades Stream
        # ============================================
        logger.info("\n[STEP 1] Consuming trades stream from Kafka...")
        trades_df = consume_trades_stream(spark, with_watermark=True)

        # Validate and enrich trades
        trades_df = validate_trades_data(trades_df)
        trades_df = add_metadata_columns(trades_df)

        # Write trades to MongoDB or console
        if debug:
            query_trades = write_to_console(trades_df, truncate=False, num_rows=10)
            queries.append(query_trades)
        else:
            query_trades = write_trades_to_mongodb(trades_df, streaming=True)
            queries.append(query_trades)

        # ============================================
        # STEP 2: Aggregate Trades to OHLCV
        # ============================================
        logger.info("\n[STEP 2] Aggregating trades to OHLCV (1 minute)...")
        ohlcv_1m_df = aggregate_trades_to_ohlcv(trades_df, window_duration="1 minute")

        # Add interval column
        ohlcv_1m_df = add_interval_column(ohlcv_1m_df, "1m")

        # Validate and enrich OHLCV
        ohlcv_1m_df = validate_ohlcv_data(ohlcv_1m_df)
        ohlcv_1m_df = calculate_price_change(ohlcv_1m_df)
        ohlcv_1m_df = calculate_volume_metrics(ohlcv_1m_df)
        ohlcv_1m_df = add_candle_pattern(ohlcv_1m_df)
        ohlcv_1m_df = add_metadata_columns(ohlcv_1m_df)

        # Write OHLCV to MongoDB
        if not debug:
            query_ohlcv = write_ohlcv_to_mongodb(ohlcv_1m_df, streaming=True)
            queries.append(query_ohlcv)

        # ============================================
        # STEP 3: Consume Klines Stream (if available)
        # ============================================
        logger.info("\n[STEP 3] Consuming klines stream from Kafka...")
        klines_df = consume_klines_stream(spark, with_watermark=True)

        # Validate and enrich klines
        klines_df = validate_ohlcv_data(klines_df)
        klines_df = calculate_price_change(klines_df)
        klines_df = calculate_volume_metrics(klines_df)
        klines_df = add_candle_pattern(klines_df)
        klines_df = add_metadata_columns(klines_df)

        # Write klines to MongoDB
        if not debug:
            query_klines = write_ohlcv_to_mongodb(klines_df, streaming=True)
            queries.append(query_klines)

        # ============================================
        # STEP 4: Calculate Technical Indicators
        # ============================================
        logger.info("\n[STEP 4] Calculating technical indicators...")

        # Calculate indicators on klines data
        indicators_df = calculate_all_indicators(klines_df)

        # Select only relevant columns for indicators collection
        indicators_df = indicators_df.select(
            "symbol",
            "interval",
            "timestamp",
            "rsi_14",
            "macd",
            "macd_signal",
            "macd_histogram",
            "bollinger_upper",
            "bollinger_middle",
            "bollinger_lower",
            "sma_20",
            "sma_50",
            "sma_200",
            "ema_12",
            "ema_26",
        )

        indicators_df = add_metadata_columns(indicators_df)

        # Write indicators to MongoDB or console
        if debug:
            query_indicators = write_to_console(indicators_df, truncate=False, num_rows=5)
            queries.append(query_indicators)
        else:
            query_indicators = write_indicators_to_mongodb(indicators_df, streaming=True)
            queries.append(query_indicators)

        # ============================================
        # STEP 5: Calculate Aggregated Metrics
        # ============================================
        logger.info("\n[STEP 5] Calculating aggregated metrics...")

        # Calculate daily aggregated metrics on klines data
        daily_metrics_df = calculate_daily_metrics(klines_df)
        daily_metrics_df = add_metadata_columns(daily_metrics_df)

        # Write aggregated metrics to MongoDB or console
        if debug:
            query_agg = write_to_console(daily_metrics_df, truncate=False, num_rows=5)
            queries.append(query_agg)
        else:
            query_agg = write_aggregated_metrics_to_mongodb(daily_metrics_df, streaming=True)
            queries.append(query_agg)

        # ============================================
        # STEP 6: Monitor and Wait
        # ============================================
        logger.info("\n[STEP 6] Pipeline running. Monitoring queries...")
        logger.info(f"Active queries: {len(queries)}")
        for i, query in enumerate(queries):
            if query:
                logger.info(f"  Query {i+1}: {query.id}")

        logger.info("\n✅ Pipeline started successfully!")
        logger.info("Press Ctrl+C to stop the pipeline\n")

        # Wait for termination
        await_termination(queries)

    except KeyboardInterrupt:
        logger.info("\n⚠️  Received interrupt signal. Stopping pipeline...")
        stop_all_queries(queries)
    except Exception as e:
        logger.error(f"\n❌ Pipeline error: {str(e)}", exc_info=True)
        stop_all_queries(queries)
        raise
    finally:
        logger.info("\n" + "=" * 60)
        logger.info("PIPELINE STOPPED")
        logger.info("=" * 60)


def run_batch_pipeline(spark: SparkSession):
    """
    Runs a batch processing pipeline on historical data stored in Kafka.

    Reads all klines from the raw_klines topic, applies the full ETL:
      1. Batch read from Kafka (all messages)
      2. Data cleaning (nulls, validation, dedup, outliers)
      3. Transformations (price change, volume metrics, candle patterns)
      4. Technical indicators (SMA, EMA, RSI, MACD, Bollinger)
      5. Aggregated daily metrics
      6. Batch write to MongoDB (ohlcv, indicators, aggregated_metrics)

    Args:
        spark: SparkSession instance
    """
    logger.info("=" * 60)
    logger.info("STARTING BATCH PIPELINE")
    logger.info("=" * 60)

    try:
        # ============================================
        # STEP 1: Read all klines from Kafka (batch)
        # ============================================
        logger.info("\n[STEP 1] Reading klines from Kafka (batch mode)...")
        klines_df = read_klines_batch(spark)
        raw_count = klines_df.count()
        logger.info(f"  Raw klines read: {raw_count}")

        if raw_count == 0:
            logger.warning("No data found in Kafka topic raw_klines. Nothing to process.")
            return

        # ============================================
        # STEP 2: Data Cleaning
        # ============================================
        logger.info("\n[STEP 2] Cleaning data (nulls, validation, dedup, outliers)...")
        klines_clean, quality_metrics = clean_ohlcv_data(klines_df, cleaning_config)
        clean_count = quality_metrics["final_count"]
        logger.info(f"  After cleaning: {clean_count} rows (quality rate: {quality_metrics['quality_rate']:.1f}%)")
        logger.info(f"  Dropped nulls: {quality_metrics.get('dropped_nulls', 0)}")
        logger.info(f"  Invalid OHLCV: {quality_metrics.get('invalid_ohlcv', 0)}")
        logger.info(f"  Duplicates removed: {quality_metrics.get('duplicates_removed', 0)}")

        if clean_count == 0:
            logger.warning("No valid data after cleaning. Aborting batch pipeline.")
            return

        # Drop cleaning-specific columns not needed downstream
        cols_to_drop = [
            c for c in ["zscore_close", "is_outlier", "is_spike", "event_time"] if c in klines_clean.columns
        ]
        if cols_to_drop:
            klines_clean = klines_clean.drop(*cols_to_drop)

        # ============================================
        # STEP 3: Transformations
        # ============================================
        logger.info("\n[STEP 3] Applying transformations...")
        ohlcv_df = validate_ohlcv_data(klines_clean)
        ohlcv_df = calculate_price_change(ohlcv_df)
        ohlcv_df = calculate_volume_metrics(ohlcv_df)
        ohlcv_df = add_candle_pattern(ohlcv_df)
        ohlcv_df = add_metadata_columns(ohlcv_df)
        transform_count = ohlcv_df.count()
        logger.info(f"  After transformations: {transform_count} rows")

        # ============================================
        # STEP 4: Write OHLCV to MongoDB
        # ============================================
        logger.info("\n[STEP 4] Writing OHLCV data to MongoDB...")
        write_ohlcv_to_mongodb(ohlcv_df, streaming=False)
        logger.info(f"  ✅ {transform_count} OHLCV rows written to MongoDB")

        # ============================================
        # STEP 5: Technical Indicators
        # ============================================
        logger.info("\n[STEP 5] Calculating technical indicators...")
        indicators_df = calculate_all_indicators(ohlcv_df)

        # Select indicator columns for the indicators collection
        indicator_columns = [
            "symbol",
            "interval",
            "timestamp",
            "rsi_14",
            "macd",
            "macd_signal",
            "macd_histogram",
            "bollinger_upper",
            "bollinger_middle",
            "bollinger_lower",
            "sma_20",
            "sma_50",
            "sma_200",
            "ema_12",
            "ema_26",
        ]
        # Keep only columns that actually exist
        available_cols = [c for c in indicator_columns if c in indicators_df.columns]
        indicators_selected = indicators_df.select(*available_cols)
        indicators_selected = add_metadata_columns(indicators_selected)

        logger.info("\n[STEP 5b] Writing indicators to MongoDB...")
        write_indicators_to_mongodb(indicators_selected, streaming=False)
        indicators_count = indicators_selected.count()
        logger.info(f"  ✅ {indicators_count} indicator rows written to MongoDB")

        # ============================================
        # STEP 6: Aggregated Daily Metrics
        # ============================================
        logger.info("\n[STEP 6] Calculating daily aggregated metrics...")
        daily_metrics_df = calculate_daily_metrics(ohlcv_df)
        daily_metrics_df = add_metadata_columns(daily_metrics_df)

        write_aggregated_metrics_to_mongodb(daily_metrics_df, streaming=False)
        agg_count = daily_metrics_df.count()
        logger.info(f"  ✅ {agg_count} aggregated metric rows written to MongoDB")

        # ============================================
        # STEP 7: Summary
        # ============================================
        logger.info("\n" + "=" * 60)
        logger.info("BATCH PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
        logger.info(f"  📥 Raw klines from Kafka:    {raw_count}")
        logger.info(f"  🧹 After cleaning:           {clean_count}")
        logger.info(f"  📊 OHLCV written to MongoDB:  {transform_count}")
        logger.info(f"  📈 Indicators written:        {indicators_count}")
        logger.info(f"  📉 Daily metrics written:     {agg_count}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"\n❌ Batch pipeline error: {str(e)}", exc_info=True)
        raise


def main():
    """
    Main entry point for the pipeline
    """
    parser = argparse.ArgumentParser(description="Crypto ETL Pipeline - P3")
    parser.add_argument(
        "--mode",
        type=str,
        choices=["streaming", "batch", "test"],
        default="streaming",
        help="Pipeline mode (default: streaming)",
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug mode (output to console instead of MongoDB)")
    parser.add_argument("--duration", type=int, help="Duration in seconds (for test mode)", default=60)

    args = parser.parse_args()

    # Create Spark session
    logger.info("Initializing Spark session...")
    spark = create_spark_session()

    try:
        if args.mode == "streaming" or args.mode == "test":
            run_streaming_pipeline(spark, debug=args.debug or args.mode == "test")
        elif args.mode == "batch":
            run_batch_pipeline(spark)
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        # Stop Spark session
        stop_spark_session(spark)


if __name__ == "__main__":
    main()

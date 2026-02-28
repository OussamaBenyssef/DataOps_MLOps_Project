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
from kafka_consumer import (
    consume_trades_stream,
    consume_klines_stream,
    aggregate_trades_to_ohlcv
)
from transformations import (
    validate_trades_data,
    validate_ohlcv_data,
    add_metadata_columns,
    add_interval_column,
    calculate_price_change,
    calculate_volume_metrics,
    add_candle_pattern
)
from technical_indicators import calculate_all_indicators
from mongodb_writer import (
    write_trades_to_mongodb,
    write_ohlcv_to_mongodb,
    write_indicators_to_mongodb,
    write_to_console,
    await_termination,
    stop_all_queries
)
from config import processing_config

# Configure logging
import os
_log_handlers = [logging.StreamHandler(sys.stdout)]
_log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
try:
    os.makedirs(_log_dir, exist_ok=True)
    _log_handlers.append(logging.FileHandler(os.path.join(_log_dir, 'spark_processing.log')))
except OSError:
    pass  # Fallback to stdout only (e.g. inside Docker container)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
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
        ohlcv_1m_df = aggregate_trades_to_ohlcv(
            trades_df,
            window_duration="1 minute"
        )
        
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
        # STEP 4: Calculate Technical Indicators & Anomalies (foreachBatch)
        # ============================================
        logger.info("\n[STEP 4] Calculating technical indicators & detecting anomalies...")
        
        def process_micro_batch(batch_df, batch_id):
            # Skip empty batches
            if batch_df.count() == 0:
                return

            logger.info(f"Processing micro-batch {batch_id} (count: {batch_df.count()})")
            
            # Since we are in a batch context now, we can use Window functions safely
            from pyspark.sql.functions import avg
            
            # 1. Calculate technical indicators
            indicators_df = calculate_all_indicators(batch_df)
            
            # 2. Add an avg_volume_batch for anomaly detection logic (per symbol)
            # We can calculate the average volume in this micro-batch
            batch_df_with_avg_vol = indicators_df.withColumn(
                "avg_volume_batch", 
                avg("volume").over(
                    __import__('pyspark.sql.window', fromlist=['Window']).Window.partitionBy("symbol")
                )
            )

            # 3. Detect Anomalies
            from transformations import detect_anomalies
            anomalies_df = detect_anomalies(batch_df_with_avg_vol)
            
            # Filter only anomalies for the anomalies collection
            anomalies_to_write = anomalies_df.filter(col("is_anomaly") == True)
            
            # Select relevant columns for indicators collection
            inds_to_write = indicators_df.select(
                "symbol", "interval", "timestamp",
                "rsi_14", "macd", "macd_signal", "macd_histogram",
                "bollinger_upper", "bollinger_middle", "bollinger_lower",
                "sma_20", "sma_50", "sma_200", "ema_12", "ema_26"
            )
            inds_to_write = add_metadata_columns(inds_to_write)
            
            # Select relevant columns for anomalies collection
            anomalies_to_write = anomalies_to_write.select(
                "symbol", "interval", "timestamp", "close", "volume",
                "price_change_percent", "is_price_drop_anomaly", "is_volume_spike_anomaly"
            )
            anomalies_to_write = add_metadata_columns(anomalies_to_write)
            
            # Write batch data to MongoDB
            if debug:
                logger.info("--- DEBUG: Indicators ---")
                inds_to_write.show(5, truncate=False)
                logger.info("--- DEBUG: Anomalies ---")
                anomalies_to_write.show(5, truncate=False)
            else:
                from mongodb_writer import write_to_mongodb_batch
                from config import mongodb_config
                
                # Write to indicators collection
                write_to_mongodb_batch(
                    inds_to_write, 
                    mongodb_config.indicators_collection, 
                    mode="append"
                )
                
                # Write to anomalies collection
                if anomalies_to_write.count() > 0:
                    logger.warning(f"🚨 Detected {anomalies_to_write.count()} anomalies in batch {batch_id}!")
                    write_to_mongodb_batch(
                        anomalies_to_write, 
                        mongodb_config.anomalies_collection, 
                        mode="append"
                    )

        # Apply the foreachBatch processor on klines_df
        query_indicators = (klines_df.writeStream
            .outputMode("append")
            .foreachBatch(process_micro_batch)
            .start()
        )
        queries.append(query_indicators)
        
        # ============================================
        # STEP 5: Monitor and Wait
        # ============================================
        logger.info("\n[STEP 5] Pipeline running. Monitoring queries...")
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
    Runs a batch processing pipeline (for historical data)
    
    Args:
        spark: SparkSession instance
    """
    logger.info("=" * 60)
    logger.info("BATCH PIPELINE - Not yet implemented")
    logger.info("=" * 60)
    logger.warning("Batch mode is not yet implemented. Use streaming mode.")


def main():
    """
    Main entry point for the pipeline
    """
    parser = argparse.ArgumentParser(description='Crypto ETL Pipeline - P3')
    parser.add_argument(
        '--mode',
        type=str,
        choices=['streaming', 'batch', 'test'],
        default='streaming',
        help='Pipeline mode (default: streaming)'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug mode (output to console instead of MongoDB)'
    )
    parser.add_argument(
        '--duration',
        type=int,
        help='Duration in seconds (for test mode)',
        default=60
    )
    
    args = parser.parse_args()
    
    # Create Spark session
    logger.info("Initializing Spark session...")
    spark = create_spark_session()
    
    try:
        if args.mode == 'streaming' or args.mode == 'test':
            run_streaming_pipeline(spark, debug=args.debug or args.mode == 'test')
        elif args.mode == 'batch':
            run_batch_pipeline(spark)
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        # Stop Spark session
        stop_spark_session(spark)


if __name__ == "__main__":
    main()

"""
MongoDB Writer Module - P3
Handles writing DataFrames to MongoDB collections with error handling and retry logic
"""

from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery
import logging
try:
    from .config import mongodb_config, spark_config
except ImportError:
    from config import mongodb_config, spark_config

logger = logging.getLogger(__name__)


def write_to_mongodb_batch(
    df: DataFrame,
    collection: str,
    mode: str = "append"
) -> None:
    """
    Writes a batch DataFrame to MongoDB
    
    Args:
        df: DataFrame to write
        collection: MongoDB collection name
        mode: Write mode ('append', 'overwrite', 'ignore')
    """
    logger.info(f"Writing batch data to MongoDB collection: {collection}")
    logger.info(f"  - Mode: {mode}")
    logger.info(f"  - Database: {mongodb_config.database}")
    
    try:
        (df.write
         .format("mongodb")
         .mode(mode)
         .option("connection.uri", mongodb_config.uri)
         .option("database", mongodb_config.database)
         .option("collection", collection)
         .option("replaceDocument", "false")
         .save()
        )
        
        logger.info(f"Successfully wrote to {collection}")
        
    except Exception as e:
        logger.error(f"Error writing to MongoDB collection {collection}: {str(e)}")
        raise


def write_to_mongodb_stream(
    df: DataFrame,
    collection: str,
    checkpoint_location: str = None,
    trigger_interval: str = None,
    output_mode: str = "append"
) -> StreamingQuery:
    """
    Writes a streaming DataFrame to MongoDB
    
    Args:
        df: Streaming DataFrame to write
        collection: MongoDB collection name
        checkpoint_location: Checkpoint directory (default from config)
        trigger_interval: Processing trigger interval (default from config)
        output_mode: Output mode ('append', 'update', 'complete')
    
    Returns:
        StreamingQuery object
    """
    checkpoint_location = checkpoint_location or f"{spark_config.checkpoint_location}/{collection}"
    trigger_interval = trigger_interval or spark_config.trigger_interval
    
    logger.info(f"Starting streaming write to MongoDB collection: {collection}")
    logger.info(f"  - Output mode: {output_mode}")
    logger.info(f"  - Trigger interval: {trigger_interval}")
    logger.info(f"  - Checkpoint: {checkpoint_location}")
    
    try:
        query = (df.writeStream
                 .format("mongodb")
                 .outputMode(output_mode)
                 .option("connection.uri", mongodb_config.uri)
                 .option("database", mongodb_config.database)
                 .option("collection", collection)
                 .option("checkpointLocation", checkpoint_location)
                 .option("replaceDocument", "false")
                 .trigger(processingTime=trigger_interval)
                 .start()
        )
        
        logger.info(f"Streaming query started for {collection}")
        logger.info(f"   Query ID: {query.id}")
        
        return query
        
    except Exception as e:
        logger.error(f"Error starting streaming query for {collection}: {str(e)}")
        raise


def write_trades_to_mongodb(df: DataFrame, streaming: bool = True) -> StreamingQuery:
    """
    Writes trades data to MongoDB raw_trades collection
    
    Args:
        df: Trades DataFrame
        streaming: Whether this is a streaming DataFrame
    
    Returns:
        StreamingQuery if streaming, None otherwise
    """
    collection = mongodb_config.raw_trades_collection
    
    if streaming:
        return write_to_mongodb_stream(df, collection)
    else:
        write_to_mongodb_batch(df, collection)
        return None


def write_ohlcv_to_mongodb(df: DataFrame, streaming: bool = True) -> StreamingQuery:
    """
    Writes OHLCV data to MongoDB ohlcv collection
    
    Args:
        df: OHLCV DataFrame
        streaming: Whether this is a streaming DataFrame
    
    Returns:
        StreamingQuery if streaming, None otherwise
    """
    collection = mongodb_config.ohlcv_collection
    
    if streaming:
        return write_to_mongodb_stream(df, collection)
    else:
        write_to_mongodb_batch(df, collection)
        return None


def write_indicators_to_mongodb(df: DataFrame, streaming: bool = True) -> StreamingQuery:
    """
    Writes technical indicators to MongoDB indicators collection
    
    Args:
        df: Indicators DataFrame
        streaming: Whether this is a streaming DataFrame
    
    Returns:
        StreamingQuery if streaming, None otherwise
    """
    collection = mongodb_config.indicators_collection
    
    if streaming:
        return write_to_mongodb_stream(df, collection)
    else:
        write_to_mongodb_batch(df, collection)
        return None


def write_anomalies_to_mongodb(df: DataFrame, streaming: bool = True) -> StreamingQuery:
    """
    Writes anomalies to MongoDB anomalies collection
    
    Args:
        df: Anomalies DataFrame
        streaming: Whether this is a streaming DataFrame
    
    Returns:
        StreamingQuery if streaming, None otherwise
    """
    collection = mongodb_config.anomalies_collection
    
    if streaming:
        return write_to_mongodb_stream(df, collection)
    else:
        write_to_mongodb_batch(df, collection)
        return None


def write_aggregated_metrics_to_mongodb(df: DataFrame, streaming: bool = True) -> StreamingQuery:
    """
    Writes aggregated metrics to MongoDB aggregated_metrics collection
    
    Args:
        df: Aggregated metrics DataFrame
        streaming: Whether this is a streaming DataFrame
    
    Returns:
        StreamingQuery if streaming, None otherwise
    """
    collection = mongodb_config.daily_metrics_collection
    
    if streaming:
        return write_to_mongodb_stream(df, collection)
    else:
        write_to_mongodb_batch(df, collection)
        return None


def write_to_console(
    df: DataFrame,
    truncate: bool = False,
    num_rows: int = 20
) -> StreamingQuery:
    """
    Writes streaming DataFrame to console for debugging
    
    Args:
        df: Streaming DataFrame
        truncate: Whether to truncate long values
        num_rows: Number of rows to display
    
    Returns:
        StreamingQuery object
    """
    logger.info("Starting console output for debugging...")
    
    query = (df.writeStream
             .format("console")
             .outputMode("append")
             .option("truncate", str(truncate).lower())
             .option("numRows", num_rows)
             .trigger(processingTime=spark_config.trigger_interval)
             .start()
    )
    
    logger.info("Console output started")
    return query


def await_termination(queries: list, timeout: int = None):
    """
    Waits for all streaming queries to terminate
    
    Args:
        queries: List of StreamingQuery objects
        timeout: Timeout in seconds (None for infinite)
    """
    logger.info(f"Awaiting termination of {len(queries)} streaming queries...")
    
    try:
        for query in queries:
            if query is not None:
                logger.info(f"  - Waiting for query: {query.id}")
                if timeout:
                    query.awaitTermination(timeout)
                else:
                    query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, stopping queries...")
        stop_all_queries(queries)
    except Exception as e:
        logger.error(f"Error during query execution: {str(e)}")
        stop_all_queries(queries)
        raise


def stop_all_queries(queries: list):
    """
    Stops all streaming queries gracefully
    
    Args:
        queries: List of StreamingQuery objects
    """
    logger.info(f"Stopping {len(queries)} streaming queries...")
    
    for query in queries:
        if query is not None and query.isActive:
            try:
                query.stop()
                logger.info(f"Stopped query: {query.id}")
            except Exception as e:
                logger.error(f"Error stopping query {query.id}: {str(e)}")
    
    logger.info("All queries stopped")


def get_query_status(query: StreamingQuery) -> dict:
    """
    Gets the status of a streaming query
    
    Args:
        query: StreamingQuery object
    
    Returns:
        Dictionary with query status information
    """
    if query is None:
        return {"status": "not_started"}
    
    return {
        "id": query.id,
        "name": query.name,
        "is_active": query.isActive,
        "recent_progress": query.recentProgress
    }

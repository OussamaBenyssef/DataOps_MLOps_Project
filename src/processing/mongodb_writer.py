"""
MongoDB Writer pour la persistance des données traitées
"""
import logging
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

from config import MongoDBConfig

logger = logging.getLogger(__name__)


def _get_mongo_options(config: MongoDBConfig, collection: str) -> dict:
    return {
        "spark.mongodb.write.connection.uri": config.uri,
        "spark.mongodb.write.database": config.database,
        "spark.mongodb.write.collection": collection,
        "spark.mongodb.write.operationType": "insert",
        "spark.mongodb.write.ordered": str(config.ordered).lower(),
    }


def write_batch_to_mongodb(df: DataFrame, config: MongoDBConfig, collection: str) -> None:
    """Écriture batch (non-streaming) vers MongoDB"""
    opts = _get_mongo_options(config, collection)
    logger.info(f"[MongoDB] Écriture batch → {collection} ({df.count()} lignes)")
    df.write.format("mongodb").options(**opts).mode("append").save()
    logger.info(f"[MongoDB] Écriture batch terminée → {collection}")


def write_ohlcv_stream(df: DataFrame, config: MongoDBConfig, checkpoint_path: str) -> StreamingQuery:
    """Écriture streaming des données OHLCV vers MongoDB"""
    opts = _get_mongo_options(config, config.ohlcv_collection)

    def foreach_batch_fn(batch_df, batch_id):
        if not batch_df.isEmpty():
            batch_df.write.format("mongodb").options(**opts).mode("append").save()
            logger.info(f"[MongoDB] Batch {batch_id}: {batch_df.count()} OHLCV écrites")

    return (
        df.writeStream
        .foreachBatch(foreach_batch_fn)
        .option("checkpointLocation", f"{checkpoint_path}/ohlcv")
        .outputMode("update")
        .start()
    )


def write_indicators_stream(df: DataFrame, config: MongoDBConfig, checkpoint_path: str) -> StreamingQuery:
    """Écriture streaming des indicateurs techniques vers MongoDB"""
    opts = _get_mongo_options(config, config.indicators_collection)

    def foreach_batch_fn(batch_df, batch_id):
        if not batch_df.isEmpty():
            batch_df.write.format("mongodb").options(**opts).mode("append").save()
            logger.info(f"[MongoDB] Batch {batch_id}: {batch_df.count()} indicateurs écrits")

    return (
        df.writeStream
        .foreachBatch(foreach_batch_fn)
        .option("checkpointLocation", f"{checkpoint_path}/indicators")
        .outputMode("update")
        .start()
    )


def write_to_console(df: DataFrame, num_rows: int = 20, truncate: bool = False) -> StreamingQuery:
    """Mode debug: sortie console (pour tests sans MongoDB)"""
    return (
        df.writeStream
        .format("console")
        .option("numRows", num_rows)
        .option("truncate", truncate)
        .outputMode("update")
        .start()
    )

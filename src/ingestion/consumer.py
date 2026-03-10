import os
import json
import logging
from kafka import KafkaConsumer

# Optional import, fallback to relative if run as script or module
try:
    from src.ingestion.models import TradeData, KlineData, ValidationError
except ImportError:
    from models import TradeData, KlineData, ValidationError

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP", "data_validation_group")


def process_message(topic, message_value):
    """
    Deserizalizes JSON and validates using dataclass models based on the topic.
    """
    try:
        data = json.loads(message_value)
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON received from topic {topic}: {message_value[:200]}... Error: {e}")
        return

    try:
        if topic == "raw_trades":
            validated_data = TradeData.from_dict(data)
            logger.info(
                f"Valid trade received for {validated_data.symbol}: {validated_data.quantity} @ {validated_data.price}"
            )
            # Further logic can be added here: producing to a structured/cleaned topic, saving to DB, etc.
        elif topic == "raw_klines":
            validated_data = KlineData.from_dict(data)
            logger.info(
                f"Valid kline received for {validated_data.symbol}: "
                f"Open={validated_data.kline.open_price}, Close={validated_data.kline.close_price}"
            )
        else:
            logger.warning(f"Message from unknown topic {topic}")
    except ValidationError as e:
        logger.error(f"Data validation failed for topic {topic}. Data: {data}. Errors: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error processing message: {e}")


def consume_loop(consumer):
    """
    Main Kafka consume loop.
    """
    try:
        logger.info("Consumer started, waiting for messages...")
        for message in consumer:
            topic = message.topic
            value = message.value.decode("utf-8")
            process_message(topic, value)
    except KeyboardInterrupt:
        logger.info("Consumer loop aborted by user.")
    except Exception as e:
        logger.error(f"Unexpected error in consume loop: {e}")
    finally:
        consumer.close()
        logger.info("Kafka consumer closed.")


if __name__ == "__main__":
    topics_to_consume = ["raw_trades", "raw_klines"]

    try:
        logger.info(f"Starting consumer connected to {KAFKA_BROKER}")
        consumer = KafkaConsumer(
            *topics_to_consume,
            bootstrap_servers=[KAFKA_BROKER],
            group_id=GROUP_ID,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        consume_loop(consumer)
    except Exception as e:
        logger.error(f"Failed to start consumer: {e}")

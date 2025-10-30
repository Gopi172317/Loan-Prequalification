import os
import json
import logging
from confluent_kafka import Producer

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_LOAN_APPLICATIONS_TOPIC", "loan_applications_submitted")

# module-level producer reused across calls
_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})


def _delivery_report(err, msg):
    if err:
        logger.error("Delivery failed for message: %s", err)
    else:
        logger.info("Message delivered to %s [%d] at offset %s", msg.topic(), msg.partition(), msg.offset())


def publish_application(payload: dict, topic: str = KAFKA_TOPIC, timeout: float = 1.0) -> None:
    """
    Publish the given payload (dict) to the configured Kafka topic.
    This is fire-and-forget but will flush for a short timeout to improve delivery reliability.
    """
    try:
        _producer.produce(topic, value=json.dumps(payload).encode("utf-8"), callback=_delivery_report)
        # serve delivery callbacks and attempt to send outstanding messages
        _producer.poll(0)
        _producer.flush(timeout)
    except Exception as exc:
        logger.exception("Failed to publish application to Kafka: %s", exc)
# ...existing code...
from services.decision_consumer import DecisionConsumer
from config import KafkaConfig, DBConfig
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting Decision Service...")
    consumer = DecisionConsumer(
        bootstrap_servers=KafkaConfig.BOOTSTRAP_SERVERS,
        input_topic=KafkaConfig.CREDIT_REPORTS_TOPIC,
        database_url=DBConfig.DATABASE_URL
    )
    consumer.run()

if __name__ == "__main__":
    main()
# ...existing code...
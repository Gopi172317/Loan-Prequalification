from services.credit_consumer import CreditConsumer
from config import KafkaConfig
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting Credit Service...")
    consumer = CreditConsumer(
        bootstrap_servers=KafkaConfig.BOOTSTRAP_SERVERS,
        input_topic=KafkaConfig.LOAN_APPLICATIONS_TOPIC
    )
    consumer.run()

if __name__ == "__main__":
    main()
    
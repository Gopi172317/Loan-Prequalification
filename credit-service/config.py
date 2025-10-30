import os
from dotenv import load_dotenv

load_dotenv(override=True)

class KafkaConfig:
    BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    LOAN_APPLICATIONS_TOPIC = os.getenv("KAFKA_LOAN_APPLICATIONS_TOPIC", "loan_applications_submitted")
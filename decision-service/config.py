# ...existing code...
import os
from dotenv import load_dotenv

load_dotenv(override=True)

class KafkaConfig:
    BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    CREDIT_REPORTS_TOPIC = os.getenv("KAFKA_CREDIT_REPORTS_TOPIC", "credit_reports_generated")

class DBConfig:
    DATABASE_URL = os.getenv("DATABASE_URL", os.getenv("DATABASE_URL", "postgresql+psycopg2://postgres:loan_prequal@localhost:5432/loan_prequalification"))
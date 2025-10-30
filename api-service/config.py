import os
import dotenv

dotenv.load_dotenv(override=True)


class Config:
    DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql+psycopg2://postgres:mysecretpassword@")
    
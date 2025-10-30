from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from config import Config

# 1. Define the Database URL
DATABASE_URL = Config.DATABASE_URL

# 2. Create the SQLAlchemy Engine
engine = create_engine(
    DATABASE_URL,
    echo=False  # Set to True to see SQL queries in console
)

# 3. Set up the SessionLocal (SessionMaker)
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

# 4. Define the Declarative Base
Base = declarative_base()


def get_db():
    """
    Dependency function to get a database session.
    Used for dependency injection in framework routers (like FastAPI/Flask).
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        # Ensures the connection is closed after the request is finished
        db.close()

from api.routes.applications import app
from db.database import Base, engine
import uvicorn


def initialize_database():
    """
    Creates all tables defined by models that inherit from Base.
    """
    print("Creating database tables...")
    # This command checks all classes inheriting from Base and creates
    # the corresponding tables if they don't already exist.
    Base.metadata.create_all(bind=engine)
    print("Tables created successfully.")

if __name__ == "__main__":
    initialize_database()
    uvicorn.run(app, host="0.0.0.0", port=8000)

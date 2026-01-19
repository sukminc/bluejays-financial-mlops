import os
from sqlalchemy import create_engine
from src.models import Base

# Connection string for Docker environment
# Format: postgresql://user:password@host:port/dbname
db_conn = os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")
DB_URL = db_conn.replace("airflow", "postgres")


def init_db():
    engine = create_engine(DB_URL)
    print("Connecting to Postgres to initialize schema...")
    # This creates the tables if they do not exist
    Base.metadata.create_all(engine)
    print("Database schema initialized successfully.")


if __name__ == "__main__":
    init_db()

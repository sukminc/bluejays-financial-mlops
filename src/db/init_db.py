# src/db/init_db.py
from __future__ import annotations

from sqlalchemy import create_engine

from src.db.models import Base

DB_URL = "postgresql://airflow:airflow@postgres:5432/airflow"


def init_db_schema() -> None:
    engine = create_engine(DB_URL)
    Base.metadata.create_all(bind=engine)


def main() -> None:
    init_db_schema()
    print("DB schema init complete.")


if __name__ == "__main__":
    main()

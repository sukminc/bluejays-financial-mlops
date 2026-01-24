import sys
import argparse
import pandas as pd
from sqlalchemy import create_engine, text
from src.db.models import Base

# Configuration
DB_URL = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
DEFAULT_CSV = "/opt/airflow/data/raw/manual/spotrac_bluejays_2025.csv"
TABLE_NAME = "stg_spotrac_bluejays_salary_raw"


def ingest_csv(csv_path):
    """
    Reads the CSV and loads it into the raw staging table.
    """
    try:
        print(f"🚀 Starting Ingestion: {csv_path}")
        engine = create_engine(DB_URL)

        # 1. Initialize Schema
        Base.metadata.create_all(engine)

        # 2. Read CSV
        df = pd.read_csv(csv_path)
        df.columns = [c.lower().replace(" ", "_") for c in df.columns]

        print(f"📊 Rows found in CSV: {len(df)}")

        # 3. Load to DB
        with engine.begin() as conn:
            conn.execute(
                text(f"TRUNCATE TABLE {TABLE_NAME} RESTART IDENTITY;")
                )

            df.to_sql(
                TABLE_NAME,
                conn,
                if_exists='append',
                index=False,
                dtype=None
            )

        print(f"✅ Successfully loaded {len(df)} rows into {TABLE_NAME}")

    except FileNotFoundError:
        print(f"❌ Error: CSV file not found at {csv_path}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Critical Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        default=DEFAULT_CSV,
        help="Path to the input CSV file"
    )
    args = parser.parse_args()

    ingest_csv(args.input)

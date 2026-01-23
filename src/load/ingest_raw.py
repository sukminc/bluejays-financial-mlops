import sys
import pandas as pd
from sqlalchemy import create_engine, text
from src.db.models import Base

# Configuration
DB_URL = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
CSV_PATH = "/opt/airflow/data/raw/manual/spotrac_bluejays_2025.csv"
TABLE_NAME = "stg_spotrac_bluejays_salary_raw"


def ingest_csv():
    """
    Reads the manual CSV and loads it into the raw staging table.
    Performs a TRUNCATE before INSERT (Full Refresh).
    """
    try:
        print(f"🚀 Starting Ingestion: {CSV_PATH}")
        engine = create_engine(DB_URL)

        # 1. Initialize Schema (Create table if it does not exist)
        Base.metadata.create_all(engine)

        # 2. Read CSV using Pandas
        df = pd.read_csv(CSV_PATH)

        # Standardize column names:
        # lowercase and replace spaces with underscores
        df.columns = [c.lower().replace(" ", "_") for c in df.columns]

        print(f"📊 Rows found in CSV: {len(df)}")

        # 3. Load to DB using a Transaction
        with engine.begin() as conn:
            # Truncate is faster and cleaner for full reloads
            conn.execute(
                text(f"TRUNCATE TABLE {TABLE_NAME} RESTART IDENTITY;")
                )

            # Pandas to SQL: Append rows to the truncated table
            df.to_sql(
                TABLE_NAME,
                conn,
                if_exists='append',
                index=False,
                dtype=None  # Let SQLAlchemy handle types based on Model
            )

        print(f"✅ Successfully loaded {len(df)} rows into {TABLE_NAME}")

    except FileNotFoundError:
        print(f"❌ Error: CSV file not found at {CSV_PATH}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Critical Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    ingest_csv()

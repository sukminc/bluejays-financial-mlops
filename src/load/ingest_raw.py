import sys
import argparse
import pandas as pd
from sqlalchemy import create_engine, text
from src.db.models import Base

# Configuration
DB_URL = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
DEFAULT_CSV = "/opt/airflow/data/raw/manual/spotrac_bluejays_2025.csv"
DEFAULT_TABLE = "stg_spotrac_bluejays_salary_raw"


def ingest_csv(csv_path, table_name):
    """
    Reads the CSV and loads it into the specified table.
    """
    try:
        print(f"🚀 Starting Ingestion: {csv_path} -> {table_name}")
        engine = create_engine(DB_URL)

        # 1. Read CSV
        # [FIX] Added dtype=str to force all columns to be raw TEXT.
        # This prevents Pandas from inferring
        # 'Int' for salaries in the Test suite,
        # which causes the Regex SQL check to fail.
        df = pd.read_csv(csv_path, dtype=str)
        df.columns = [c.lower().replace(" ", "_") for c in df.columns]
        print(f"📊 Rows found in CSV: {len(df)}")

        # 2. Load to DB
        with engine.begin() as conn:
            if table_name == DEFAULT_TABLE:
                # Production Mode: Ensure Schema exists, Truncate, Append
                Base.metadata.create_all(engine)
                conn.execute(
                    text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY;"))
                df.to_sql(table_name, conn, if_exists='append', index=False)
            else:
                # Test Mode: Create/Replace table on the fly
                # Since we used dtype=str,
                # this will create TEXT columns in Postgres
                df.to_sql(table_name, conn, if_exists='replace', index=False)

        print(f"✅ Successfully loaded {len(df)} rows into {table_name}")

    except FileNotFoundError:
        print(f"❌ Error: CSV file not found at {csv_path}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Critical Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=DEFAULT_CSV)
    parser.add_argument("--table", default=DEFAULT_TABLE)
    args = parser.parse_args()

    ingest_csv(args.input, args.table)

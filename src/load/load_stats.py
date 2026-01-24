import pandas as pd
import glob
import os
import sys
from sqlalchemy import create_engine, text
from src.db.models import Base

# Configuration
DB_URL = os.getenv(
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
    "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
)

STATS_DIR = "data/raw/stats"


def load_all_stats():
    """
    Loads all MLB Stats CSVs into the fact_player_stats table.
    Performs a full refresh (Drop/Create) to ensure schema consistency.
    """
    print("=== 🚀 Starting Deep Stats Ingestion ===")

    try:
        engine = create_engine(DB_URL)

        # 1. Drop old table (Use connection for DDL)
        print("🗑️  Dropping old table to ensure schema match...")
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS fact_player_stats"))

        # 2. Re-create table schema
        print("🔨 Creating new table schema...")
        Base.metadata.create_all(engine)

        # 3. Find files
        csv_pattern = os.path.join(STATS_DIR, "mlb_stats_bluejays_*.csv")
        csv_files = glob.glob(csv_pattern)

        if not csv_files:
            print(f"❌ No CSV files found in {STATS_DIR}")
            sys.exit(1)

        total_rows = 0

        for csv_file in sorted(csv_files):
            try:
                print(f"📄 Processing: {os.path.basename(csv_file)}")
                df = pd.read_csv(csv_file)
                df.columns = [c.lower() for c in df.columns]

                # [FIX] Reverted to passing 'engine'.
                # With updated Pandas/SQLAlchemy,
                # this is the standard, stable method.
                df.to_sql(
                    'fact_player_stats',
                    con=engine,  # <--- WE ARE BACK TO USING ENGINE
                    if_exists='append',
                    index=False
                )

                count = len(df)
                total_rows += count
                print(f"   ✅ Loaded {count} rows.")

            except Exception as e:
                print(f"   ❌ Error loading {csv_file}: {e}")
                sys.exit(1)

        print(f"🎉 Success! Total Loaded: {total_rows} rows.")

    except Exception as e:
        print(f"❌ Critical Database Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    load_all_stats()

import pandas as pd
import os
import sys
from sqlalchemy import create_engine, text

# Configuration
DB_URL = os.getenv(
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
    "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
)

# New Combined CSV Path
CSV_PATH = "data/raw/spotrac/bluejays_salary_history.csv"


def load_raw_data():
    print("=== 🚀 Starting Salary Data Ingestion (2023-2026) ===")

    try:
        if not os.path.exists(CSV_PATH):
            print(f"❌ Error: CSV file not found at {CSV_PATH}")
            sys.exit(1)

        engine = create_engine(DB_URL)

        # 1. Read the Combined CSV
        print(f"   📄 Reading {CSV_PATH}...")
        df = pd.read_csv(CSV_PATH)

        # 2. Add missing columns expected by Staging Table (if any)
        # The staging table expects specific columns.
        # Since our CSV is simpler, we fill these with defaults.
        if 'luxury_tax_usd' not in df.columns:
            # Assume same for now
            df['luxury_tax_usd'] = df['cash_salary_usd']
        if 'luxury_tax_pct' not in df.columns:
            df['luxury_tax_pct'] = 0
        if 'contract_type' not in df.columns:
            df['contract_type'] = 'Standard'
        if 'free_agent_year' not in df.columns:
            df['free_agent_year'] = 'Unknown'

        # 3. Load to Staging (Replace Old Data)
        print("   💾 Uploading to 'stg_spotrac_bluejays_salary_raw'...")
        with engine.begin() as conn:
            # We truncate (wipe) the table first to avoid duplicates
            conn.execute(
                text("TRUNCATE TABLE stg_spotrac_bluejays_salary_raw "
                     "RESTART IDENTITY")
            )

            df.to_sql(
                'stg_spotrac_bluejays_salary_raw',
                con=conn,
                if_exists='append',
                index=False
            )

        print(f"   ✅ Success! Loaded {len(df)} rows (2023-2026) into Staging.")

    except Exception as e:
        print(f"❌ Load Failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    load_raw_data()

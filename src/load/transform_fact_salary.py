import pandas as pd
import os
import sys
from sqlalchemy import create_engine, text
from src.db.models import Base

# Configuration
DB_URL = os.getenv(
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
    "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
)


def run_transform():
    print("=== 🔄 Starting Fact Salary Transformation ===")

    try:
        engine = create_engine(DB_URL)

        # 1. Read from Staging
        print("   📄 Extracting data...")
        query = """
        SELECT
            season,
            player_name,
            cash_salary_usd,
            luxury_tax_usd,
            luxury_tax_pct,
            contract_type
        FROM stg_spotrac_bluejays_salary_raw
        """
        df = pd.read_sql(query, engine)
        # 2. Clean Data
        df['cash_salary'] = df['cash_salary_usd'].replace(
            {r'\$': '', ',': ''}, regex=True
        )
        df['luxury_tax_salary'] = df['luxury_tax_usd'].replace(
            {r'\$': '', ',': ''}, regex=True
        )

        df['cash_salary'] = pd.to_numeric(
            df['cash_salary'], errors='coerce'
        ).fillna(0)

        df['luxury_tax_salary'] = pd.to_numeric(
            df['luxury_tax_salary'], errors='coerce'
        ).fillna(0)

        df['luxury_tax_pct'] = pd.to_numeric(
            df['luxury_tax_pct'], errors='coerce'
        ).fillna(0)

        # 3. Prepare DataFrame
        df_fact = df[[
            'season',
            'player_name',
            'cash_salary',
            'luxury_tax_salary',
            'luxury_tax_pct',
            'contract_type'
        ]].copy()

        # 4. [CRITICAL CHANGE] Split DDL and DML to avoid Deadlocks
        # Part A: Drop and Create Table (Transaction 1)
        print("   🔨 Recreating Schema (DDL)...")
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS fact_salary"))
            # Pass 'conn' here so it happens in the SAME transaction
            Base.metadata.create_all(conn)

        # Part B: Insert Data (Transaction 2)
        # Now that the table exists and Trans 1 is closed, we can safely insert
        print("   💾 Loading Data (DML)...")
        df_fact.to_sql(
            'fact_salary',
            con=engine,
            # Engine is safe to use now
            if_exists='append',
            index=False
        )

        print(f"   ✅ Success! Loaded {len(df_fact)} rows into fact_salary.")

    except Exception as e:
        print(f"❌ Transformation Failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    run_transform()

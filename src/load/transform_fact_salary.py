import os
import pandas as pd
from sqlalchemy import create_engine, text
from src.db.models import Base, FactSalary  # FactSalary 임포트 필요

# Configuration
DB_URL = os.getenv(
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
    "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
)


def transform_and_load():
    """
    Reads from Raw Staging, cleans currency/numeric fields,
    and loads into the Fact Table.
    """
    print("=== 🏗️ Starting Transformation: Raw -> Fact Salary ===")
    engine = create_engine(DB_URL)

    print("🗑️ Dropping old fact_salary table to ensure schema match...")
    FactSalary.__table__.drop(engine, checkfirst=True)

    Base.metadata.create_all(engine)

    with engine.connect() as conn:
        df_raw = pd.read_sql(
            "SELECT * FROM stg_spotrac_bluejays_salary_raw",
            conn
            )

    if df_raw.empty:
        print("⚠️ No raw data found. Skipping transformation.")
        return

    print(f"📊 Raw Rows Input: {len(df_raw)}")

    # 3. Data Cleaning Function
    def clean_currency(val):
        """Removes '$' and ',' and converts to integer."""
        if not val or pd.isna(val):
            return None
        # Normalize text: remove whitespace and currency symbols
        s_val = str(val).lower().replace('$', '').replace(',', '').strip()
        if s_val in ['unknown', 'invaliddata', '']:
            return None
        try:
            return int(float(s_val))
        except ValueError:
            return None

    # Apply Cleaning
    df_raw['clean_luxury_tax'] = df_raw['luxury_tax_usd'].apply(clean_currency)
    df_raw['clean_cash'] = df_raw['cash_salary_usd'].apply(clean_currency)
    df_raw['clean_season'] = pd.to_numeric(df_raw['season'], errors='coerce')

    # 4. Filter: Drop rows where cleaning failed (e.g., 'Unknown' salary)
    df_valid = df_raw.dropna(subset=['clean_luxury_tax', 'clean_season'])
    dropped_count = len(df_raw) - len(df_valid)

    if dropped_count > 0:
        print(f"🧹 Dropped {dropped_count} rows due to invalid types.")

    # 5. Prepare Final DataFrame
    df_final = pd.DataFrame({
        'season': df_valid['clean_season'],
        'luxury_tax_salary': df_valid['clean_luxury_tax'],
        'cash_salary': df_valid['clean_cash'],
        'luxury_tax_pct': pd.to_numeric(
            df_valid['luxury_tax_pct'], errors='coerce'
        ),
        'contract_type': df_valid['contract_type'],
        'player_id': None  # Placeholder
    })

    print(f"✅ Final Rows to Load: {len(df_final)}")

    # 6. Load to Fact Table
    if not df_final.empty:
        with engine.begin() as conn:
            # Drop/Create를 했으므로 Truncate는 굳이 필요 없지만 안전장치로 유지
            conn.execute(text("TRUNCATE TABLE fact_salary RESTART IDENTITY;"))
            df_final.to_sql(
                'fact_salary',
                conn,
                if_exists='append',
                index=False
            )
        print("🎉 Successfully loaded fact_salary.")
    else:
        print("❌ Transformation resulted in 0 rows.")


if __name__ == "__main__":
    transform_and_load()

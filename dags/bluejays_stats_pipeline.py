import pandas as pd
import os
import sys
from sqlalchemy import create_engine

# Configuration
DB_URL = os.getenv(
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
    "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
)

# Raw Data
RAW_DATA = [
    # 2023
    [2023, 'George Springer', 'RF', 33, 'Active', 24166666],
    [2023, 'Kevin Gausman', 'SP', 32, 'Active', 21000000],
    [2023, 'Hyun Jin Ryu', 'SP', 36, 'Injured', 20000000],
    [2023, 'Chris Bassitt', 'SP', 34, 'Active', 19000000],
    [2023, 'Vladimir Guerrero Jr.', '1B', 24, 'Active', 14500000],
    [2023, 'Matt Chapman', '3B', 30, 'Active', 12500000],
    [2023, 'Jose Berrios', 'SP', 29, 'Active', 10714285],
    [2023, 'Yusei Kikuchi', 'SP', 32, 'Active', 10000000],
    [2023, 'Brandon Belt', 'DH', 35, 'Active', 9300000],
    [2023, 'Kevin Kiermaier', 'CF', 33, 'Active', 9000000],
    [2023, 'Whit Merrifield', '2B', 34, 'Active', 6750000],
    [2023, 'Yimi Garcia', 'RP', 32, 'Active', 5500000],
    [2023, 'Chad Green', 'RP', 32, 'Injured', 2250000],
    [2023, 'Danny Jansen', 'C', 28, 'Active', 3500000],
    [2023, 'Bo Bichette', 'SS', 25, 'Active', 3350000],
    [2023, 'Cavan Biggio', '2B', 28, 'Active', 2800000],
    [2023, 'Jordan Romano', 'RP', 30, 'Active', 4537500],
    [2023, 'Tim Mayza', 'RP', 31, 'Active', 2100000],
    [2023, 'Santiago Espinal', '2B', 28, 'Active', 2100000],
    [2023, 'Adam Cimber', 'RP', 32, 'Injured', 3150000],
    [2023, 'Trevor Richards', 'RP', 30, 'Active', 1500000],
    [2023, 'Erik Swanson', 'RP', 29, 'Active', 1250000],
    [2023, 'Daulton Varsho', 'CF', 26, 'Active', 3050000],
    [2023, 'Alejandro Kirk', 'C', 24, 'Active', 763500],
    [2023, 'Alek Manoah', 'SP', 25, 'Minors', 745600],
    # 2024
    [2024, 'George Springer', 'RF', 34, 'Active', 25000000],
    [2024, 'Kevin Gausman', 'SP', 33, 'Active', 22000000],
    [2024, 'Chris Bassitt', 'SP', 35, 'Active', 21000000],
    [2024, 'Vladimir Guerrero Jr.', '1B', 25, 'Active', 19900000],
    [2024, 'Jose Berrios', 'SP', 30, 'Active', 18714286],
    [2024, 'Bo Bichette', 'SS', 26, 'Injured', 11200000],
    [2024, 'Justin Turner', 'DH', 39, 'Traded', 10596774],
    [2024, 'Chad Green', 'RP', 33, 'Active', 10500000],
    [2024, 'Kevin Kiermaier', 'CF', 34, 'Traded', 8660000],
    [2024, 'Jordan Romano', 'RP', 31, 'Injured', 7750000],
    [2024, 'Isiah Kiner-Falefa', '2B', 29, 'Traded', 7250000],
    [2024, 'Yusei Kikuchi', 'SP', 33, 'Traded', 6612903],
    [2024, 'Daulton Varsho', 'CF', 27, 'Injured', 5650000],
    [2024, 'Cavan Biggio', '2B', 29, 'Traded', 3772335],
    [2024, 'Tim Mayza', 'RP', 32, 'Released', 3590000],
    [2024, 'Yimi Garcia', 'RP', 33, 'Traded', 3577957],
    [2024, 'Alejandro Kirk', 'C', 25, 'Active', 2800000],
    [2024, 'Erik Swanson', 'RP', 30, 'Active', 2750000],
    [2024, 'Genesis Cabrera', 'RP', 27, 'Active', 1512500],
    [2024, 'Ryan Yarbrough', 'RP', 32, 'Active', 1300000],
    [2024, 'Alek Manoah', 'SP', 26, 'Injured', 782500],
    [2024, 'Ernie Clement', 'SS', 28, 'Active', 757700],
    [2024, 'Davis Schneider', '3B', 25, 'Active', 744900],
    [2024, 'Bowden Francis', 'SP', 28, 'Active', 691784],
    [2024, 'Brendon Little', 'RP', 27, 'Active', 517140],
    [2024, 'Spencer Horwitz', '1B', 26, 'Active', 453492],
    [2024, 'Addison Barger', '3B', 24, 'Active', 405756],
    [2024, 'Ryan Burr', 'RP', 30, 'Active', 369954],
    [2024, 'Leonardo Jimenez', 'SS', 23, 'Active', 350064],
    [2024, 'Steward Berroa', 'OF', 25, 'Active', 286416],
    [2024, 'Joey Loperfido', 'LF', 25, 'Active', 238680],
    [2024, 'Dillon Tate', 'RP', 30, 'Active', 225820],
    [2024, 'Will Wagner', '2B', 25, 'Injured', 190944],
    [2024, 'Luis De Los Santos', 'SS', 26, 'Active', 167076],
    [2024, 'Brandon Eisert', 'RP', 26, 'Active', 135268],
    [2024, 'Nathan Lukes', 'CF', 29, 'Active', 123318],
    [2024, 'Tyler Heineman', 'C', 33, 'Active', 47736],
    [2024, 'Jonatan Clase', 'OF', 22, 'Active', 39780],
    [2024, 'Brett de Geus', 'RP', 26, 'Active', 15912],
    [2024, 'Nick Robertson', 'RP', 25, 'Active', 7956],
    # 2025
    [2025, 'Vladimir Guerrero Jr.', '1B', 26, 'Active', 29400000],
    [2025, 'George Springer', 'RF', 35, 'Active', 24166666],
    [2025, 'Kevin Gausman', 'SP', 34, 'Active', 23000000],
    [2025, 'Chris Bassitt', 'SP', 36, 'Active', 22000000],
    [2025, 'Jose Berrios', 'SP', 31, 'Active', 18714285],
    [2025, 'Bo Bichette', 'SS', 27, 'Active', 17583333],
    [2025, 'Chad Green', 'RP', 34, 'Active', 10500000],
    [2025, 'Jordan Romano', 'RP', 32, 'Active', 7750000],
    [2025, 'Daulton Varsho', 'CF', 28, 'Active', 7700000],
    [2025, 'Erik Swanson', 'RP', 31, 'Active', 3200000],
    [2025, 'Alejandro Kirk', 'C', 26, 'Active', 2800000],
    [2025, 'Genesis Cabrera', 'RP', 28, 'Active', 1512500],
    [2025, 'Dillon Tate', 'RP', 31, 'Active', 1500000],
    [2025, 'Will Wagner', '2B', 26, 'Active', 760000],
    [2025, 'Leo Jimenez', 'SS', 24, 'Active', 760000],
    [2025, 'Davis Schneider', '2B', 26, 'Active', 760000],
    [2025, 'Ernie Clement', 'SS', 29, 'Active', 760000],
    [2025, 'Bowden Francis', 'SP', 29, 'Active', 760000],
    [2025, 'Yariel Rodriguez', 'SP', 28, 'Active', 5000000],
    # 2026
    [2026, 'George Springer', 'RF', 36, 'Active', 24166666],
    [2026, 'Kevin Gausman', 'SP', 35, 'Active', 23000000],
    [2026, 'Jose Berrios', 'SP', 32, 'Active', 18714285],
    [2026, 'Yariel Rodriguez', 'SP', 29, 'Active', 5000000],
]


def load_clean_data():
    print("=== 🚀 Starting Clean Salary Data Load ===")
    try:
        engine = create_engine(DB_URL)

        # 1. Create DataFrame
        df = pd.DataFrame(
            RAW_DATA,
            columns=[
                'season', 'player_name', 'position',
                'age', 'status', 'cash_salary_usd'
            ]
        )

        # 2. Add Missing Columns
        df['luxury_tax_usd'] = df['cash_salary_usd']
        df['luxury_tax_pct'] = 0.0
        df['contract_type'] = 'Standard'
        df['free_agent_year'] = 'Unknown'

        # 3. Load to DB (Full Replace)
        print("   💾 Uploading to 'stg_spotrac_bluejays_salary_raw'...")
        # [FIX] using if_exists='replace'
        # This Drops the old table (missing 'status') and creates a new one.
        df.to_sql(
            'stg_spotrac_bluejays_salary_raw',
            con=engine,
            if_exists='replace',
            index=False
        )

        print(f"   ✅ Success! Loaded {len(df)} rows.")

    except Exception as e:
        print(f"❌ Load Failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    load_clean_data()

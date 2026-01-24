import sys
import os
from pathlib import Path
from sqlalchemy import create_engine, text

# Configuration
DB_URL = os.getenv(
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
    "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
)
BASE_DIR = Path(__file__).parent
SQL_DIR = BASE_DIR / "sql"


def run_dq_checks():
    """
    Executes SQL files in src/dq/sql/.
    If any query returns a count > 0, the check fails.
    """
    print("=== 🛡️ Starting DQ Gate: Bad Data Detection ===")
    engine = create_engine(DB_URL)

    # Map descriptive names to SQL filenames
    checks = {
        "Null Check": "check_nulls.sql",
        "Duplicate Check": "check_duplicates.sql",
        "Type Format Check": "check_invalid_types.sql"
    }

    failed_count = 0

    with engine.connect() as conn:
        for check_name, filename in checks.items():
            file_path = SQL_DIR / filename
            if not file_path.exists():
                print(f"⚠️ Warning: {filename} not found at {file_path}")
                continue

            with open(file_path, 'r') as f:
                query = f.read()

            try:
                # Execute query. The result should be the count of bad rows.
                # using .scalar() to get the single integer value.
                result = conn.execute(text(query)).scalar()

                # Handle None results (empty tables) safely
                if result is None:
                    result = 0
                else:
                    result = int(result)

                if result == 0:
                    print(f"✅ [PASS] {check_name}: 0 bad rows found.")
                else:
                    print(f"❌ [FAIL] {check_name}: Found {result} bad rows!")
                    failed_count += 1

            except Exception as e:
                print(f"❌ [ERROR] Could not execute {check_name}: {e}")
                failed_count += 1

    print("============================================")

    if failed_count > 0:
        print(f"🚨 DQ Gate FAILED. Total failures: {failed_count}")
        sys.exit(1)  # Signal Airflow Task Failure
    else:
        print("🎉 DQ Gate PASSED. Data is clean.")
        sys.exit(0)


if __name__ == "__main__":
    run_dq_checks()

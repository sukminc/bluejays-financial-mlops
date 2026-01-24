import sys
import os
import argparse
from pathlib import Path
from sqlalchemy import create_engine, text

# Configuration
DB_URL = os.getenv(
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
    "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
)
BASE_DIR = Path(__file__).parent
SQL_DIR = BASE_DIR / "sql"
DEFAULT_TABLE = "stg_spotrac_bluejays_salary_raw"


def run_dq_checks(target_table, expect_fail=False):
    """
    Executes SQL checks against a specific target_table.
    If expect_fail is True, the script succeeds ONLY if bad rows are found.
    """
    print(f"=== 🛡️ Starting DQ Gate on table: {target_table} ===")
    if expect_fail:
        print("🧪 MODE: Negative Test (Expecting Failures)")

    engine = create_engine(DB_URL)
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
                print(f"⚠️ Warning: {filename} not found")
                continue

            with open(file_path, 'r') as f:
                raw_query = f.read()

            # DYNAMIC REPLACEMENT:
            # Swap the production table name for the test table
            query = raw_query.replace(DEFAULT_TABLE, target_table)

            try:
                result = conn.execute(text(query)).scalar() or 0
                result = int(result)

                if result == 0:
                    print(f"✅ [PASS] {check_name}: 0 bad rows.")
                else:
                    print(f"❌ [FAIL] {check_name}:Found {result} bad rows")
                    failed_count += 1

            except Exception as e:
                print(f"❌ [ERROR] {check_name}: {e}")
                failed_count += 1

    print("============================================")

    if expect_fail:
        # NEGATIVE TEST: Success means we caught errors
        if failed_count > 0:
            print("🎉 Regression Test Passed: Bad data was correctly caught.")
            sys.exit(0)
        else:
            print("🚨 Regression Test FAILED: Bad data slipped through!")
            sys.exit(1)
    else:
        # POSITIVE TEST: Success means no errors
        if failed_count > 0:
            print(f"🚨 DQ Gate FAILED. Total failures: {failed_count}")
            sys.exit(1)
        else:
            print("🎉 DQ Gate PASSED. Data is clean.")
            sys.exit(0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", default=DEFAULT_TABLE)
    parser.add_argument(
        "--expect-fail",
        action="store_true",
        help="Pass if errors are found (for testing bad data)"
    )
    args = parser.parse_args()

    run_dq_checks(args.table, args.expect_fail)

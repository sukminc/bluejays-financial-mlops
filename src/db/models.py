"""Load manual Spotrac Blue Jays salary CSV into a raw staging table.

Goal (portfolio-friendly):
- Keep the pipeline simple and reliable.
- Treat the CSV as the source of truth that you maintain manually.
- Load the CSV into Postgres as-is (TEXT columns) for later refinement into
  silver/gold layers.

This module intentionally avoids:
- name-to-player_id matching
- strict type casting
- complex exception handling

Table target (created if missing):
public.stg_spotrac_bluejays_salary_raw

Expected CSV header columns:
- season
- player_name
- position
- age
- contract_type
- luxury_tax_usd
- luxury_tax_pct
- cash_salary_usd
- free_agent_year

All columns are stored as TEXT; `raw_loaded_at` is assigned by the DB.
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

from sqlalchemy import create_engine, text

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
DEFAULT_DB_URL = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
DEFAULT_INPUT = "/opt/airflow/data/raw/manual/spotrac_bluejays_2025.csv"
TARGET_TABLE = "public.stg_spotrac_bluejays_salary_raw"

CSV_COLUMNS = (
    "season",
    "player_name",
    "position",
    "age",
    "contract_type",
    "luxury_tax_usd",
    "luxury_tax_pct",
    "cash_salary_usd",
    "free_agent_year",
)


def _ensure_table(engine) -> None:
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
      season TEXT,
      player_name TEXT,
      position TEXT,
      age TEXT,
      contract_type TEXT,
      luxury_tax_usd TEXT,
      luxury_tax_pct TEXT,
      cash_salary_usd TEXT,
      free_agent_year TEXT,
      raw_loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def _truncate_table(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {TARGET_TABLE};"))


def load_spotrac_salary_csv_raw(
    input_path: str,
    db_url: str,
    truncate: bool = False,
) -> int:
    """Load the CSV into the raw staging table.

    Returns:
        Number of rows inserted.
    """
    path = Path(input_path)
    if not path.exists():
        raise FileNotFoundError(f"Input CSV not found: {path}")

    engine = create_engine(db_url)
    _ensure_table(engine)

    if truncate:
        _truncate_table(engine)

    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError("CSV has no header row")

        missing = [c for c in CSV_COLUMNS if c not in reader.fieldnames]
        if missing:
            joined = ", ".join(missing)
            raise ValueError(f"CSV missing required columns: {joined}")

        insert_sql = text(
            f"""
            INSERT INTO {TARGET_TABLE} (
              season,
              player_name,
              position,
              age,
              contract_type,
              luxury_tax_usd,
              luxury_tax_pct,
              cash_salary_usd,
              free_agent_year
            ) VALUES (
              :season,
              :player_name,
              :position,
              :age,
              :contract_type,
              :luxury_tax_usd,
              :luxury_tax_pct,
              :cash_salary_usd,
              :free_agent_year
            );
            """
        )

        rows = []
        for row in reader:
            payload = {k: (row.get(k) or "").strip() for k in CSV_COLUMNS}
            # Treat empty strings as NULL for free_agent_year.
            if payload["free_agent_year"] == "":
                payload["free_agent_year"] = None
            rows.append(payload)

    inserted = 0
    if rows:
        with engine.begin() as conn:
            conn.execute(insert_sql, rows)
        inserted = len(rows)

    return inserted


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Load manual salary CSV into raw staging table (TEXT)."
    )
    parser.add_argument(
        "--input",
        default=DEFAULT_INPUT,
        help=f"Input CSV file path (default: {DEFAULT_INPUT})",
    )
    parser.add_argument(
        "--db-url",
        default=DEFAULT_DB_URL,
        help="Database URL for Postgres in Docker .",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Truncate the staging table before inserting.",
    )

    args = parser.parse_args()

    try:
        inserted = load_spotrac_salary_csv_raw(
            input_path=args.input,
            db_url=args.db_url,
            truncate=args.truncate,
        )
    except Exception as exc:
        print(f"spotrac CSV raw load failed: {exc}")
        sys.exit(1)

    print(
        "spotrac CSV raw load complete: "
        f"table={TARGET_TABLE} inserted={inserted}"
    )

    if inserted == 0:
        print("No rows inserted; failing task.")
        sys.exit(1)


if __name__ == "__main__":
    main()

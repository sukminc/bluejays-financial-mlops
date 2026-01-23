from __future__ import annotations

import csv
from pathlib import Path
from decimal import Decimal
from typing import Dict

from src.db.models import FactSalary
from src.db.session import get_session


def load_salary_from_csv(path: str | Path) -> Dict[str, int]:
    input_path = Path(path)
    if not input_path.exists():
        raise FileNotFoundError(f"CSV not found: {input_path}")

    loaded = 0

    with get_session() as session:
        with input_path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                record = FactSalary(
                    season=int(row["season"]),
                    player_name=row["player_name"],
                    position=row["position"],
                    age=int(row["age"]),
                    contract_type=row["contract_type"],
                    luxury_tax_usd=Decimal(row["luxury_tax_usd"]),
                    luxury_tax_pct=Decimal(row["luxury_tax_pct"]),
                    cash_salary_usd=Decimal(row["cash_salary_usd"]),
                    free_agent_year=(
                        int(row["free_agent_year"])
                        if row["free_agent_year"]
                        else None
                    ),
                )
                session.merge(record)
                loaded += 1

        session.commit()

    return {"loaded_rows": loaded}


def main() -> None:
    import argparse
    import sys

    parser = argparse.ArgumentParser(
        description="One-time load Spotrac CSV into fact_salary"
    )
    parser.add_argument(
        "--input",
        default="/opt/airflow/data/raw/manual/spotrac_bluejays_2025.csv",
    )
    args = parser.parse_args()

    result = load_salary_from_csv(args.input)
    print(f"fact_salary CSV load complete: {result}")

    if result["loaded_rows"] == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()

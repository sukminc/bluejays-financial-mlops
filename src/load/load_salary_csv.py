from __future__ import annotations

import argparse
import csv
import os
import sys

from sqlalchemy import text
from src.db.session import get_session


def load_salary_csv(input_path: str, truncate: bool = False) -> dict:
    if not os.path.isfile(input_path):
        print(f"Input file not found: {input_path}")
        return {"loaded": 0, "bad": 0}

    loaded = 0
    bad = 0

    with get_session() as session:
        if truncate:
            session.execute(
                text("TRUNCATE TABLE public.stg_spotrac_bluejays_salary_raw;")
            )
            session.commit()

        with open(input_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    session.execute(
                        text(
                            """
                            INSERT INTO public.stg_spotrac_bluejays_salary_raw (
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
                            )
                            """
                        ),
                        row,
                    )
                    loaded += 1
                except Exception as e:
                    print("BAD ROW:", row, e)
                    bad += 1

        session.commit()

    return {"loaded": loaded, "bad": bad}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="RAW load Spotrac Blue Jays salary CSV"
    )
    parser.add_argument(
        "--input",
        required=True,
        help="CSV file path",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Truncate raw table before load",
    )
    args = parser.parse_args()

    result = load_salary_csv(args.input, args.truncate)

    print(result)

    if result["loaded"] == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()

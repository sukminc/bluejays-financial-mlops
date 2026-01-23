from __future__ import annotations

import argparse
import csv
import difflib
import os
import re
import sys
import unicodedata
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from src.db.models import DimPlayer, FactSalary
from src.db.session import get_session


def _normalize_name(value: str) -> str:
    """Normalize player names for robust matching.

    - Lowercase
    - Strip accents (e.g., García -> garcia)
    - Replace punctuation with spaces
    - Collapse whitespace
    """
    s = value.strip().lower()

    # Strip accents
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))

    # Keep alnum + whitespace only; turn punctuation into spaces
    s = re.sub(r"[^a-z0-9\s]+", " ", s)

    # Collapse whitespace
    return " ".join(s.split())


def _to_int_dollars(value: Any) -> int | None:
    """
    Convert currency-like values to int dollars (bigint-compatible).
    Accepts: 35714286, "35,714,286", "$35,714,286", "35714286.00"
    """
    if value is None:
        return None
    s = str(value).strip()
    if s == "":
        return None

    s = s.replace("$", "").replace(",", "")
    try:
        d = Decimal(s)
    except (InvalidOperation, ValueError):
        return None

    # Store as whole dollars (int). (If cents appear, they are truncated.)
    return int(d)


def _parse_snapshot_date(value: str) -> date:
    return date.fromisoformat(value)


def _build_player_name_index(session) -> dict[str, int]:
    """Build an in-memory normalized-name -> player_id index.

    This avoids SQL-side accent/collation issues (e.g., García vs Garcia).
    """
    rows = session.query(
        DimPlayer.player_id,
        DimPlayer.name_display_first_last
        ).all()
    index: dict[str, int] = {}
    for pid, name in rows:
        if not name:
            continue
        index[_normalize_name(str(name))] = int(pid)
    return index


def _resolve_player_id(
    name_index: dict[str, int],
    player_name: str,
) -> int | None:
    """Resolve player_id using the in-memory normalized index."""
    if not player_name:
        return None

    norm = _normalize_name(player_name)
    hit = name_index.get(norm)
    if hit is not None:
        return hit

    # Try a close match for small spelling differences (e.g., Louie vs Louis)
    candidates = difflib.get_close_matches(
        norm,
        name_index.keys(),
        n=1,
        cutoff=0.92
        )
    if candidates:
        return name_index[candidates[0]]

    # Fallback: token containment (safe, but last resort)
    for k, pid in name_index.items():
        if norm in k or k in norm:
            return pid

    return None


def load_salary_csv(
    input_path: str | Path,
    snapshot_date: date,
    truncate: bool = False,
) -> dict[str, int]:
    """
    Load manual salary CSV into current DB schema: public.fact_salary

    Required CSV columns:
      - season, player_name, luxury_tax_usd, cash_salary_usd

    Mapping:
      - cash_salary_usd  -> FactSalary.base_salary
      - luxury_tax_usd   -> FactSalary.luxury_tax_salary
    """
    path = Path(input_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV not found: {path}")

    required = {"season", "player_name", "luxury_tax_usd", "cash_salary_usd"}

    loaded = 0
    skipped_no_match = 0
    skipped_bad_row = 0

    with get_session() as session:
        name_index = _build_player_name_index(session)

        if truncate:
            # DB에는 team_abbr/source가 없으므로 snapshot_date 기준으로만 지움
            session.query(FactSalary).filter(
                FactSalary.snapshot_date == snapshot_date
            ).delete(synchronize_session=False)
            session.commit()

        with path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            headers = set(reader.fieldnames or [])
            missing = required - headers
            if missing:
                joined = ", ".join(sorted(missing))
                raise ValueError(f"CSV missing required columns: {joined}")

            for row in reader:
                try:
                    season_raw = (row.get("season") or "").strip()
                    player_name = (row.get("player_name") or "").strip()

                    if not season_raw or not player_name:
                        skipped_bad_row += 1
                        continue

                    season = int(season_raw)
                    base_salary = _to_int_dollars(row.get("cash_salary_usd"))
                    luxury_tax_salary = _to_int_dollars(
                        row.get("luxury_tax_usd")
                    )

                    if base_salary is None or luxury_tax_salary is None:
                        skipped_bad_row += 1
                        continue

                    player_id = _resolve_player_id(name_index, player_name)
                    if player_id is None:
                        skipped_no_match += 1
                        continue

                    rec = FactSalary(
                        player_id=player_id,
                        season=season,
                        base_salary=base_salary,
                        luxury_tax_salary=luxury_tax_salary,
                        snapshot_date=snapshot_date,
                    )
                    session.add(rec)
                    loaded += 1

                except Exception:
                    skipped_bad_row += 1
                    continue

        session.commit()

    return {
        "loaded": loaded,
        "skipped_no_match": skipped_no_match,
        "skipped_bad_row": skipped_bad_row,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="One-time load: manual salary CSV -> public.fact_salary",
    )
    parser.add_argument(
        "--input",
        default=os.environ.get(
            "SALARY_CSV_INPUT",
            "/opt/airflow/data/raw/manual/spotrac_bluejays_2025.csv",
        ),
        help=(
            "CSV path (default: "
            "/opt/airflow/data/raw/manual/spotrac_bluejays_2025.csv)"
        ),
    )
    parser.add_argument(
        "--snapshot-date",
        default=os.environ.get("SNAPSHOT_DATE", date.today().isoformat()),
        help="YYYY-MM-DD snapshot date",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Delete rows for the same snapshot_date before load",
    )

    args = parser.parse_args()

    snapshot = _parse_snapshot_date(args.snapshot_date)
    summary = load_salary_csv(
        input_path=args.input,
        snapshot_date=snapshot,
        truncate=args.truncate,
    )

    print("fact_salary CSV load complete:")
    print(
        f"loaded={summary['loaded']} "
        f"skipped_no_match={summary['skipped_no_match']} "
        f"skipped_bad_row={summary['skipped_bad_row']}"
    )

    if summary["loaded"] == 0:
        print("No rows loaded; failing task.")
        sys.exit(1)


if __name__ == "__main__":
    main()

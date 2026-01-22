from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

from sqlalchemy import create_engine, text

DEFAULT_DB_URL = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
DB_URL = os.getenv("DATABASE_URL", DEFAULT_DB_URL)

SQL_DIR = Path(__file__).resolve().parent / "sql"


@dataclass(frozen=True)
class Thresholds:
    """
    Tunable thresholds for the DQ gate.

    - min_salary_stats_join_pct: minimum % of salary rows that successfully
      join to stats
    - max_pk_duplicate_rows: maximum allowed duplicate PK rows (should be 0)
    - max_null_critical_rows: maximum allowed null violations (should be 0)
    - max_freshness_hours: max age (hours) for latest load time/window
    """
    min_salary_stats_join_pct: float = float(
        os.getenv("DQ_MIN_JOIN_PCT", "90")
    )
    max_pk_duplicate_rows: int = int(
        os.getenv("DQ_MAX_PK_DUP", "0")
    )
    max_null_critical_rows: int = int(
        os.getenv("DQ_MAX_NULLS", "0")
    )
    max_freshness_hours: int = int(
        os.getenv("DQ_MAX_FRESHNESS_HOURS", "48")
    )


def _read_sql_files() -> List[Path]:
    if not SQL_DIR.exists():
        raise RuntimeError(f"SQL directory not found: {SQL_DIR}")
    files = sorted(SQL_DIR.glob("*.sql"))
    if not files:
        raise RuntimeError(f"No SQL files found under: {SQL_DIR}")
    return files


def _row_to_dict(row: Any, keys: List[str]) -> Dict[str, Any]:
    return {k: row[i] for i, k in enumerate(keys)}


def _execute_sql(
    engine,
    sql_path: Path,
) -> Tuple[List[str], List[Dict[str, Any]]]:
    sql_text = sql_path.read_text(encoding="utf-8").strip()
    if not sql_text:
        return [], []

    with engine.begin() as conn:
        result = conn.execute(text(sql_text))
        keys = list(result.keys()) if result.returns_rows else []
        if result.returns_rows:
            rows = [dict(r) for r in result.mappings().all()]
        else:
            rows = []
        return keys, rows


def _evaluate(
    sql_name: str,
    rows: List[Dict[str, Any]],
    t: Thresholds,
) -> Tuple[bool, str]:
    """
    Best-effort evaluation based on expected query outputs.

    Supported conventions:
    - Query returns a single row with a numeric metric column:
        * salary_stats_join_pct
        * pk_duplicate_rows / dup_rows / duplicate_rows / duplicate_count
        * null_critical_rows / null_rows / null_count
        * freshness_hours / max_age_hours
    - OR returns a single row with boolean column:
        * pass / ok
    """
    if not rows:
        return False, (
            f"{sql_name}: returned 0 rows (treat as FAIL for safety)"
        )

    first = rows[0]

    # 1) Explicit boolean
    for k in ("pass", "ok"):
        if k in first:
            passed = bool(first[k])
            return passed, f"{sql_name}: {k}={passed}"

    # 2) Known metrics
    if "salary_stats_join_pct" in first:
        val = float(first["salary_stats_join_pct"])
        passed = val >= t.min_salary_stats_join_pct
        msg = (
            f"{sql_name}: salary_stats_join_pct={val:.2f} "
            f"(min={t.min_salary_stats_join_pct:.2f})"
        )
        return passed, msg

    for k in (
        "pk_duplicate_rows",
        "dup_rows",
        "duplicate_rows",
        "duplicate_count",
    ):
        if k in first:
            val = int(first[k])
            passed = val <= t.max_pk_duplicate_rows
            msg = (
                f"{sql_name}: {k}={val} "
                f"(max={t.max_pk_duplicate_rows})"
            )
            return passed, msg

    for k in ("null_critical_rows", "null_rows", "null_count"):
        if k in first:
            val = int(first[k])
            passed = val <= t.max_null_critical_rows
            msg = (
                f"{sql_name}: {k}={val} "
                f"(max={t.max_null_critical_rows})"
            )
            return passed, msg

    for k in ("freshness_hours", "max_age_hours"):
        if k in first:
            val = float(first[k])
            passed = val <= float(t.max_freshness_hours)
            msg = (
                f"{sql_name}: {k}={val:.2f} "
                f"(max={t.max_freshness_hours})"
            )
            return passed, msg

    # 3) Fallback: if a single numeric column exists, do not guess thresholds.
    # Mark as PASS but print the metric (to avoid blocking unexpectedly).
    numeric_cols = []
    for k, v in first.items():
        if isinstance(v, (int, float)) and v is not None:
            numeric_cols.append(k)

    if len(numeric_cols) == 1:
        k = numeric_cols[0]
        return True, f"{sql_name}: metric {k}={first[k]} (no threshold bound)"

    return False, (
        f"{sql_name}: unsupported output shape -> keys={list(first.keys())}"
    )


def run_dq_checks() -> None:
    thresholds = Thresholds()
    engine = create_engine(DB_URL, pool_pre_ping=True)

    sql_files = _read_sql_files()

    failures: List[str] = []
    print("=== DQ Gate: starting ===")
    print(f"DB_URL={DB_URL}")
    print(f"SQL_DIR={SQL_DIR}")
    print(
        "Thresholds: "
        f"min_join_pct={thresholds.min_salary_stats_join_pct}, "
        f"max_pk_dup={thresholds.max_pk_duplicate_rows}, "
        f"max_nulls={thresholds.max_null_critical_rows}, "
        f"max_freshness_h={thresholds.max_freshness_hours}"
    )

    for sql_path in sql_files:
        keys, rows = _execute_sql(engine, sql_path)
        passed, msg = _evaluate(sql_path.name, rows, thresholds)

        status = "PASS" if passed else "FAIL"
        print(f"[{status}] {msg}")

        if not passed:
            failures.append(msg)

    if failures:
        print("=== DQ Gate: FAILED ===")
        for f in failures:
            print(f"- {f}")
        sys.exit(1)

    print("=== DQ Gate: PASSED ===")
    sys.exit(0)


if __name__ == "__main__":
    run_dq_checks()

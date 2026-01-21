"""Data quality checks for the Blue Jays financial data platform.

This module provides a small, practical DQ gate for the pipeline:
- Join coverage: salary rows that can join to player dimension and stats.
- Null checks: required keys must not be null.
- Duplicate checks: detect duplicates on natural PKs.
- Drift checks: compare row counts between consecutive snapshots.

The functions return a structured summary dict and can optionally raise
an exception when thresholds are violated (fail-fast behavior for Airflow).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, Optional, Tuple

from sqlalchemy import and_, func

from src.db.models import DimPlayer, FactPlayerStats, FactSalary
from src.db.session import get_session


@dataclass(frozen=True)
class DQThresholds:
    """Thresholds for failing the pipeline."""

    min_salary_dim_join_pct: float = 98.0
    min_salary_stats_join_pct: float = 95.0
    max_salary_row_drift_pct: float = 25.0
    max_stats_row_drift_pct: float = 25.0


def _as_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


def _pct(numer: int, denom: int) -> float:
    if denom <= 0:
        return 0.0
    return round(100.0 * float(numer) / float(denom), 2)


def _find_prev_snapshot(
    snapshot_date: date,
    table_model: Any,
    date_col: Any,
) -> Optional[date]:
    """Find the most recent snapshot date prior to the given date."""

    with get_session() as session:
        prev = (
            session.query(func.max(date_col))
            .select_from(table_model)
            .filter(date_col < snapshot_date)
            .scalar()
        )

    return prev


def _row_count_for_snapshot(
    table_model: Any,
    snapshot_date: date,
    date_col: Any,
) -> int:
    with get_session() as session:
        return (
            session.query(func.count())
            .select_from(table_model)
            .filter(date_col == snapshot_date)
            .scalar()
            or 0
        )


def _duplicate_count(
    table_model: Any,
    snapshot_date: date,
    date_col: Any,
    key_cols: Tuple[Any, ...],
) -> int:
    """Count duplicates for the given key columns within a snapshot."""

    group_cols = list(key_cols)

    with get_session() as session:
        subq = (
            session.query(*group_cols, func.count().label("cnt"))
            .select_from(table_model)
            .filter(date_col == snapshot_date)
            .group_by(*group_cols)
            .subquery()
        )
        dup_groups = (
            session.query(func.count())
            .select_from(subq)
            .filter(subq.c.cnt > 1)
            .scalar()
            or 0
        )

    return int(dup_groups)


def _null_count(
    table_model: Any,
    snapshot_date: date,
    date_col: Any,
    col: Any,
) -> int:
    with get_session() as session:
        return (
            session.query(func.count())
            .select_from(table_model)
            .filter(and_(date_col == snapshot_date, col.is_(None)))
            .scalar()
            or 0
        )


def _salary_join_coverage(
    snapshot_date: date,
    season: Optional[int] = None,
) -> Dict[str, Any]:
    """Compute salary join coverage against dim and stats."""

    with get_session() as session:
        salary_q = session.query(FactSalary).filter(
            FactSalary.snapshot_date == snapshot_date,
        )
        if season is not None:
            salary_q = salary_q.filter(FactSalary.season == season)

        total_salary = salary_q.count()

        # Salary -> dim_players join
        salary_dim = (
            session.query(func.count())
            .select_from(FactSalary)
            .join(DimPlayer, DimPlayer.player_id == FactSalary.player_id)
            .filter(FactSalary.snapshot_date == snapshot_date)
        )
        if season is not None:
            salary_dim = salary_dim.filter(FactSalary.season == season)

        salary_dim_count = salary_dim.scalar() or 0

        # Salary -> stats join (same season, same snapshot)
        salary_stats = (
            session.query(func.count())
            .select_from(FactSalary)
            .join(
                FactPlayerStats,
                and_(
                    FactPlayerStats.player_id == FactSalary.player_id,
                    FactPlayerStats.season == FactSalary.season,
                    FactPlayerStats.snapshot_date == FactSalary.snapshot_date,
                ),
            )
            .filter(FactSalary.snapshot_date == snapshot_date)
        )
        if season is not None:
            salary_stats = salary_stats.filter(FactSalary.season == season)

        salary_stats_count = salary_stats.scalar() or 0

    return {
        "total_salary_rows": int(total_salary),
        "salary_dim_join_rows": int(salary_dim_count),
        "salary_dim_join_pct": _pct(int(salary_dim_count), int(total_salary)),
        "salary_stats_join_rows": int(salary_stats_count),
        "salary_stats_join_pct": _pct(
            int(salary_stats_count),
            int(total_salary),
        ),
    }


def _row_count_drift(
    table_model: Any,
    snapshot_date: date,
    date_col: Any,
) -> Dict[str, Any]:
    """Compute row count drift vs the previous snapshot."""

    current = _row_count_for_snapshot(table_model, snapshot_date, date_col)
    prev_date = _find_prev_snapshot(snapshot_date, table_model, date_col)

    if prev_date is None:
        return {
            "current": current,
            "prev_snapshot_date": None,
            "prev": None,
            "drift_pct": None,
        }

    prev = _row_count_for_snapshot(table_model, prev_date, date_col)
    if prev == 0:
        drift_pct = None
    else:
        drift_pct = round(100.0 * abs(current - prev) / float(prev), 2)

    return {
        "current": current,
        "prev_snapshot_date": prev_date.isoformat(),
        "prev": prev,
        "drift_pct": drift_pct,
    }


def run_dq_checks(
    snapshot_date: str | date,
    season: Optional[int] = None,
    thresholds: Optional[DQThresholds] = None,
    fail_on_error: bool = True,
) -> Dict[str, Any]:
    """Run DQ checks for a given snapshot date.

    Args:
        snapshot_date: Snapshot date (YYYY-MM-DD) or date.
        season: Optional season filter for join coverage.
        thresholds: Thresholds for failing the pipeline.
        fail_on_error: If True, raise ValueError on threshold violation.

    Returns:
        A dict containing check results and a pass/fail indicator.
    """

    thresholds = thresholds or DQThresholds()
    snap = _as_date(snapshot_date)

    coverage = _salary_join_coverage(snap, season=season)

    salary_drift = _row_count_drift(FactSalary, snap, FactSalary.snapshot_date)
    stats_drift = _row_count_drift(
        FactPlayerStats,
        snap,
        FactPlayerStats.snapshot_date,
    )

    dup_salary = _duplicate_count(
        FactSalary,
        snap,
        FactSalary.snapshot_date,
        (FactSalary.season, FactSalary.player_id),
    )
    dup_stats = _duplicate_count(
        FactPlayerStats,
        snap,
        FactPlayerStats.snapshot_date,
        (FactPlayerStats.season, FactPlayerStats.player_id),
    )

    salary_null_player = _null_count(
        FactSalary,
        snap,
        FactSalary.snapshot_date,
        FactSalary.player_id,
    )
    stats_null_player = _null_count(
        FactPlayerStats,
        snap,
        FactPlayerStats.snapshot_date,
        FactPlayerStats.player_id,
    )

    issues = []

    if coverage["salary_dim_join_pct"] < thresholds.min_salary_dim_join_pct:
        issues.append(
            "Salary->Dim join coverage below threshold: "
            f"{coverage['salary_dim_join_pct']}%"
        )

    if (
        coverage["salary_stats_join_pct"]
        < thresholds.min_salary_stats_join_pct
    ):
        issues.append(
            "Salary->Stats join coverage below threshold: "
            f"{coverage['salary_stats_join_pct']}%"
        )

    salary_drift_pct = salary_drift.get("drift_pct")
    if salary_drift_pct is not None:
        if salary_drift_pct > thresholds.max_salary_row_drift_pct:
            issues.append(
                "Salary row count drift too high: "
                f"{salary_drift_pct}%"
            )

    stats_drift_pct = stats_drift.get("drift_pct")
    if stats_drift_pct is not None:
        if stats_drift_pct > thresholds.max_stats_row_drift_pct:
            issues.append(
                "Stats row count drift too high: "
                f"{stats_drift_pct}%"
            )

    if dup_salary > 0:
        issues.append(f"Duplicate salary groups detected: {dup_salary}")

    if dup_stats > 0:
        issues.append(f"Duplicate stats groups detected: {dup_stats}")

    if salary_null_player > 0:
        issues.append(
            "Salary rows with null player_id: "
            f"{salary_null_player}"
        )

    if stats_null_player > 0:
        issues.append(
            "Stats rows with null player_id: "
            f"{stats_null_player}"
        )

    passed = len(issues) == 0

    result = {
        "snapshot_date": snap.isoformat(),
        "season": season,
        "passed": passed,
        "coverage": coverage,
        "row_count_drift": {
            "salary": salary_drift,
            "stats": stats_drift,
        },
        "duplicates": {
            "salary_duplicate_groups": dup_salary,
            "stats_duplicate_groups": dup_stats,
        },
        "nulls": {
            "salary_null_player_id": salary_null_player,
            "stats_null_player_id": stats_null_player,
        },
        "issues": issues,
    }

    if fail_on_error and not passed:
        joined = "; ".join(issues)
        raise ValueError(
            "DQ checks failed: "
            f"{joined}"
        )

    return result

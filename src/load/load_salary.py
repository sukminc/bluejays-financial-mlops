

"""Load Spotrac salary data into fact_salary.

This module loads Spotrac payroll snapshots and writes them into the
`fact_salary` table using canonical `player_id` resolved via the
`bridge_spotrac_player_map` table.

Design assumptions:
- Spotrac data is name-based and must be mapped before salary load.
- Salary rows without a resolved `player_id` are skipped and reported.
- Upsert is performed via ORM merge for deterministic re-runs.
"""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.db.models import BridgeSpotracPlayerMap, FactSalary
from src.db.session import get_session
from src.load.map_spotrac import _spotrac_player_key


class SalaryRow:
    """Normalized salary row for loading into fact_salary."""

    def __init__(
        self,
        season: int,
        snapshot_date: str,
        spotrac_name_raw: str,
        salary_usd: Decimal,
        team_abbr: Optional[str] = None,
        position: Optional[str] = None,
    ) -> None:
        self.season = season
        self.snapshot_date = snapshot_date
        self.spotrac_name_raw = spotrac_name_raw
        self.salary_usd = salary_usd
        self.team_abbr = team_abbr
        self.position = position


def _to_decimal(value: Any) -> Optional[Decimal]:
    """Convert numeric values into Decimal."""
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _parse_salary_row(obj: Dict[str, Any]) -> SalaryRow:
    """Parse a Spotrac salary dict into a SalaryRow.

    Required keys (best effort):
    - season
    - name / player_name / spotrac_name
    - salary or salary_usd
    - snapshot_date
    """

    name = (
        obj.get("name")
        or obj.get("player_name")
        or obj.get("spotrac_name")
    )
    if not name:
        raise ValueError("Salary row missing player name")

    season = int(obj.get("season"))
    snapshot_date = str(obj.get("snapshot_date"))

    salary_val = obj.get("salary_usd") or obj.get("salary")
    salary = _to_decimal(salary_val)
    if salary is None:
        raise ValueError("Salary row missing valid salary value")

    return SalaryRow(
        season=season,
        snapshot_date=snapshot_date,
        spotrac_name_raw=str(name).strip(),
        salary_usd=salary,
        team_abbr=obj.get("team_abbr"),
        position=obj.get("position") or obj.get("pos"),
    )


def load_salary_from_json(path: str | Path) -> Dict[str, Any]:
    """Load Spotrac salary JSON into fact_salary.

    Returns:
        Summary dict with loaded and skipped row counts.
    """

    input_path = Path(path)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    payload = json.loads(input_path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("Expected a JSON array of salary rows")

    rows: List[SalaryRow] = []
    for obj in payload:
        if not isinstance(obj, dict):
            continue
        try:
            rows.append(_parse_salary_row(obj))
        except ValueError:
            continue

    loaded = 0
    skipped = 0

    with get_session() as session:
        for r in rows:
            spotrac_key = _spotrac_player_key(
                r.spotrac_name_raw,
                r.team_abbr,
                r.position,
            )

            mapping = (
                session.query(BridgeSpotracPlayerMap)
                .filter(
                    BridgeSpotracPlayerMap.spotrac_player_key
                    == spotrac_key
                )
                .one_or_none()
            )

            if mapping is None:
                skipped += 1
                continue

            record = FactSalary(
                season=r.season,
                player_id=mapping.player_id,
                snapshot_date=r.snapshot_date,
                salary_usd=r.salary_usd,
                team_abbr=r.team_abbr,
            )

            session.merge(record)
            loaded += 1

    return {
        "total_rows": len(rows),
        "loaded_rows": loaded,
        "skipped_rows": skipped,
    }

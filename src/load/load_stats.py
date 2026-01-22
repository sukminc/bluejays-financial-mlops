

"""Load player stats into fact_player_stats.

This module loads normalized player season stats
(from MLB Stats API extractor)into Postgres.
Data is snapshot-based to support reproducibility and drift checks.

Expected input JSON:
- A list of objects.
- Required keys: season, player_id, snapshot_date
- Optional keys: team_abbr, games, pa, ops, ip, era, war

Notes:
- `player_id` is the canonical MLBAM id.
- Upsert is performed via ORM merge for simplicity in this project phase.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

from src.db.models import FactPlayerStats
from src.db.session import get_session


@dataclass(frozen=True)
class StatsRecord:
    """Minimal stats record for fact_player_stats upsert."""

    season: int
    player_id: int
    snapshot_date: date

    team_abbr: str | None = None

    games: int | None = None

    pa: int | None = None
    ops: Decimal | None = None

    ip: Decimal | None = None
    era: Decimal | None = None

    war: Decimal | None = None


def _parse_date(value: Any) -> date:
    """Parse snapshot_date from YYYY-MM-DD string."""
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value)
    raise ValueError("snapshot_date must be a YYYY-MM-DD string")


def _to_int(value: Any) -> int | None:
    """Convert value to int when possible."""
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_decimal(value: Any) -> Decimal | None:
    """Convert value to Decimal when possible."""
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (TypeError, ValueError):
        return None


def _validate_required(obj: dict[str, Any]) -> None:
    """Validate required keys for stats records."""
    required = ("season", "player_id", "snapshot_date")
    missing = [k for k in required if k not in obj]
    if missing:
        joined = ", ".join(missing)
        raise ValueError(f"Missing required field(s): {joined}")


def _to_stats_record(obj: dict[str, Any]) -> StatsRecord:
    """Convert a dict into StatsRecord with basic validation."""
    _validate_required(obj)

    return StatsRecord(
        season=int(obj["season"]),
        player_id=int(obj["player_id"]),
        snapshot_date=_parse_date(obj["snapshot_date"]),
        team_abbr=obj.get("team_abbr"),
        games=_to_int(obj.get("games")),
        pa=_to_int(obj.get("pa")),
        ops=_to_decimal(obj.get("ops")),
        ip=_to_decimal(obj.get("ip")),
        era=_to_decimal(obj.get("era")),
        war=_to_decimal(obj.get("war")),
    )


def load_stats_from_json(path: str | Path) -> int:
    """Load player stats from a JSON file and upsert into fact_player_stats.

    Returns:
        Number of processed records.
    """
    input_path = Path(path)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    payload = json.loads(input_path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("Expected a JSON array of stats objects")

    records = (_to_stats_record(row) for row in payload)
    count = 0

    with get_session() as session:
        for rec in records:
            row = FactPlayerStats(
                season=rec.season,
                player_id=rec.player_id,
                snapshot_date=rec.snapshot_date,
                team_abbr=rec.team_abbr,
                games=rec.games,
                pa=rec.pa,
                ops=rec.ops,
                ip=rec.ip,
                era=rec.era,
                war=rec.war,
            )
            session.merge(row)
            count += 1
        session.commit()

    return count


def main() -> None:
    """CLI entrypoint for Airflow BashOperator."""
    import argparse
    import os
    import sys

    parser = argparse.ArgumentParser(
        description="Load player stats JSON into Postgres (fact_player_stats)."
    )
    parser.add_argument(
        "--input",
        default=os.environ.get(
            "STATS_INPUT",
            "/opt/airflow/data/stats/player_stats.json",
        ),
        help=(
            "Path to stats JSON file. "
            "Can also be set via STATS_INPUT env var. "
            "Default: /opt/airflow/data/stats/player_stats.json"
        ),
    )
    args = parser.parse_args()

    processed = load_stats_from_json(args.input)
    print(
        f"fact_player_stats load complete. "
        f"processed={processed}"
    )

    # Fail fast if nothing was loaded; avoids 'green but empty' pipelines.
    if processed == 0:
        print("No stats records processed; failing task.")
        sys.exit(1)


if __name__ == "__main__":
    main()

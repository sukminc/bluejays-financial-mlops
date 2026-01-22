"""Extract player season stats from MLB Stats API and write to JSON.

This extractor is the upstream producer for `src.load.load_stats`.

Output:
- Writes a JSON array to `/opt/airflow/data/stats/player_stats.json`
by default.
- Each element is a normalized record with required keys:
  - season, player_id, snapshot_date
- Optional keys (when available): team_abbr, games, pa, ops, ip, era,
  war

Notes:
- MLB Stats API does not provide Fangraphs-style WAR directly.
  For now we emit `war` as null unless a reliable source is added.
- This is intentionally "baseline" (BashOperator-friendly):
  reproducible via CLI inside the Airflow worker container.

Example:
  PYTHONPATH=/opt/airflow python -m src.extract.player_stats_api --season 2025
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any

import requests

from src.db.models import DimPlayer
from src.db.session import get_session


MLB_API_BASE = "https://statsapi.mlb.com/api/v1"
DEFAULT_TEAM_ID = 141  # Toronto Blue Jays


@dataclass(frozen=True)
class PlayerSeasonStats:
    # Required
    season: int
    player_id: int
    snapshot_date: str

    # Optional
    team_abbr: str | None = None
    games: int | None = None
    pa: int | None = None
    ops: str | None = None
    ip: str | None = None
    era: str | None = None
    war: str | None = None


def _request_json(
    url: str,
    *,
    retries: int = 3,
    backoff_s: float = 0.5,
) -> dict[str, Any]:
    """GET JSON with minimal retry/backoff."""
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            payload = resp.json()
            if isinstance(payload, dict):
                return payload
            raise ValueError("Unexpected non-dict JSON payload")
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt < retries:
                time.sleep(backoff_s * attempt)
    raise RuntimeError(
        f"Request failed after {retries} attempts: "
        f"{url}"
    ) from last_exc


def _get_bluejays_player_ids(team_id: int) -> list[int]:
    """Read player_ids from dim_players (canonical source for pipeline)."""
    with get_session() as session:
        rows = (
            session.query(DimPlayer.player_id)
            .filter(DimPlayer.primary_position.isnot(None))
            .order_by(DimPlayer.player_id)
            .all()
        )
    return [int(r[0]) for r in rows]


def _extract_statline(payload: dict[str, Any]) -> dict[str, Any]:
    """Pick a single best statline from MLB 'hydrate=stats' response.

    We prefer a season statline (type=season). If both hitting and pitching
    exist, we pick whichever has non-empty splits.

    Returns a dict with keys: games, pa, ops, ip, era, team_abbr.
    Missing values will not be present.
    """
    stats_blocks = payload.get("people", [{}])[0].get("stats", [])
    if not isinstance(stats_blocks, list):
        return {}

    def _first_split(block: dict[str, Any]) -> dict[str, Any] | None:
        splits = block.get("splits")
        if isinstance(splits, list) and splits:
            split0 = splits[0]
            if isinstance(split0, dict):
                return split0
        return None

    split: dict[str, Any] | None = None

    # Prefer hitting then pitching, but whichever exists.
    for group in ("hitting", "pitching"):
        for block in stats_blocks:
            if block.get("group", {}).get("displayName") == group:
                split = _first_split(block)
                if split:
                    break
        if split:
            break

    if not split:
        # Fallback: any first available split.
        for block in stats_blocks:
            split = _first_split(block)
            if split:
                break

    if not split:
        return {}

    stat = split.get("stat", {})
    team = split.get("team", {})

    out: dict[str, Any] = {}

    # Team abbreviation (if present)
    team_abbr = None
    if isinstance(team, dict):
        team_abbr = team.get("abbreviation")
    if team_abbr:
        out["team_abbr"] = str(team_abbr)

    if isinstance(stat, dict):
        # Common
        if "gamesPlayed" in stat:
            out["games"] = stat.get("gamesPlayed")

        # Hitting-ish
        if "plateAppearances" in stat:
            out["pa"] = stat.get("plateAppearances")
        if "ops" in stat:
            out["ops"] = stat.get("ops")

        # Pitching-ish
        if "inningsPitched" in stat:
            out["ip"] = stat.get("inningsPitched")
        if "era" in stat:
            out["era"] = stat.get("era")

    return out


def build_player_stats_records(
    *,
    season: int,
    team_id: int = DEFAULT_TEAM_ID,
    limit: int | None = None,
) -> list[PlayerSeasonStats]:
    """Build normalized season stat records for players in dim_players."""
    snapshot = date.today().isoformat()
    player_ids = _get_bluejays_player_ids(team_id)
    if limit is not None:
        player_ids = player_ids[: max(0, int(limit))]

    records: list[PlayerSeasonStats] = []

    for idx, player_id in enumerate(player_ids, start=1):
        # Hydrate stats for the given season.
        url = (
            f"{MLB_API_BASE}/people/{player_id}"
            f"?hydrate=stats(group=[hitting,pitching],type=[season],"
            f"season={season})"
        )
        try:
            payload = _request_json(url)
            extracted = _extract_statline(payload)

            rec = PlayerSeasonStats(
                season=int(season),
                player_id=int(player_id),
                snapshot_date=snapshot,
                team_abbr=extracted.get("team_abbr"),
                games=_safe_int(extracted.get("games")),
                pa=_safe_int(extracted.get("pa")),
                ops=_safe_str(extracted.get("ops")),
                ip=_safe_str(extracted.get("ip")),
                era=_safe_str(extracted.get("era")),
                war=None,
            )
            records.append(rec)
        except Exception as exc:  # noqa: BLE001
            # Fail-soft per player;
            # keep pipeline moving and let DQ detect gaps.
            print(
                "WARN: stats fetch failed "
                f"player_id={player_id} season={season} "
                f"err={exc}",
                file=sys.stderr,
            )

        # Basic progress every 10 players
        if idx % 10 == 0:
            print(f"Progress: processed={idx}/{len(player_ids)}")

    return records


def _safe_int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except Exception:  # noqa: BLE001
        return None


def _safe_str(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def write_stats_json(
        records: list[PlayerSeasonStats],
        output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = [asdict(r) for r in records]
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Extract Blue Jays player season stats from MLB Stats API "
            "and write JSON for the load layer."
        )
    )
    parser.add_argument(
        "--season",
        type=int,
        default=int(
            os.environ.get("STATS_SEASON", str(date.today().year))
        ),
        help="Season year to extract (default: current year or STATS_SEASON).",
    )
    parser.add_argument(
        "--team-id",
        type=int,
        default=int(
            os.environ.get("TEAM_ID", str(DEFAULT_TEAM_ID))
        ),
        help="MLB team id (default: 141 / Blue Jays or TEAM_ID env var).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional limit to N players (useful for fast debug).",
    )
    parser.add_argument(
        "--output",
        default=os.environ.get(
            "STATS_OUTPUT",
            "/opt/airflow/data/stats/player_stats.json",
        ),
        help=(
            "Output JSON path (default: /opt/airflow/data/stats/"
            "player_stats.json or STATS_OUTPUT env var)."
        ),
    )

    args = parser.parse_args()
    out = Path(args.output)

    records = build_player_stats_records(
        season=args.season,
        team_id=args.team_id,
        limit=args.limit,
    )
    write_stats_json(records, out)

    total = len(records)
    print(f"player_stats extract complete. output={out} records={total}")

    # Fail fast if we produced nothing; this prevents green-but-empty DAGs.
    if total == 0:
        print("No stats records produced; failing task.")
        sys.exit(1)


if __name__ == "__main__":
    main()

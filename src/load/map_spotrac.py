

"""Map Spotrac payroll rows to MLBAM player_id.

Spotrac payroll data is typically name-based and may not include MLBAM ids.
This module builds a bridge table that maps Spotrac records to canonical
`dim_players.player_id` values.

Expected input JSON:
- A list of dict objects.
- Required keys (best effort):
  - name: player's display name (e.g. "Vladimir Guerrero Jr.")
- Optional keys:
  - team_abbr: team abbreviation (e.g. "TOR")
  - position: player position string

Output:
- Upserts rows into `bridge_spotrac_player_map`.
- Returns a summary dict with mapping coverage.

Notes:
- Matching strategy is conservative:
  1) Exact normalized full name match.
  2) Heuristic match by last name + first initial (only when unique).
- Unmatched rows can be written to a JSON file for manual overrides.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from src.db.models import BridgeSpotracPlayerMap, DimPlayer
from src.db.session import get_session


_SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}


@dataclass(frozen=True)
class SpotracRow:
    """Minimal Spotrac row representation for mapping."""

    spotrac_name_raw: str
    team_abbr: Optional[str] = None
    position: Optional[str] = None


def _norm_space(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _normalize_name(name: str) -> str:
    """Normalize a player name for matching."""

    lowered = name.lower()
    lowered = re.sub(r"[\.,'\"()]", " ", lowered)
    lowered = re.sub(r"\s+", " ", lowered).strip()

    parts = [p for p in lowered.split(" ") if p]
    if parts and parts[-1] in _SUFFIXES:
        parts = parts[:-1]

    return " ".join(parts)


def _name_key_last_first_initial(norm_name: str) -> Optional[str]:
    """Build a heuristic key using last name and first initial."""

    parts = [p for p in norm_name.split(" ") if p]
    if len(parts) < 2:
        return None

    first = parts[0]
    last = parts[-1]
    return f"{last}|{first[0]}"


def _spotrac_player_key(
    name_raw: str,
    team_abbr: Optional[str],
    position: Optional[str],
) -> str:
    """Create a deterministic key for a Spotrac row."""

    norm_name = _normalize_name(name_raw)
    team = (team_abbr or "").strip().upper()
    pos = _norm_space(position or "").upper()
    return f"{norm_name}|{team}|{pos}"


def _parse_spotrac_row(obj: Dict[str, Any]) -> SpotracRow:
    """Parse a Spotrac dict into a SpotracRow.

    Supports flexible field names to reduce coupling:
    - name keys: name, player_name, spotrac_name, spotrac_name_raw
    - team keys: team_abbr, team, teamAbbr
    - position keys: position, pos
    """

    name = (
        obj.get("name")
        or obj.get("player_name")
        or obj.get("spotrac_name")
        or obj.get("spotrac_name_raw")
    )
    if not name or not str(name).strip():
        raise ValueError("Spotrac row missing a valid name field")

    team = obj.get("team_abbr") or obj.get("team") or obj.get("teamAbbr")
    position = obj.get("position") or obj.get("pos")

    return SpotracRow(
        spotrac_name_raw=str(name).strip(),
        team_abbr=str(team).strip() if team else None,
        position=str(position).strip() if position else None,
    )


def _build_player_indexes(
    players: Iterable[DimPlayer],
) -> Tuple[Dict[str, List[int]], Dict[str, List[int]]]:
    """Build indexes for exact and heuristic matching."""

    by_full: Dict[str, List[int]] = {}
    by_last_initial: Dict[str, List[int]] = {}

    for player in players:
        norm_full = _normalize_name(player.full_name)
        by_full.setdefault(norm_full, []).append(int(player.player_id))

        key = _name_key_last_first_initial(norm_full)
        if key:
            by_last_initial.setdefault(key, []).append(int(player.player_id))

    return by_full, by_last_initial


def map_spotrac_rows_to_players(
    spotrac_json_path: str | Path,
    unmapped_output_path: str | Path | None = None,
) -> Dict[str, Any]:
    """Map Spotrac rows to MLBAM player_id and upsert bridge rows."""

    input_path = Path(spotrac_json_path)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    payload = json.loads(input_path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("Expected a JSON array of Spotrac rows")

    spotrac_rows: List[SpotracRow] = []
    for obj in payload:
        if not isinstance(obj, dict):
            continue
        spotrac_rows.append(_parse_spotrac_row(obj))

    with get_session() as session:
        players = session.query(DimPlayer).all()

    by_full, by_last_initial = _build_player_indexes(players)

    mapped = 0
    total = len(spotrac_rows)
    unmatched: List[Dict[str, Any]] = []

    with get_session() as session:
        for row in spotrac_rows:
            norm_name = _normalize_name(row.spotrac_name_raw)

            player_id: Optional[int] = None
            method = ""
            confidence = 0.0

            full_matches = by_full.get(norm_name, [])
            if len(full_matches) == 1:
                player_id = full_matches[0]
                method = "exact_name"
                confidence = 100.0
            else:
                key = _name_key_last_first_initial(norm_name)
                candidates = by_last_initial.get(key or "", [])
                if key and len(candidates) == 1:
                    player_id = candidates[0]
                    method = "heuristic_last_initial"
                    confidence = 70.0

            if player_id is None:
                unmatched.append(
                    {
                        "spotrac_name_raw": row.spotrac_name_raw,
                        "team_abbr": row.team_abbr,
                        "position": row.position,
                        "normalized_name": norm_name,
                    }
                )
                continue

            spotrac_key = _spotrac_player_key(
                row.spotrac_name_raw,
                row.team_abbr,
                row.position,
            )

            bridge = BridgeSpotracPlayerMap(
                spotrac_player_key=spotrac_key,
                player_id=player_id,
                spotrac_name_raw=row.spotrac_name_raw,
                team_abbr=row.team_abbr,
                position=row.position,
                confidence_score=confidence,
                match_method=method,
            )

            session.merge(bridge)
            mapped += 1

    if unmapped_output_path is not None:
        out_path = Path(unmapped_output_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            json.dumps(unmatched, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    mapped_pct = 0.0
    if total > 0:
        mapped_pct = round(100.0 * mapped / total, 2)

    return {
        "total_rows": total,
        "mapped_rows": mapped,
        "mapped_pct": mapped_pct,
        "unmatched_rows": len(unmatched),
        "unmatched_preview": unmatched[:25],
    }

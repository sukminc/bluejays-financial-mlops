"""Upsert players into dim_players.

This loader reads a normalized list of player dicts produced by the MLB
Stats API extractor and upserts them into Postgres.

Expected input:
- A JSON file containing a list of player objects.
- Each player object must include `player_id` and `full_name`.

Design:
- player_id (MLBAM) is the canonical key.
- Upsert is implemented via ORM merge to keep it DB-agnostic for now.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from src.db.models import DimPlayer
from src.db.session import get_session


@dataclass(frozen=True)
class PlayerRecord:
    """Minimal player record for dim_players upsert."""

    player_id: int
    full_name: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    bats: Optional[str] = None
    throws: Optional[str] = None
    primary_position: Optional[str] = None
    birth_date: Optional[date] = None
    mlb_debut_date: Optional[date] = None


def _parse_date(value: Any) -> Optional[date]:
    """Parse YYYY-MM-DD strings into date objects."""
    if not value:
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None
    return None


def _to_player_record(obj: Dict[str, Any]) -> PlayerRecord:
    """Convert a dict into PlayerRecord with basic validation."""
    if "player_id" not in obj:
        raise ValueError("Missing required field: player_id")
    if "full_name" not in obj or not obj["full_name"]:
        raise ValueError("Missing required field: full_name")

    return PlayerRecord(
        player_id=int(obj["player_id"]),
        full_name=str(obj["full_name"]),
        first_name=obj.get("first_name"),
        last_name=obj.get("last_name"),
        bats=obj.get("bats"),
        throws=obj.get("throws"),
        primary_position=obj.get("primary_position"),
        birth_date=_parse_date(obj.get("birth_date")),
        mlb_debut_date=_parse_date(obj.get("mlb_debut_date")),
    )


def load_players_from_json(path: str | Path) -> int:
    """Load players from a JSON file and upsert into dim_players.

    Returns:
        Number of processed records.
    """
    input_path = Path(path)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    payload = json.loads(input_path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("Expected a JSON array of player objects")

    records: Iterable[PlayerRecord] = (_to_player_record(p) for p in payload)
    count = 0

    with get_session() as session:
        for rec in records:
            player = DimPlayer(
                player_id=rec.player_id,
                full_name=rec.full_name,
                first_name=rec.first_name,
                last_name=rec.last_name,
                bats=rec.bats,
                throws=rec.throws,
                primary_position=rec.primary_position,
                birth_date=rec.birth_date,
                mlb_debut_date=rec.mlb_debut_date,
            )
            # ORM-level upsert. If PK exists, merge updates fields.
            session.merge(player)
            count += 1

    return count

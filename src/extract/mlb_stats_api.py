
import os
from typing import Any, Dict, List

import requests

from src.db.session import SessionLocal
from src.db.models import DimPlayer


DEFAULT_TEAM_ID = 141  # Toronto Blue Jays
DEFAULT_TIMEOUT_SECS = 15


def fetch_roster(team_id: int = DEFAULT_TEAM_ID) -> List[Dict[str, Any]]:
    url = f"https://statsapi.mlb.com/api/v1/teams/{team_id}/roster"
    resp = requests.get(url, timeout=DEFAULT_TIMEOUT_SECS)
    resp.raise_for_status()
    return resp.json().get("roster", [])


def load_roster_to_dw(team_id: int = DEFAULT_TEAM_ID) -> int:
    """Upsert Blue Jays roster into dim_players.

    Returns:
        int: number of inserted rows (new players).
    """
    inserted = 0
    session = SessionLocal()
    try:
        roster = fetch_roster(team_id=team_id)
        for entry in roster:
            person = entry.get("person") or {}
            player_id = person.get("id")
            full_name = person.get("fullName")
            position = (entry.get("position") or {}).get("abbreviation")

            if not player_id or not full_name:
                continue

            existing = (
                session.query(DimPlayer)
                .filter(DimPlayer.player_id == int(player_id))
                .first()
            )

            if existing is None:
                session.add(
                    DimPlayer(
                        player_id=int(player_id),
                        name_display_first_last=str(full_name),
                        primary_position=str(position) if position else None,
                    )
                )
                inserted += 1
            else:
                # Keep roster sync idempotent; update name/position if changed.
                existing.name_display_first_last = str(full_name)
                existing.primary_position = (
                    str(position) if position else existing.primary_position
                )

        session.commit()
        return inserted
    finally:
        session.close()


if __name__ == "__main__":
    team_id = int(os.getenv("MLB_TEAM_ID", str(DEFAULT_TEAM_ID)))
    count = load_roster_to_dw(team_id=team_id)
    print(f"dim_players roster sync complete. inserted={count}")

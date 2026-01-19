import os
import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.models import DimPlayer, FactContract

# Database Setup
DB_URL = os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")
DB_URL = DB_URL.replace("airflow", "postgres")
engine = create_engine(DB_URL)
Session = sessionmaker(bind=engine)


def fetch_roster(team_id=141):  # 141 is Toronto Blue Jays
    """Fetches the current 40-man roster from MLB API."""
    url = f"https://statsapi.mlb.com/api/v1/teams/{team_id}/roster"
    response = requests.get(url)
    return response.json()['roster']


def load_roster_to_dw():
    session = Session()
    roster = fetch_roster()

    print(f"Processing {len(roster)} players...")

    for entry in roster:
        person = entry['person']
        player_id = person['id']
        full_name = person['fullName']
        position = entry['position']['abbreviation']

        # 1. Upsert Dimension: dim_players
        # We check if player exists; if not, create them.
        player = (
            session.query(DimPlayer)
            .filter_by(player_id=player_id)
            .first()
        )
        if not player:
            player = DimPlayer(
                player_id=player_id,
                name_display_first_last=full_name,
                primary_position=position
            )
            session.add(player)

        # 2. Upsert Fact: fact_contracts (Using 2025 dummy data)
        # In a real DE pipeline, this would join with a salary CSV or API
        contract = (
            session.query(FactContract)
            .filter_by(player_id=player_id, season=2025)
            .first()
        )
        if not contract:
            new_contract = FactContract(
                player_id=player_id,
                season=2025,
                base_salary=750000,  # MLB Minimum for practice
                luxury_tax_salary=750000,
                war=0.0
            )
            session.add(new_contract)

    session.commit()
    session.close()
    print("ETL Sync Complete.")


if __name__ == "__main__":
    load_roster_to_dw()

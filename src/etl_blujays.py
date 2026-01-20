import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.models import DimPlayer

DB_URL = "postgresql://airflow:airflow@postgres:5432/airflow"
engine = create_engine(DB_URL)
Session = sessionmaker(bind=engine)


def fetch_roster(team_id=141):
    url = f"https://statsapi.mlb.com/api/v1/teams/{team_id}/roster"
    response = requests.get(url)
    return response.json().get('roster', [])


def load_roster_to_dw():
    session = Session()
    try:
        roster = fetch_roster()
        for entry in roster:
            p_info = entry['person']
            p_id = p_info['id']
            # PEP 8 Multi-line query
            player = (
                session.query(DimPlayer)
                .filter_by(player_id=p_id)
                .first()
            )
            if not player:
                new_player = DimPlayer(
                    player_id=p_id,
                    name_display_first_last=p_info['fullName'],
                    primary_position=entry['position']['abbreviation']
                )
                session.add(new_player)
        session.commit()
        print(f"✅ ETL Sync: {len(roster)} players processed.")
    finally:
        session.close()


if __name__ == "__main__":
    load_roster_to_dw()

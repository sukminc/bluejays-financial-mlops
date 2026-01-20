import sys
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.models import DimPlayer, FactContract

# Database Setup
DB_URL = "postgresql://airflow:airflow@postgres:5432/airflow"
engine = create_engine(DB_URL)
Session = sessionmaker(bind=engine)


def load_salary_data():
    session = Session()
    try:
        # Example Data Mapping: {Player Name Fragment: (Salary, WAR)}
        # In a production DE pipeline, this would come from a CSV or API
        salary_map = {
            "George Springer": (24166666, 1.2),
            "Kevin Gausman": (21000000, 3.5),
            "José Berríos": (18000000, 2.8),
            "Chris Bassitt": (21000000, 2.1),
            "Vladimir Guerrero Jr.": (19900000, 5.0),
            "Bo Bichette": (12000000, 0.5)
        }

        print("Starting Salary/Fact load...")

        for name_fragment, (salary, war) in salary_map.items():
            # Find the player in our Dimension table
            player = (
                session.query(DimPlayer)
                .filter(
                    DimPlayer
                    .name_display_first_last
                    .like(f"%{name_fragment}%")
                    )
                .first()
            )

            if player:
                # Update or Insert Fact record for 2025
                contract = (
                    session.query(FactContract)
                    .filter_by(player_id=player.player_id, season=2025)
                    .first()
                )

                if not contract:
                    new_fact = FactContract(
                        player_id=player.player_id,
                        season=2025,
                        base_salary=salary,
                        luxury_tax_salary=salary,  # Simplified for now
                        war=war
                    )
                    session.add(new_fact)
                    print(
                        f"Added contract for {player.name_display_first_last}"
                        )
                else:
                    contract.base_salary = salary
                    contract.war = war
                    print(
                        f"Updated record for {player.name_display_first_last}"
                        )

        session.commit()
        print("✅ Fact table population complete.")
    except Exception as e:
        print(f"❌ Error loading salaries: {e}", file=sys.stderr)
        session.rollback()
    finally:
        session.close()


if __name__ == "__main__":
    load_salary_data()

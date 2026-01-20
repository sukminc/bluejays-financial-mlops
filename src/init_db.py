import sys
from sqlalchemy import create_engine
from src.models import Base


def init_db():
    conn_string = "postgresql://airflow:airflow@postgres:5432/airflow"
    try:
        engine = create_engine(conn_string)
        Base.metadata.create_all(engine)
        print("✅ Success: Schema initialized.")
    except Exception as e:
        print(f"❌ CRITICAL ERROR: {str(e)}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    init_db()

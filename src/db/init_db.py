"""Database schema initializer.

This script creates all ORM tables in Postgres for local development.
In production, use migrations (e.g., Alembic). For this project phase,
`create_all` is sufficient and deterministic.
"""

from __future__ import annotations

import sys

from src.db.models import Base
from src.db.session import engine


def init_db() -> None:
    """Create all tables defined in ORM models."""
    try:
        Base.metadata.create_all(bind=engine)
        print("Success: schema initialized.")
    except Exception as exc:
        print(f"CRITICAL ERROR: {exc}", file=sys.stderr)
        raise


if __name__ == "__main__":
    init_db()

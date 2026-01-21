"""SQLAlchemy ORM models for the Blue Jays financial data platform.

Design goals:
- Use MLBAM `player_id` as the canonical join key.
- Separate salary facts from performance stats facts.
- Support snapshot-based ingestion (reproducible, auditable runs).
- Provide a bridging table to map Spotrac (name-based) records
  to MLBAM ids.

Notes:
- This file defines schema only.
  Connection/session logic belongs in `src/db/session.py`.
"""

from __future__ import annotations

from sqlalchemy import (
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class DimPlayer(Base):
    """Player dimension table (canonical MLBAM player key)."""

    __tablename__ = "dim_players"

    player_id = Integer(primary_key=True)  # MLBAM player id
    full_name = Text(nullable=False)

    first_name = Text(nullable=True)
    last_name = Text(nullable=True)

    bats = String(8, nullable=True)
    throws = String(8, nullable=True)
    primary_position = String(16, nullable=True)

    birth_date = Date(nullable=True)
    mlb_debut_date = Date(nullable=True)

    updated_at = DateTime(
        timezone=True,
        server_default=func.now(),
        onupdate=func.now(),
    )

    # Relationships
    salaries = relationship(
        "FactSalary",
        back_populates="player"
        )
    stats = relationship(
        "FactPlayerStats",
        back_populates="player"
        )
    spotrac_maps = relationship(
        "BridgeSpotracPlayerMap",
        back_populates="player"
        )


class BridgeSpotracPlayerMap(Base):
    """Bridge table mapping Spotrac (name-based) records.

    Maps Spotrac records to canonical MLBAM `player_id`.
    """

    __tablename__ = "bridge_spotrac_player_map"

    # A deterministic key derived from a Spotrac raw record.
    # Example: normalized name + team + position.
    spotrac_player_key = Text(primary_key=True)

    player_id = Integer(
        ForeignKey("dim_players.player_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    spotrac_name_raw = Text(nullable=False)
    team_abbr = String(8, nullable=True)
    position = String(16, nullable=True)

    confidence_score = Numeric(5, 2, nullable=False, server_default="0")
    match_method = String(32, nullable=False)
    # exact_name, fuzzy, manual_override

    created_at = DateTime(timezone=True, server_default=func.now())
    updated_at = DateTime(
        timezone=True,
        server_default=func.now(),
        onupdate=func.now(),
    )

    player = relationship("DimPlayer", back_populates="spotrac_maps")


class FactSalary(Base):
    """Salary fact table (snapshot-based)."""

    __tablename__ = "fact_salary"

    season = Integer(primary_key=True)
    player_id = Integer(
        ForeignKey("dim_players.player_id", ondelete="CASCADE"),
        primary_key=True,
    )
    snapshot_date = Date(primary_key=True)

    team_abbr = String(8, nullable=True)

    salary_usd = Numeric(14, 2, nullable=True)
    aav_usd = Numeric(14, 2, nullable=True)

    contract_type = String(64, nullable=True)
    source = String(32, nullable=False, server_default="spotrac")

    created_at = DateTime(timezone=True, server_default=func.now())

    player = relationship("DimPlayer", back_populates="salaries")

    __table_args__ = (
        Index("idx_fact_salary_player_snapshot", "player_id", "snapshot_date"),
    )


class FactPlayerStats(Base):
    """Player performance fact table (snapshot-based)."""

    __tablename__ = "fact_player_stats"

    season = Integer(primary_key=True)
    player_id = Integer(
        ForeignKey("dim_players.player_id", ondelete="CASCADE"),
        primary_key=True,
    )
    snapshot_date = Date(primary_key=True)

    team_abbr = String(8, nullable=True)

    # Keep MVP stats minimal; expand as needed.
    games = Integer(nullable=True)

    # Hitting
    pa = Integer(nullable=True)
    ops = Numeric(6, 3, nullable=True)

    # Pitching
    ip = Numeric(6, 1, nullable=True)
    era = Numeric(6, 3, nullable=True)

    # Advanced (optional; depends on the chosen source)
    war = Numeric(6, 3, nullable=True)

    source = String(32, nullable=False, server_default="mlb_stats_api")
    created_at = DateTime(timezone=True, server_default=func.now())

    player = relationship("DimPlayer", back_populates="stats")

    __table_args__ = (
        Index("idx_fact_stats_player_snapshot", "player_id", "snapshot_date"),
    )


class IngestRun(Base):
    """Optional lineage table for pipeline runs and raw snapshot paths."""

    __tablename__ = "ingest_runs"

    run_id = Text(primary_key=True)  # Airflow run_id or a custom id
    dag_id = Text(nullable=True)
    task_id = Text(nullable=True)

    source = String(32, nullable=False)
    # spotrac, mlb_stats_api
    snapshot_date = Date(nullable=False)

    raw_path = Text(nullable=True)
    row_count = Integer(nullable=True)

    status = String(16, nullable=False, server_default="ok")
    created_at = DateTime(timezone=True, server_default=func.now())

    __table_args__ = (
        UniqueConstraint(
            "source",
            "snapshot_date",
            "raw_path",
            name="uq_ingest_source_date_path",
        ),
    )

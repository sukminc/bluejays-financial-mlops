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
    Column,
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
    """Player dimension table (canonical MLBAM player key).

    NOTE:
    - Current DB schema uses `name_display_first_last`.
    - Keep the Python attribute name as `full_name` for canonical joins.
    """

    __tablename__ = "dim_players"

    player_id = Column(Integer, primary_key=True)  # MLBAM player id

    # DB column is `name_display_first_last`; expose as `full_name` in Python.
    full_name = Column("name_display_first_last", Text, nullable=False)

    bats = Column(String(8), nullable=True)
    throws = Column(String(8), nullable=True)
    primary_position = Column(String(16), nullable=True)

    birth_date = Column(Date, nullable=True)

    # Relationships
    salaries = relationship("FactSalary", back_populates="player")
    stats = relationship("FactPlayerStats", back_populates="player")
    spotrac_maps = relationship(
        "BridgeSpotracPlayerMap",
        back_populates="player",
    )


class BridgeSpotracPlayerMap(Base):
    """Bridge table mapping Spotrac (name-based) records.

    Maps Spotrac records to canonical MLBAM `player_id`.
    """

    __tablename__ = "bridge_spotrac_player_map"

    # A deterministic key derived from a Spotrac raw record.
    # Example: normalized name + team + position.
    spotrac_player_key = Column(Text, primary_key=True)

    player_id = Column(
        Integer,
        ForeignKey("dim_players.player_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    spotrac_name_raw = Column(Text, nullable=False)
    team_abbr = Column(String(8), nullable=True)
    position = Column(String(16), nullable=True)

    confidence_score = Column(
        Numeric(5, 2),
        nullable=False,
        server_default="0",
    )
    match_method = Column(String(32), nullable=False)
    # exact_name, fuzzy, manual_override

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )

    player = relationship("DimPlayer", back_populates="spotrac_maps")


class FactSalary(Base):
    """Salary fact table (snapshot-based)."""

    __tablename__ = "fact_salary"

    season = Column(Integer, primary_key=True)
    player_id = Column(
        Integer,
        ForeignKey("dim_players.player_id", ondelete="CASCADE"),
        primary_key=True,
    )
    snapshot_date = Column(Date, primary_key=True)

    team_abbr = Column(String(8), nullable=True)

    salary_usd = Column(Numeric(14, 2), nullable=True)
    aav_usd = Column(Numeric(14, 2), nullable=True)

    contract_type = Column(String(64), nullable=True)
    source = Column(String(32), nullable=False, server_default="spotrac")

    created_at = Column(DateTime(timezone=True), server_default=func.now())

    player = relationship("DimPlayer", back_populates="salaries")

    __table_args__ = (
        Index("idx_fact_salary_player_snapshot", "player_id", "snapshot_date"),
    )


class FactPlayerStats(Base):
    """Player performance fact table (snapshot-based)."""

    __tablename__ = "fact_player_stats"

    season = Column(Integer, primary_key=True)
    player_id = Column(
        Integer,
        ForeignKey("dim_players.player_id", ondelete="CASCADE"),
        primary_key=True,
    )
    snapshot_date = Column(Date, primary_key=True)

    team_abbr = Column(String(8), nullable=True)

    # Keep MVP stats minimal; expand as needed.
    games = Column(Integer, nullable=True)

    # Hitting
    pa = Column(Integer, nullable=True)
    ops = Column(Numeric(6, 3), nullable=True)

    # Pitching
    ip = Column(Numeric(6, 1), nullable=True)
    era = Column(Numeric(6, 3), nullable=True)

    # Advanced (optional; depends on the chosen source)
    war = Column(Numeric(6, 3), nullable=True)

    source = Column(String(32), nullable=False, server_default="mlb_stats_api")
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    player = relationship("DimPlayer", back_populates="stats")

    __table_args__ = (
        Index("idx_fact_stats_player_snapshot", "player_id", "snapshot_date"),
    )


class IngestRun(Base):
    """Optional lineage table for pipeline runs and raw snapshot paths."""

    __tablename__ = "ingest_runs"

    # Airflow run_id or a custom id
    run_id = Column(Text, primary_key=True)
    dag_id = Column(Text, nullable=True)
    task_id = Column(Text, nullable=True)

    source = Column(String(32), nullable=False)
    # spotrac, mlb_stats_api
    snapshot_date = Column(Date, nullable=False)

    raw_path = Column(Text, nullable=True)
    row_count = Column(Integer, nullable=True)

    status = Column(String(16), nullable=False, server_default="ok")
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint(
            "source",
            "snapshot_date",
            "raw_path",
            name="uq_ingest_source_date_path",
        ),
    )

"""SQLAlchemy ORM models.

Notes:
- Keep the schema simple and stable for Airflow + Docker execution.
- Use explicit Column(...) declarations (do not call
  Integer(primary_key=True)).
"""

from __future__ import annotations

from datetime import date

from sqlalchemy import BigInteger
from sqlalchemy import Column
from sqlalchemy import Date
from sqlalchemy import Float
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import UniqueConstraint
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import relationship


Base = declarative_base()


class DimPlayer(Base):
    """Canonical player dimension keyed by MLBAM player_id."""

    __tablename__ = "dim_players"

    # MLBAM player id
    player_id = Column(Integer, primary_key=True)
    full_name = Column(String(200), nullable=False)
    first_name = Column(String(100), nullable=True)
    last_name = Column(String(100), nullable=True)
    team_id = Column(Integer, nullable=True)
    team_name = Column(String(100), nullable=True)
    primary_position = Column(String(50), nullable=True)
    bats = Column(String(5), nullable=True)
    throws = Column(String(5), nullable=True)

    salaries = relationship("FactSalary", back_populates="player")
    stats = relationship("FactPlayerStats", back_populates="player")


class SpotracPlayerMap(Base):
    """Bridge table to map Spotrac display name -> MLBAM player_id."""

    __tablename__ = "bridge_spotrac_player_map"

    id = Column(Integer, primary_key=True, autoincrement=True)
    spotrac_name = Column(String(200), nullable=False)
    player_id = Column(
        Integer,
        ForeignKey("dim_players.player_id"),
        nullable=False,
    )
    season = Column(Integer, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "spotrac_name",
            "season",
            name="uq_spotrac_name_season",
        ),
    )

    player = relationship("DimPlayer")


class FactSalary(Base):
    """Salary fact table keyed by (player_id, season)."""

    __tablename__ = "fact_salary"

    salary_id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(
        Integer,
        ForeignKey("dim_players.player_id"),
        nullable=False,
    )
    season = Column(Integer, nullable=False)
    base_salary = Column(BigInteger, nullable=True)
    luxury_tax_salary = Column(BigInteger, nullable=True)
    source = Column(String(50), nullable=False, default="spotrac")
    snapshot_date = Column(Date, nullable=False, default=date.today)

    __table_args__ = (
        UniqueConstraint(
            "player_id",
            "season",
            name="uq_fact_salary_player_season",
        ),
    )

    player = relationship("DimPlayer", back_populates="salaries")


class FactPlayerStats(Base):
    """Player performance stats fact table keyed by (player_id, season).

    Keep this minimal for MVP; extend as needed.
    """

    __tablename__ = "fact_player_stats"

    stats_id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(
        Integer,
        ForeignKey("dim_players.player_id"),
        nullable=False,
    )
    season = Column(Integer, nullable=False)

    # Example metric(s). Add more columns later once the API payload is stable.
    war = Column(Float, nullable=True)

    snapshot_date = Column(Date, nullable=False, default=date.today)

    __table_args__ = (
        UniqueConstraint(
            "player_id",
            "season",
            name="uq_fact_stats_player_season",
        ),
    )

    player = relationship("DimPlayer", back_populates="stats")

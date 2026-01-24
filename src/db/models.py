from sqlalchemy import (
    Column, Integer, Float, DateTime, Text, BigInteger
)
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()


class StgSpotracRaw(Base):
    """
    Raw Staging Table for Spotrac CSV Data.
    """
    __tablename__ = 'stg_spotrac_bluejays_salary_raw'

    id = Column(Integer, primary_key=True, autoincrement=True)
    raw_loaded_at = Column(DateTime(timezone=True), server_default=func.now())

    season = Column(Text)
    player_name = Column(Text)
    position = Column(Text)
    age = Column(Text)
    contract_type = Column(Text)
    luxury_tax_usd = Column(Text)
    luxury_tax_pct = Column(Text)
    cash_salary_usd = Column(Text)
    free_agent_year = Column(Text)


class FactSalary(Base):
    """
    Final Fact Table: Cleaned Salary Data.
    """
    __tablename__ = 'fact_salary'

    id = Column(Integer, primary_key=True, autoincrement=True)
    player_name = Column(Text)
    player_id = Column(Integer, nullable=True)
    season = Column(Integer, nullable=False)
    luxury_tax_salary = Column(BigInteger)
    cash_salary = Column(BigInteger)
    luxury_tax_pct = Column(Float)
    contract_type = Column(Text)
    loaded_at = Column(DateTime(timezone=True), server_default=func.now())


class FactPlayerStats(Base):
    """
    Detailed MLB Stats (2022-2025)
    Source: MLB Stats API
    """
    __tablename__ = 'fact_player_stats'

    id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(Integer)
    player_name = Column(Text)
    season = Column(Integer)
    position = Column(Text)
    stat_type = Column(Text)  # 'hitting' or 'pitching'
    games = Column(Integer)

    # Hitting (Detailed)
    plate_appearances = Column(Integer)
    at_bats = Column(Integer)
    hits = Column(Integer)
    doubles = Column(Integer)
    triples = Column(Integer)
    homeruns = Column(Integer)
    rbi = Column(Integer)
    base_on_balls = Column(Integer)
    strikeouts = Column(Integer)
    stolen_bases = Column(Integer)
    caught_stealing = Column(Integer)
    avg = Column(Text)
    obp = Column(Text)
    slg = Column(Text)
    ops = Column(Text)

    # Pitching (Detailed)
    innings_pitched = Column(Text)
    wins = Column(Integer)
    losses = Column(Integer)
    saves = Column(Integer)
    blown_saves = Column(Integer)
    hits_allowed = Column(Integer)
    runs_allowed = Column(Integer)
    era = Column(Text)
    whip = Column(Text)

    loaded_at = Column(DateTime(timezone=True), server_default=func.now())

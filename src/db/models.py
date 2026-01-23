from sqlalchemy import Column, Integer, DateTime, func, Text
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class StgSpotracRaw(Base):
    """
    Raw Staging Table for Spotrac CSV Data.
    Stores all columns as TEXT to prevent load failures during ingestion.
    """
    __tablename__ = 'stg_spotrac_bluejays_salary_raw'

    # Auto-incrementing ID for tracking
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Metadata: when was this row inserted?
    raw_loaded_at = Column(DateTime(timezone=True), server_default=func.now())

    # CSV Columns
    season = Column(Text)
    player_name = Column(Text)
    position = Column(Text)
    age = Column(Text)
    contract_type = Column(Text)
    luxury_tax_usd = Column(Text)
    luxury_tax_pct = Column(Text)
    cash_salary_usd = Column(Text)
    free_agent_year = Column(Text)
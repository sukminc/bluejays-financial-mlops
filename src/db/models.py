from sqlalchemy import Column, Integer, DateTime, func, Text, BigInteger, Float
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class StgSpotracRaw(Base):
    """
    Raw Staging Table for Spotrac CSV Data.
    Stores all columns as TEXT to prevent load failures during ingestion.
    """
    __tablename__ = 'stg_spotrac_bluejays_salary_raw'

    id = Column(Integer, primary_key=True, autoincrement=True)
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


class FactSalary(Base):
    """
    Final Fact Table: Cleaned, Typed, and Ready for Analytics.
    Source: stg_spotrac_bluejays_salary_raw
    """
    __tablename__ = 'fact_salary'

    id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(Integer, nullable=True)  # Placeholder for future link
    season = Column(Integer, nullable=False)

    # Cleaned Numeric Values
    luxury_tax_salary = Column(BigInteger)
    cash_salary = Column(BigInteger)
    luxury_tax_pct = Column(Float)

    # Metadata
    contract_type = Column(Text)
    loaded_at = Column(DateTime(timezone=True), server_default=func.now())

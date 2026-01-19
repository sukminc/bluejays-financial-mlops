from sqlalchemy import (
    Column, Integer, String, Float, ForeignKey, Date, BigInteger
)
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


class DimPlayer(Base):
    __tablename__ = "dim_players"

    player_id = Column(Integer, primary_key=True)  # MLB AM ID
    name_display_first_last = Column(String(100), nullable=False)
    primary_position = Column(String(10))
    birth_date = Column(Date)
    bats = Column(String(5))
    throws = Column(String(5))
    # Relationship to facts
    contracts = relationship("FactContract", back_populates="player")


class FactContract(Base):
    __tablename__ = "fact_contracts"

    contract_id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(
        Integer,
        ForeignKey("dim_players.player_id"),
        nullable=False
    )
    season = Column(Integer, nullable=False)
    base_salary = Column(BigInteger)
    luxury_tax_salary = Column(BigInteger)  # AAV (Average Annual Value)
    war = Column(Float)  # Performance metric for efficiency analysis

    player = relationship("DimPlayer", back_populates="contracts")

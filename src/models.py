from sqlalchemy import (
    Column,
    Integer,
    String,
    BigInteger,
    ForeignKey,
    Date,
    Float,
)
from sqlalchemy.orm import relationship, declarative_base

# Legacy compatible base for SQLAlchemy 1.4/2.0
Base = declarative_base()


class DimPlayer(Base):
    __tablename__ = "dim_players"

    player_id = Column(Integer, primary_key=True)
    name_display_first_last = Column(String(100), nullable=False)
    primary_position = Column(String(10))
    birth_date = Column(Date)
    bats = Column(String(5))
    throws = Column(String(5))
    contracts = relationship("FactContract", back_populates="player")


class FactContract(Base):
    __tablename__ = "fact_contracts"

    contract_id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(
        Integer,
        ForeignKey("dim_players.player_id"),
        nullable=False,
    )
    season = Column(Integer, nullable=False)
    base_salary = Column(BigInteger)
    luxury_tax_salary = Column(BigInteger)
    war = Column(Float)

    player = relationship("DimPlayer", back_populates="contracts")

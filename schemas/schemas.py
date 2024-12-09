import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey
from sqlalchemy.orm import relationship

from schemas.base import Base


class NOCMapping(Base):
    __tablename__ = 'noc_mapping'

    id = Column(Integer, primary_key=True, autoincrement=True)
    noc_code = Column(String(3), unique=True, nullable=False)
    country_name = Column(String(100), nullable=False)

    # Relationships
    olympics_records = relationship('OlympicsMedals', back_populates='noc_reference')
    country_records = relationship('Countries', back_populates='noc_reference')  # Use string reference


class Countries(Base):
    __tablename__ = 'countries'

    id = Column(Integer, primary_key=True, autoincrement=True)
    country = Column(String(100), nullable=False)  # Country name
    region = Column(String(100))  # Region
    population = Column(Integer)  # Population of the country
    area_sq_mi = Column(Float)  # Area in square miles
    pop_density_per_sq_mi = Column(Float)  # Population density per square mile
    coastline_ratio = Column(Float)  # Coastline (coast/area ratio)
    net_migration = Column(Float)  # Net migration
    infant_mortality_per_1000 = Column(Float)  # Infant mortality per 1000 births
    gdp_per_capita = Column(Integer)  # GDP per capita
    literacy_percent = Column(Float)  # Literacy percentage
    phones_per_1000 = Column(Float)  # Phones per 1000 people
    arable_percent = Column(Float)  # Arable land percentage
    crops_percent = Column(Float)  # Crops percentage
    other_percent = Column(Float)  # Other land usage percentage
    climate = Column(Float)  # Climate type
    birthrate = Column(Float)  # Birthrate
    deathrate = Column(Float)  # Deathrate
    agriculture = Column(Float)  # Agriculture
    industry = Column(Float)  # Industry
    service = Column(Float)  # Service

    # Foreign Key and Relationship with NOCMapping
    noc_mapping_id = Column(Integer, ForeignKey('noc_mapping.id'))
    noc_reference = relationship("NOCMapping", back_populates="country_records")

    olympics_records = relationship("CountryOlympics", back_populates="country")


class OlympicsMedals(Base):
    __tablename__ = 'olympics_medals'

    id = Column(Integer, primary_key=True, autoincrement=True)
    nation = Column(String(100), nullable=False)  # Nation name
    year = Column(Integer, nullable=False)  # Year of the event
    gold = Column(Integer, default=0)  # Number of gold medals
    silver = Column(Integer, default=0)  # Number of silver medals
    bronze = Column(Integer, default=0)  # Number of bronze medals
    total = Column(Integer, default=0)  # Total medals

    # Foreign Key and Relationship with NOCMapping
    noc_mapping_id = Column(Integer, ForeignKey('noc_mapping.id'))
    noc_reference = relationship("NOCMapping", back_populates="olympics_records")

    country_records = relationship("CountryOlympics", back_populates="olympics_medals")


class CountryOlympics(Base):
    __tablename__ = 'country_olympics'

    id = Column(Integer, primary_key=True, autoincrement=True)
    country_id = Column(Integer, ForeignKey('countries.id'), nullable=False)
    olympics_medals_id = Column(Integer, ForeignKey('olympics_medals.id'), nullable=False)
    total_medals = Column(Integer, nullable=False)
    first_year = Column(Integer, nullable=False)
    avg_medals_per_year = Column(Float, nullable=False)
    total_gold = Column(Integer, nullable=False)
    total_silver = Column(Integer, nullable=False)
    total_bronze = Column(Integer, nullable=False)
    years_participated = Column(Integer, nullable=False)

    country = relationship("Countries", back_populates="olympics_records")
    olympics_medals = relationship("OlympicsMedals", back_populates="country_records")


if __name__ == "__main__":
    load_dotenv()

    # Connect to the database using credentials from the environment variables
    db_url = os.getenv("DATABASE_URL")
    engine = create_engine(db_url, echo=True)
import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, inspect
from sqlalchemy.orm import relationship, declarative_base, sessionmaker
from schemas.noc_mapping_schema import NOCMapping

# Set up the base class for our ORM models
Base = declarative_base()


# Define the Countries table structure
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
    noc_mapping_id = Column(Integer, ForeignKey(NOCMapping.id))
    noc_reference = relationship("NOCMapping", back_populates="country_records")

    olympics_records = relationship("CountryOlympics", back_populates="country")


def main():
    # Load environment variables from a .env file to get DB credentials and other settings
    load_dotenv()

    # Connect to the database using credentials from the environment variables
    db_url = os.getenv("DATABASE_URL")
    engine = create_engine(db_url, echo=True)  # Enable echo for SQL statement logging

    # Create the table in the database if it doesn't already exist
    Base.metadata.create_all(engine)

    # Use the inspector to verify that the table was actually created
    inspector = inspect(engine)
    tables = inspector.get_table_names(schema='public')
    if 'countries' in tables:
        print("Table 'countries' created successfully in olympics_data.")
    else:
        print("Table 'countries' was not created in olympics_data.")

    # Set up a session to interact with the database
    Session = sessionmaker(bind=engine)
    session = Session()

    # Always close the session when done to free up resources
    session.close()


if __name__ == "__main__":
    main()

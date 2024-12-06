import os
from sqlalchemy import create_engine, Column, Integer, String, inspect
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from dotenv import load_dotenv

Base = declarative_base()


# Define the NOCMapping table structure
class NOCMapping(Base):
    __tablename__ = 'noc_mapping'

    id = Column(Integer, primary_key=True, autoincrement=True)
    noc_code = Column(String(3), unique=True, nullable=False)  # 3-letter NOC code (e.g., 'USA', 'AFG')
    country_name = Column(String(100), nullable=False)  # Full country name (e.g., 'United States', 'Afghanistan')

    # Relationships with other tables
    olympics_records = relationship("OlympicsMedals", back_populates="noc_reference")
    country_records = relationship("Countries", back_populates="noc_reference")


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
    if 'noc_mapping' in tables:
        print("Table 'noc_mapping' created successfully in olympics_data.")
    else:
        print("Table 'noc_mapping' was not created in olympics_data.")

    # Set up a session to interact with the database
    Session = sessionmaker(bind=engine)
    session = Session()

    # Always close the session when done to free up resources
    session.close()


if __name__ == "__main__":
    main()

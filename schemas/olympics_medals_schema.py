import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, inspect
from sqlalchemy.orm import declarative_base, sessionmaker

# Set up the base class for our ORM models
Base = declarative_base()


# Define the OlympicsMedals table structure
class OlympicsMedals(Base):
    __tablename__ = 'olympics_medals'
    __table_args__ = {'schema': 'public'}

    # Define columns for the table
    id = Column(Integer, primary_key=True, autoincrement=True)
    nation = Column(String(100), nullable=False)  # Nation name, must not be null
    year = Column(Integer, nullable=False)  # Year of the event, must not be null
    gold = Column(Integer, default=0)  # Number of gold medals, default to 0
    silver = Column(Integer, default=0)  # Number of silver medals, default to 0
    bronze = Column(Integer, default=0)  # Number of bronze medals, default to 0
    total = Column(Integer, default=0)  # Total medals, default to 0
    event = Column(String(100))  # Event description


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
    if 'olympics_medals' in tables:
        print("Table 'olympics_medals' created successfully in holman_test_database.")
    else:
        print("Table 'olympics_medals' was not created in holman_test_database.")

    # Set up a session to interact with the database
    Session = sessionmaker(bind=engine)
    session = Session()

    # Always close the session when done to free up resources
    session.close()


if __name__ == "__main__":
    main()
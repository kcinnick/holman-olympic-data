import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from schemas.countries_schema import Base


def create_countries_dataframe(file_path):
    """
    Create a dataframe from the CSV file containing countries data.
    :param file_path: Path to the CSV file.
    :return: A Pandas DataFrame containing the data.
    """
    df = pd.read_csv(file_path)
    # Clean up column names to be more consistent
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

    return df


def upsert_countries_data(df, engine):
    """
    Insert or update the countries data into the database.
    :param df: DataFrame containing countries data.
    :param engine: SQLAlchemy engine connected to the target database.
    :return: None
    """
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Insert data into the countries table
        df.to_sql('countries', con=engine, if_exists='replace', index=False)
        session.commit()
        print("Data upserted successfully!")
    except Exception as e:
        print(f"An error occurred: {e}")
        session.rollback()
    finally:
        session.close()

    return


def main():
    # Load environment variables from a .env file to get DB credentials and other settings
    load_dotenv()

    # Connect to the database using credentials from the environment variables
    db_url = os.getenv("DATABASE_URL")
    engine = create_engine(db_url, echo=True)  # Enable echo for SQL statement logging

    # Ensure that the tables are created in the database
    Base.metadata.create_all(engine)

    # Load the countries data from the CSV file
    file_path = os.getenv("COUNTRIES_DATASET")
    df = create_countries_dataframe(file_path)

    # Upsert the countries data into the database
    upsert_countries_data(df, engine)


if __name__ == "__main__":
    main()

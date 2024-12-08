import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from schemas.base import Base


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

    # Rename columns to match the database schema
    df = df.rename(columns={
        'area_(sq._mi.)': 'area_sq_mi',
        'pop._density_(per_sq._mi.)': 'pop_density_per_sq_mi',
        'coastline_(coast/area_ratio)': 'coastline_ratio',
        'infant_mortality_(per_1000_births)': 'infant_mortality_per_1000',
        'gdp_($_per_capita)': 'gdp_per_capita',
        'literacy_(%)': 'literacy_percent',
        'phones_(per_1000)': 'phones_per_1000',
        'arable_(%)': 'arable_percent',
        'crops_(%)': 'crops_percent',
        'other_(%)': 'other_percent',
    })

    # Replace commas with periods only in numeric columns if they are strings
    numeric_columns = [
        'pop_density_per_sq_mi',
        'coastline_ratio',
        'net_migration',
        'infant_mortality_per_1000',
        'gdp_per_capita',
        'literacy_percent',
        'phones_per_1000',
        'arable_percent',
        'crops_percent',
        'other_percent',
        'climate',
        'birthrate',
        'deathrate',
        'agriculture',
        'industry',
        'service',
    ]
    for col in numeric_columns:
        if col in df.columns and df[col].dtype == 'object':
            df[col] = df[col].str.replace(',', '.', regex=False)

    # Convert numeric columns to proper types
    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')

    # Replace NaN values with None
    df = df.replace({pd.NA: None, float('nan'): None, pd.NaT: None})

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

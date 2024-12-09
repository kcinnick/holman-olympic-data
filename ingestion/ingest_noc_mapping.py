import os
import requests
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from schemas.base import Base
from sqlalchemy.exc import IntegrityError


def fetch_noc_mapping():
    """
    Fetch NOC mapping data from the REST API and apply custom name mappings.
    :return: Pandas DataFrame containing country names and their country codes.
    """
    # Custom mapping for country names
    custom_mappings = {
        "South Korea": "Korea, South",
        "North Korea": "Korea, North",
        "Czechia": "Czech Republic",
        "Trinidad and Tobago": "Trinidad & Tobago",
        "Ivory Coast": "Cote d'Ivoire",
        "North Macedonia": "Macedonia",
        "Taiwan": "Chinese Taipei",
        "Germany": "Germany",
        "Greece": "Greece",
        "Netherlands": "Netherlands",
        "Ireland": "Ireland",
        "Montenegro": "Montenegro"
    }

    url = "https://restcountries.com/v3.1/all"
    response = requests.get(url)

    if response.status_code != 200:
        raise Exception(f"Failed to fetch data from API. Status code: {response.status_code}")

    data = response.json()

    records = []
    for country in data:
        name = country.get("name", {}).get("common")
        codes = country.get("cca3")  # Use ISO 3166-1 alpha-3 code

        if name and codes:
            # Apply custom mapping if necessary
            name = custom_mappings.get(name, name)
            records.append({
                "country_name": name,
                "noc_code": codes
            })

    return pd.DataFrame(records)


def log_unmatched_rows(df, filename, message):
    """
    Log unmatched rows to a CSV file and print a message.
    """
    if not df.empty:
        file_path = os.path.join("debug_logs", filename)
        df.to_csv(file_path, index=False)
        print(f"{message}: {len(df)} rows logged to {file_path}")
    else:
        print(f"{message}: No unmatched rows.")


def upsert_noc_mapping_data(df, engine):
    """
    Insert or update the NOC mapping data into the database using Pandas.
    :param df: DataFrame containing NOC mapping data.
    :param engine: SQLAlchemy engine connected to the target database.
    :return: None
    """
    try:
        # Log unmatched rows (if applicable, adjust logic for specific checks)
        unmatched_df = df[df['country_name'].isnull() | df['noc_code'].isnull()]
        log_unmatched_rows(unmatched_df, "unmatched_noc_mapping.csv", "Unmatched NOC mapping rows")

        # Insert data into the noc_mapping table
        df.to_sql('noc_mapping', con=engine, if_exists='replace', index=False)
        print("NOCMapping data upserted successfully!")
    except IntegrityError as e:
        print(f"Integrity error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")



def main():
    """
    Main function to fetch and upsert NOC mapping data into the database.
    """
    # Load environment variables
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")

    if not db_url:
        raise EnvironmentError("DATABASE_URL is not set in the .env file.")

    # Create SQLAlchemy engine
    engine = create_engine(db_url, echo=True)

    # Ensure that the tables are created in the database
    Base.metadata.create_all(engine)

    # Fetch the NOC mapping data
    df = fetch_noc_mapping()

    # Upsert the NOC mapping data into the database
    upsert_noc_mapping_data(df, engine)


if __name__ == "__main__":
    main()

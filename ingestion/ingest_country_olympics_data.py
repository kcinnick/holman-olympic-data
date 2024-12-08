import os

import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from sqlalchemy.exc import IntegrityError


def load_datasets(engine):
    """
    Load the required tables into Pandas DataFrames.
    :param engine: SQLAlchemy engine connected to the target database.
    :return: A dictionary containing DataFrames for noc_mapping, countries, and olympics_medals.
    """
    noc_mapping_df = pd.read_sql_table('noc_mapping', engine)
    countries_df = pd.read_sql_table('countries', engine)
    olympics_medals_df = pd.read_sql_table('olympics_medals', engine)

    dataframes = {
        'noc_mapping': noc_mapping_df,
        'countries': countries_df,
        'olympics_medals': olympics_medals_df
    }

    return dataframes


def merge_data(dataframes):
    """
    Load and merge the noc_mapping, countries, and olympics_medals DataFrames.
    """
    noc_mapping_df = dataframes['noc_mapping']
    countries_df = dataframes['countries']
    olympics_medals_df = dataframes['olympics_medals']

    # Normalize and map data
    noc_mapping_df['noc_code'] = noc_mapping_df['noc_code'].str.lower().str.strip()
    noc_mapping_df['country_name'] = noc_mapping_df['country_name'].str.lower().str.strip()
    countries_df['country'] = countries_df['country'].str.lower().str.strip()
    olympics_medals_df['nation'] = olympics_medals_df['nation'].str.lower().str.strip()

    # Join olympics medals with NOC mapping on NOC code and nation
    olympics_noc_df = pd.merge(
        olympics_medals_df,
        noc_mapping_df,
        left_on='nation',
        right_on='noc_code',
        how='inner'
    )

    # Join with countries on normalized country name
    final_df = pd.merge(
        olympics_noc_df,
        countries_df,
        left_on='country_name',  # country_name from NOC mapping
        right_on='country',  # country from countries table
        how='inner'
    )

    # Aggregate the data
    aggregated_df = (
        final_df.groupby(['id', 'country_name'], as_index=False)
        .agg(
            total_medals=('total', 'sum'),  # Total medals over all years
            first_year=('year', 'min')  # First year of participation
        )
    )

    # Rename columns for clarity
    aggregated_df.rename(columns={'id': 'country_id'}, inplace=True)

    # Include country name
    aggregated_df = aggregated_df[['country_id', 'country_name', 'total_medals', 'first_year']]

    return aggregated_df


def upsert_country_olympics_data(df, engine):
    """
    Insert or update the country_olympics data into the database.
    """
    try:
        # Insert data into the country_olympics table
        df.to_sql('country_olympics', con=engine, if_exists='replace', index=False)
        print("CountryOlympics data upserted successfully!")
    except IntegrityError as e:
        print(f"Integrity error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

    return


def main():
    """
    Main function to create and upsert records into the CountryOlympics table.
    """
    # Load environment variables
    load_dotenv()

    # Connect to the database
    db_url = os.getenv("DATABASE_URL")
    engine = create_engine(db_url, echo=False)

    # Load tables into DataFrames
    dataframes = load_datasets(engine)

    # Create the CountryOlympics DataFrame
    country_olympics_df = merge_data(dataframes)

    # Upsert the CountryOlympics data into the database
    upsert_country_olympics_data(country_olympics_df, engine)

    return


if __name__ == "__main__":
    main()

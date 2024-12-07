import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os


def load_and_merge_data(engine):
    """
    Load the noc_mapping, countries, and olympics_medals tables into memory via pandas,
    merge them into one DataFrame, and save as a CSV & Parquet file.
    """
    # Load tables into pandas DataFrames
    noc_mapping_df = pd.read_sql_table('noc_mapping', engine)
    countries_df = pd.read_sql_table('countries', engine)
    olympics_medals_df = pd.read_sql_table('olympics_medals', engine)

    # Normalize and map data
    noc_mapping_df['noc_code'] = noc_mapping_df['noc_code'].str.lower().str.strip()
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

    # Normalize 'country_name' and 'country' for consistent matching
    olympics_noc_df['country_name'] = olympics_noc_df['country_name'].str.lower().str.strip()
    countries_df['country'] = countries_df['country'].str.lower().str.strip()

    # Join with countries on normalized country name
    final_df = pd.merge(
        olympics_noc_df,
        countries_df,
        left_on='country_name',  # country_name from NOC mapping
        right_on='country',  # country from countries table
        how='inner'
    )

    # Preserve noc_mapping_id
    final_df['noc_mapping_id'] = noc_mapping_df['id']  # Ensure the correct NOCMapping ID is included

    # Debugging prints
    print("Olympics NOC DataFrame:")
    print(olympics_noc_df)

    print("\nCountries DataFrame:")
    print(countries_df)

    print("\nFinal Merged DataFrame:")
    print(final_df)

    return final_df


def main():
    # Load environment variables from a .env file
    load_dotenv()

    # Connect to the database
    db_url = os.getenv("DATABASE_URL")
    engine = create_engine(db_url, echo=False)  # Disable SQL logging for cleaner output

    # Load, merge, and save data
    merged_data = load_and_merge_data(engine)
    if merged_data.empty:
        print("The resulting dataset is empty. Check data consistency or missing NOC mappings.")
    else:
        merged_data.to_csv('merged_country_olympics_data.csv', index=False)
        print("Merged data saved to 'merged_country_olympics_data.csv'.")
        merged_data.to_parquet('merged_country_olympics_data.parquet', index=False)
        print("Merged data saved to 'merged_country_olympics_data.parquet'.")


if __name__ == "__main__":
    main()

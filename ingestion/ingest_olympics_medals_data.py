import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from schemas.base import Base


def get_year_from_filename(filename):
    """
    Extract the year from the filename given known file patterns.
    :param filename: Filename of the dataset.
    :return: year extracted from the filename
    """
    try:
        year = int(filename.split()[1])
    except IndexError:
        year = int(filename.split('_')[1])
    return year


def load_datasets():
    """
    Load datasets from the data directory into a single DataFrame.
    :return: Pandas DataFrame containing all data combined.
    """
    datasets_path = os.getenv("OLYMPICS_DATA_PATH")
    all_data = []

    for filename in os.listdir(datasets_path):
        if filename.endswith(".csv"):
            file_path = os.path.join(datasets_path, filename)
            year = get_year_from_filename(filename)

            # Load the CSV and add a 'year' column
            df = pd.read_csv(file_path)
            df['year'] = year

            # Normalize numeric columns to integers
            numeric_columns = ['Gold', 'Silver', 'Bronze', 'Total']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

            all_data.append(df)

    # Concatenate all datasets into a single DataFrame
    combined_df = pd.concat(all_data, ignore_index=True)

    # Clean column names to be more consistent
    combined_df.columns = combined_df.columns.str.strip().str.lower()

    return combined_df


def upsert_olympics_medals_data(df, engine):
    """
    Insert or update the olympics_medals data into the database using Pandas.
    :param df: DataFrame containing olympics medals data.
    :param engine: SQLAlchemy engine connected to the target database.
    :return: None
    """
    # Clean and normalize data
    df = df.rename(columns={
        'noc': 'nation',
        'gold': 'gold',
        'silver': 'silver',
        'bronze': 'bronze',
        'total': 'total',
    })

    numeric_columns = ['gold', 'silver', 'bronze', 'total']
    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce').fillna(0).astype(int)

    # Replace NaN values with None for non-numeric columns
    df = df.where(pd.notnull(df), None)

    try:
        # Insert data into the olympics_medals table
        df.to_sql('olympics_medals', con=engine, if_exists='replace', index=False)
        print("OlympicsMedals data upserted successfully!")
    except Exception as e:
        print(f"An error occurred: {e}")


def main():
    """
    Main function to load datasets and insert data into the database.
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

    # Load datasets into a DataFrame
    df = load_datasets()

    # Upsert the olympics medals data into the database
    upsert_olympics_medals_data(df, engine)


if __name__ == "__main__":
    main()

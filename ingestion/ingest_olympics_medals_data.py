import os
import csv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from schemas.olympics_medals_schema import OlympicsMedals, Base


def get_year_from_filename(filename):
    """
    Extract the year from the filename given known file patterns.
    :param filename:
    :return: year extracted from the filename
    """
    try:
        year = int(filename.split()[1])
    except IndexError:
        year = int(filename.split('_')[1])

    return year


def load_datasets(file_path='../datasets/olympics'):
    datasets_path = "../datasets/olympics"
    for filename in os.listdir(datasets_path):
        if filename.endswith(".csv"):
            file_path = os.path.join(datasets_path, filename)
            year = get_year_from_filename(filename)
            with open(file_path, mode='r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    yield row, year


def create_olympics_medals_entry(row, year):
    return OlympicsMedals(
        nation=row['NOC'],
        year=year,
        gold=int(row.get('Gold', 0)),
        silver=int(row.get('Silver', 0)),
        bronze=int(row.get('Bronze', 0)),
        total=int(row.get('Total', 0)),
    )


def main():
    # Load environment variables from a .env file to get DB credentials and other settings
    load_dotenv()

    # Connect to the database using credentials from the environment variables
    db_url = os.getenv("DATABASE_URL")
    engine = create_engine(db_url, echo=True)  # Enable echo for SQL statement logging

    # Ensure that the tables are created in the database
    Base.metadata.create_all(engine)

    # Set up a session to interact with the database
    Session = sessionmaker(bind=engine)
    session = Session()

    # Load datasets and insert data into the database
    for row, year in load_datasets():
        olympics_medals_entry = create_olympics_medals_entry(row, year)
        session.add(olympics_medals_entry)

    try:
        session.commit()
        print("Data inserted successfully!")
    except Exception as e:
        print(f"An error occurred: {e}")
        session.rollback()
    finally:
        session.close()


if __name__ == "__main__":
    main()

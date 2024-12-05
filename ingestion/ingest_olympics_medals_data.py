import os
import csv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from schemas.olympics_medals_schema import OlympicsMedals, Base


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

    try:
        # Load data from CSV files into the database
        datasets_path = "../datasets/olympics"
        for filename in os.listdir(datasets_path):
            if filename.endswith(".csv"):
                file_path = os.path.join(datasets_path, filename)
                with open(file_path, mode='r', encoding='utf-8') as csvfile:
                    reader = csv.DictReader(csvfile)
                    for row in reader:
                        # Extract year from the filename
                        print(filename)
                        try:
                            year = int(filename.split()[1])
                        except IndexError:
                            year = int(filename.split('_')[1])
                            # TODO: control for more potential file name formats
                        print(f"Ingesting data for {row['NOC']} in {year}...")

                        # Create an OlympicsMedals instance for each row
                        medal_entry = OlympicsMedals(
                            nation=row['NOC'],
                            year=year,
                            gold=int(row.get('Gold', 0)),
                            silver=int(row.get('Silver', 0)),
                            bronze=int(row.get('Bronze', 0)),
                            total=int(row.get('Total', 0)),
                            event=row.get('Event', None)
                        )
                        # Add the entry to the session
                        session.add(medal_entry)

        # Commit all the new records to the database
        session.commit()
        print("Data ingestion completed successfully.")
    except Exception as e:
        # Rollback the session in case of an error
        session.rollback()
        print(f"An error occurred during data ingestion: {e}")
    finally:
        # Close the session
        session.close()


if __name__ == "__main__":
    main()

import csv
import os
import tempfile
import unittest

from dotenv import load_dotenv
from sqlalchemy import create_engine

from ingestion.ingest_olympics_medals_data import get_year_from_filename, load_datasets, upsert_olympics_medals_data
import pandas as pd
from sqlalchemy.sql import text


class TestIngestOlympicsData(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Load environment variables from a .env file
        load_dotenv()

    def setUp(self):
        # Create a temporary directory to hold CSV files for testing
        self.temp_dir = tempfile.TemporaryDirectory()
        # Set the environment variable to point to the temporary directory
        os.environ["OLYMPICS_DATA_PATH"] = self.temp_dir.name

    def tearDown(self):
        # Clean up the temporary directory
        self.temp_dir.cleanup()

    def test_get_year_from_filename(self):
        # Test extracting the year from filenames with different patterns
        filename1 = "Athens 2004 Olympics Nations Medals.csv"
        filename2 = "olympics_2012_data.csv"
        self.assertEqual(get_year_from_filename(filename1), 2004)
        self.assertEqual(get_year_from_filename(filename2), 2012)

    def test_load_datasets(self):
        # Create a temporary CSV file for testing
        test_file_path = os.path.join(self.temp_dir.name, "Athens 2004 Olympics Nations Medals.csv")
        with open(test_file_path, mode='w', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['NOC', 'Gold', 'Silver', 'Bronze', 'Total'])
            writer.writerow(['USA', '10', '5', '3', '18'])

        # Load datasets and check that the data is loaded correctly
        combined_df = load_datasets()

        # Assert that the DataFrame contains the expected number of rows
        self.assertEqual(len(combined_df), 1)

        # Assert the contents of the first row
        self.assertEqual(combined_df.iloc[0]['noc'], 'USA')
        self.assertEqual(combined_df.iloc[0]['gold'], 10)  # Check numeric value is correctly converted
        self.assertEqual(combined_df.iloc[0]['silver'], 5)
        self.assertEqual(combined_df.iloc[0]['bronze'], 3)
        self.assertEqual(combined_df.iloc[0]['total'], 18)
        self.assertEqual(combined_df.iloc[0]['year'], 2004)

    def test_upsert_olympics_medals_data(self):
        # Create a DataFrame for testing
        test_data = pd.DataFrame([
            {'noc': 'USA', 'gold': 10, 'silver': 5, 'bronze': 3, 'total': 18, 'year': 2004},
            {'noc': 'CHN', 'gold': 8, 'silver': 6, 'bronze': 4, 'total': 18, 'year': 2004},
        ])

        # Use a temporary SQLite database for testing
        db_url = "sqlite:///:memory:"
        engine = create_engine(db_url)

        # Create the olympics_medals table schema
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE olympics_medals (
                    nation TEXT NOT NULL,
                    year INTEGER NOT NULL,
                    gold INTEGER DEFAULT 0,
                    silver INTEGER DEFAULT 0,
                    bronze INTEGER DEFAULT 0,
                    total INTEGER DEFAULT 0
                );
            """))

        # Test upserting the data
        try:
            upsert_olympics_medals_data(test_data, engine)

            # Verify the data in the database
            result = pd.read_sql_table('olympics_medals', con=engine)
            self.assertEqual(len(result), 2)
            self.assertEqual(result[result['nation'] == 'USA']['gold'].iloc[0], 10)
            self.assertEqual(result[result['nation'] == 'CHN']['total'].iloc[0], 18)
        finally:
            engine.dispose()


if __name__ == "__main__":
    unittest.main()

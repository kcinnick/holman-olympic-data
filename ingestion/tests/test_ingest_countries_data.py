import os
import tempfile
import unittest

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from ingestion.ingest_countries_data import create_countries_dataframe, upsert_countries_data
from schemas.schemas import Countries


class TestIngestCountriesData(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Load environment variables from a .env file
        load_dotenv()

    def setUp(self):
        # Create a temporary directory to hold the test CSV file
        self.temp_dir = tempfile.TemporaryDirectory()
        # Set the environment variable to point to the temporary directory
        self.test_file_path = os.path.join(self.temp_dir.name, "test_countries.csv")
        os.environ["COUNTRIES_DATASET"] = self.test_file_path

        # Create a temporary SQLite database for testing
        self.db_url = "sqlite:///:memory:"
        self.engine = create_engine(self.db_url)

        # Create the countries table
        with self.engine.connect() as conn:
            conn.execute(text("""
            CREATE TABLE countries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                country TEXT NOT NULL,
                region TEXT,
                population INTEGER,
                area_sq_mi FLOAT,
                pop_density_per_sq_mi FLOAT,
                coastline_ratio FLOAT,
                net_migration FLOAT,
                infant_mortality_per_1000 FLOAT,
                gdp_per_capita INTEGER,
                literacy_percent FLOAT,
                phones_per_1000 FLOAT,
                arable_percent FLOAT,
                crops_percent FLOAT,
                other_percent FLOAT,
                climate FLOAT,
                birthrate FLOAT,
                deathrate FLOAT,
                agriculture FLOAT,
                industry FLOAT,
                service FLOAT
            );
            """))

    def tearDown(self):
        # Clean up temporary directory
        self.temp_dir.cleanup()

    def test_create_countries_dataframe(self):
        # Write test CSV data
        with open(self.test_file_path, mode='w', encoding='utf-8') as csvfile:
            csvfile.write(
                "country,region,population,area_(sq._mi.),pop._density_(per_sq._mi.),"
                "coastline_(coast/area_ratio),net_migration,infant_mortality_(per_1000_births),"
                "gdp_($_per_capita),literacy_(%),phones_(per_1000),arable_(%),crops_(%),other_(%),"
                "climate,birthrate,deathrate,agriculture,industry,service\n"
                "Testland,Test Region,123456,789.1,10.5,0.1,1.2,3.4,5000,99.9,500,10,5,85,1,20,10,0.1,0.2,0.7\n"
            )

        # Create a DataFrame from the CSV file
        df = create_countries_dataframe(self.test_file_path)

        # Verify DataFrame contents
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["country"], "Testland")
        self.assertAlmostEqual(df.iloc[0]["area_(sq._mi.)"], 789.1)

    def test_upsert_countries_data(self):
        # Create a DataFrame with test data
        test_data = pd.DataFrame([
            {
                "country": "Testland",
                "region": "Test Region",
                "population": 123456,
                "area_sq_mi": 789.1,
                "pop_density_per_sq_mi": 10.5,
                "coastline_ratio": 0.1,
                "net_migration": 1.2,
                "infant_mortality_per_1000": 3.4,
                "gdp_per_capita": 5000,
                "literacy_percent": 99.9,
                "phones_per_1000": 500,
                "arable_percent": 10,
                "crops_percent": 5,
                "other_percent": 85,
                "climate": 1,
                "birthrate": 20,
                "deathrate": 10,
                "agriculture": 0.1,
                "industry": 0.2,
                "service": 0.7,
            }
        ])

        # Upsert the data into the database
        upsert_countries_data(test_data, self.engine)

        # Verify the data in the database
        result = pd.read_sql_table("countries", con=self.engine)
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["country"], "Testland")
        self.assertEqual(result.iloc[0]["population"], 123456)


if __name__ == "__main__":
    unittest.main()

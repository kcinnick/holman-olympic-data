import os
import tempfile
import unittest

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

from ingestion.ingest_country_olympics_data import load_datasets, merge_data, upsert_country_olympics_data


class TestIngestCountryOlympicsData(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Load environment variables from a .env file
        load_dotenv()

        # Create a temporary SQLite database for testing
        cls.db_url = "sqlite:///:memory:"
        cls.engine = create_engine(cls.db_url)

    def setUp(self):
        # Define mock data using pandas DataFrames
        self.noc_mapping_df = pd.DataFrame({
            "noc_code": ["usa", "chn", "rus"],
            "country_name": ["United States", "China", "Russia"]
        })

        self.countries_df = pd.DataFrame({
            "id": [1, 2, 3],
            "country": ["United States", "China", "Russia"],
            "region": ["Americas", "Asia", "Europe"],
            "population": [331000000, 1439323776, 145912025]
        })

        self.olympics_medals_df = pd.DataFrame({
            "nation": ["usa", "chn", "rus"],
            "year": [2004, 2004, 2004],
            "gold": [36, 32, 28],
            "silver": [39, 17, 26],
            "bronze": [26, 15, 36],
            "total": [101, 64, 90]
        })

    def test_load_datasets(self):
        # Mock load_datasets to return the predefined DataFrames
        def mock_load_datasets(engine):
            return {
                "noc_mapping": self.noc_mapping_df,
                "countries": self.countries_df,
                "olympics_medals": self.olympics_medals_df
            }

        dataframes = mock_load_datasets(self.engine)

        # Verify data is loaded correctly
        self.assertEqual(len(dataframes['noc_mapping']), 3)
        self.assertEqual(len(dataframes['countries']), 3)
        self.assertEqual(len(dataframes['olympics_medals']), 3)

    def test_merge_data(self):
        # Mock load_datasets to return the predefined DataFrames
        def mock_load_datasets(engine):
            return {
                "noc_mapping": self.noc_mapping_df,
                "countries": self.countries_df,
                "olympics_medals": self.olympics_medals_df
            }

        dataframes = mock_load_datasets(self.engine)
        merged_df = merge_data(dataframes)

        # Verify merged data
        self.assertEqual(len(merged_df), 3)
        self.assertIn('country_id', merged_df.columns)
        self.assertIn('total_medals', merged_df.columns)

    def test_upsert_country_olympics_data(self):
        # Mock load_datasets to return the predefined DataFrames
        def mock_load_datasets(engine):
            return {
                "noc_mapping": self.noc_mapping_df,
                "countries": self.countries_df,
                "olympics_medals": self.olympics_medals_df
            }

        dataframes = mock_load_datasets(self.engine)
        merged_df = merge_data(dataframes)
        upsert_country_olympics_data(merged_df, self.engine)

        # Verify data in the country_olympics table
        result = pd.read_sql_table("country_olympics", con=self.engine)
        self.assertEqual(len(result), 3)

        # Validate specific row data
        usa_row = result[result['country_name'] == 'united states']
        self.assertFalse(usa_row.empty, "Row for 'united states' is missing")
        self.assertEqual(usa_row['total_medals'].iloc[0], 101)

    @classmethod
    def tearDownClass(cls):
        # Dispose of the engine after tests
        cls.engine.dispose()


if __name__ == "__main__":
    unittest.main()

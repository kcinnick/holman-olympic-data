import csv
import os
import tempfile
import unittest

from dotenv import load_dotenv

from ingestion.ingest_olympics_medals_data import get_year_from_filename, load_datasets, create_olympics_medals_entry
from schemas.olympics_medals_schema import OlympicsMedals


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

        datasets_path = self.temp_dir.name
        # Load datasets and check that the data is loaded correctly
        datasets = list(load_datasets(datasets_path))
        self.assertEqual(len(datasets), 1)
        row, year = datasets[0]
        self.assertEqual(year, 2004)
        self.assertEqual(row['NOC'], 'USA')
        self.assertEqual(int(row['Gold']), 10)

    def test_create_olympics_medals_entry(self):
        # Test creating an OlympicsMedals entry from a row
        row = {
            'NOC': 'USA',
            'Gold': '10',
            'Silver': '5',
            'Bronze': '3',
            'Total': '18'
        }
        year = 2004
        entry = create_olympics_medals_entry(row, year)
        self.assertIsInstance(entry, OlympicsMedals)
        self.assertEqual(entry.nation, 'USA')
        self.assertEqual(entry.year, 2004)
        self.assertEqual(entry.gold, 10)
        self.assertEqual(entry.silver, 5)
        self.assertEqual(entry.bronze, 3)
        self.assertEqual(entry.total, 18)


if __name__ == "__main__":
    unittest.main()

import unittest
import os
import tempfile
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from schemas.countries_schema import Base, Countries


class TestCountriesSchema(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Load environment variables from a .env file
        load_dotenv()
        # Set up an in-memory SQLite database for testing
        cls.db_url = "sqlite:///:memory:"
        cls.engine = create_engine(cls.db_url, echo=True)
        Base.metadata.create_all(cls.engine)
        cls.Session = sessionmaker(bind=cls.engine)

    def setUp(self):
        # Create a new session for each test
        self.session = self.Session()

    def tearDown(self):
        # Rollback and close the session after each test
        self.session.rollback()
        self.session.close()

    def test_table_creation(self):
        # Use the inspector to verify that the table was actually created
        inspector = inspect(self.engine)
        tables = inspector.get_table_names()
        self.assertIn('countries', tables, "Table 'countries' should be created in the database.")

    def test_insert_country(self):
        # Insert a country entry into the database
        country = Countries(
            country="Cortland",
            region="Upstate New York",
            population=20000,
            area_sq_mi=50000.0,
            pop_density_per_sq_mi=20.0,
            coastline_ratio=0.1,
            net_migration=1.2,
            infant_mortality_per_1000=10.5,
            gdp_per_capita=15000,
            literacy_percent=95.5,
            phones_per_1000=300.0,
            arable_percent=25.0,
            crops_percent=5.0,
            other_percent=70.0,
            climate=2.0,
            birthrate=12.5,
            deathrate=8.5,
            agriculture=0.15,
            industry=0.25,
            service=0.60
        )
        self.session.add(country)
        self.session.commit()

        # Query the database to ensure the entry was inserted
        result = self.session.query(Countries).filter_by(country="Cortland").first()
        self.assertIsNotNone(result, "Country 'Cortland' should be inserted into the database.")
        self.assertEqual(result.country, "Cortland")
        self.assertEqual(result.population, 20000)

        # remove the country entry
        self.session.delete(result)
        self.session.commit()

    def test_query_all_countries(self):
        # Insert multiple country entries into the database
        country1 = Countries(country="Cortland", region="Upstate New York", population=20000)
        country2 = Countries(country="Homer", region="RegionB", population=10000)
        self.session.add_all([country1, country2])
        self.session.commit()

        # Query the database to ensure all entries were inserted
        results = self.session.query(Countries).all()
        self.assertEqual(len(results), 2, "There should be two countries in the database.")
        self.assertEqual(results[0].country, "Cortland")
        self.assertEqual(results[1].country, "Homer")


if __name__ == "__main__":
    unittest.main()

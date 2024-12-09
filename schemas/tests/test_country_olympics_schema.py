import unittest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from schemas.base import Base
from schemas.schemas import Countries, OlympicsMedals, CountryOlympics


class TestCountryOlympicsSchema(unittest.TestCase):
    def setUp(self):
        # Create a temporary SQLite database for testing
        self.db_url = "sqlite:///:memory:"
        self.engine = create_engine(self.db_url)
        Base.metadata.create_all(self.engine)  # Create all tables

        # Create a session
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

        # Insert test data into Countries and OlympicsMedals tables
        self.country = Countries(id=1, country="USA", region="North America", population=331002651)
        self.olympics_medal = OlympicsMedals(
            id=1, nation="USA", year=2020, gold=39, silver=41, bronze=33, total=113
        )

        self.session.add(self.country)
        self.session.add(self.olympics_medal)
        self.session.commit()

    def tearDown(self):
        self.session.close()

    def test_country_olympics_insert(self):
        # Insert a record into the CountryOlympics table
        country_olympics_record = CountryOlympics(
            country_id=self.country.id,
            olympics_medals_id=self.olympics_medal.id,
            total_medals=113,
            first_year=2020,
            avg_medals_per_year=113,
            total_gold=39,
            total_silver=41,
            total_bronze=33,
            years_participated=1,
        )
        self.session.add(country_olympics_record)
        self.session.commit()

        # Retrieve the record from the database
        result = self.session.query(CountryOlympics).first()

        self.assertIsNotNone(result)
        self.assertEqual(result.country_id, self.country.id)
        self.assertEqual(result.olympics_medals_id, self.olympics_medal.id)

        # Check relationships
        self.assertEqual(result.country.country, "USA")
        self.assertEqual(result.olympics_medals.nation, "USA")


if __name__ == "__main__":
    unittest.main()

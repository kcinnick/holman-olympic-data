import unittest
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from schemas.schemas import Base


class TestOlympicsSchema(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Load environment variables from a .env file to get DB credentials and other settings
        load_dotenv()

        # Set up a temporary in-memory SQLite database for testing
        cls.db_url = "sqlite:///:memory:"
        cls.engine = create_engine(cls.db_url, echo=True)

        # Create all tables for testing
        Base.metadata.create_all(cls.engine)

        # Set up session maker
        cls.Session = sessionmaker(bind=cls.engine)

    def setUp(self):
        # Create a new session for each test
        self.session = self.Session()

    def tearDown(self):
        # Rollback and close the session after each test
        self.session.rollback()
        self.session.close()

    def test_table_exists(self):
        # Check if the 'olympics_medals' table exists in the test database
        inspector = inspect(self.engine)
        tables = inspector.get_table_names()
        self.assertIn('olympics_medals', tables, "Table 'olympics_medals' should exist in the database.")

    def test_column_definitions(self):
        # Check if the columns are correctly defined
        inspector = inspect(self.engine)
        columns = inspector.get_columns('olympics_medals')
        column_names = {column['name']: column['type'].__class__.__name__ for column in columns}
        expected_columns = {
            'id': 'INTEGER',
            'nation': 'VARCHAR',
            'year': 'INTEGER',
            'gold': 'INTEGER',
            'silver': 'INTEGER',
            'bronze': 'INTEGER',
            'total': 'INTEGER',
        }
        for col_name, col_type in expected_columns.items():
            self.assertIn(col_name, column_names, f"Column '{col_name}' should exist in the table.")
            self.assertEqual(column_names[col_name], col_type, f"Column '{col_name}' should be of type '{col_type}'.")


if __name__ == "__main__":
    unittest.main()

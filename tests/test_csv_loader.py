# Tests for csv_loader.py
# Run with: py tests/test_csv_loader.py

import sqlite3
import pandas as pd
import unittest
import tempfile
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loader.csv_loader import load_csv, insert_rows
from schema.schema_manager import create_table_from_df


# Tests for load_csv()

class TestLoadCSV(unittest.TestCase):

    def test_reads_file_correctly(self):
        # Create a temporary CSV file and check it loads with right row count
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as tmp:
            tmp.write("Description,Year,Total_Population\nAlabama,2020,5024279\nAlaska,2020,733391\n")
            tmp_path = tmp.name

        try:
            df = load_csv(tmp_path)
            self.assertEqual(len(df), 2)
            self.assertIn("Description", df.columns)
        finally:
            os.remove(tmp_path) # Clean up the temp file

    def test_fixes_spaces_in_column_names(self):
        # Spaces in column names must become underscores so SQL doesn't break
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as tmp:
            tmp.write("Total Population,White Alone\n1000,800\n")
            tmp_path = tmp.name

        try:
            df = load_csv(tmp_path)
            self.assertIn("Total_Population", df.columns)
            self.assertIn("White_Alone", df.columns)
        finally:
            os.remove(tmp_path)

    def test_raises_error_for_missing_file(self):
        # Should raise FileNotFoundError if path doesn't exist
        with self.assertRaises(FileNotFoundError):
            load_csv("/nonexistent/path/file.csv")


# Tests for insert_rows()

class TestInsertRows(unittest.TestCase):

    def setUp(self):
        # Fresh empty in-memory database and disappears after each test
        self.mem_db = sqlite3.connect(":memory:")

        # Fake sample version of my population CSV file just to test
        self.pop_df = pd.DataFrame({
            "Description":      ["U.S.", "Alabama", "Alaska"],
            "Year":             [2020, 2020, 2020],
            "Total_Population": [331449281, 5024279, 733391],
            "Hispanic":         [62080044, 228669, 55279],
        })

        # Database that already has population table loaded with 3 rows
        create_table_from_df(self.mem_db, "population_by_race", self.pop_df)
        insert_rows(self.mem_db, "population_by_race", self.pop_df)

    def tearDown(self):
        # Closes the database after the test finishes
        self.mem_db.close()

    def test_correct_row_count(self):
        # Row count in database should match DataFrame
        cursor = self.mem_db.cursor()
        cursor.execute("SELECT COUNT(*) FROM population_by_race;")
        self.assertEqual(cursor.fetchone()[0], len(self.pop_df))

    def test_correct_values_stored(self):
        # Check actual data was stored correctly
        cursor = self.mem_db.cursor()
        cursor.execute("SELECT Total_Population FROM population_by_race WHERE Description = 'U.S.'")
        row = cursor.fetchone()
        
        self.assertIsNotNone(row)
        self.assertEqual(row[0], 331449281)

    def test_handles_null_values(self):
        # NaN in pandas should become NULL in SQLite without crashing since some race columns are missing data for earlier years in CSV file
        df = pd.DataFrame({
            "Description": ["Test"],
            "Year":        [2020],
            "Value":       [float("nan")],
        })
        create_table_from_df(self.mem_db, "null_test", df)
        count = insert_rows(self.mem_db, "null_test", df)
        self.assertEqual(count, 1)


# Can run file directly from terminal
if __name__ == '__main__':
    unittest.main()
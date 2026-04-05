# Tests for schema/schema_manager.py
# Run with: py tests/test_schema_manager.py

import sqlite3
import pandas as pd
import unittest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loader.csv_loader import insert_rows
from schema.schema_manager import (
    create_table_from_df, get_table_schema, list_tables,
    schemas_match, describe_schema, pandas_dtype_to_sql
)

class TestSchemaManager(unittest.TestCase):

    def setUp(self):
        # Empty in-memory database
        self.mem_db = sqlite3.connect(":memory:")

        # Fake sample version of my population CSV file just to test
        self.pop_df = pd.DataFrame({
            "Description":      ["U.S.", "Alabama", "Alaska"],
            "Year":             [2020, 2020, 2020],
            "Total_Population": [331449281, 5024279, 733391],
            "Hispanic":         [62080044, 228669, 55279],
        })

    def tearDown(self):
        self.mem_db.close()

    def load_population_data(self):
        # Database that already has population table loaded with 3 rows
        create_table_from_df(self.mem_db, "population_by_race", self.pop_df)
        insert_rows(self.mem_db, "population_by_race", self.pop_df)


    # Tests for list_tables()

    def test_empty_database_has_no_tables(self):
        self.assertEqual(list_tables(self.mem_db), [])

    def test_table_appears_after_create(self):
        create_table_from_df(self.mem_db, "population_by_race", self.pop_df)
        self.assertIn("population_by_race", list_tables(self.mem_db))


    # Tests for get_table_schema()

    def test_includes_id_column(self):
        self.load_population_data()
        schema = get_table_schema(self.mem_db, "population_by_race")
        names = [col["name"] for col in schema]
        self.assertIn("id", names)

    def test_includes_all_csv_columns(self):
        # Every column from DataFrame would appear in schema
        self.load_population_data()
        schema = get_table_schema(self.mem_db, "population_by_race")
        names = [col["name"] for col in schema]
        for col in self.pop_df.columns:
            self.assertIn(col, names)

    def test_raises_error_for_nonexistent_table(self):
        with self.assertRaisesRegex(ValueError, "does not exist"):
            get_table_schema(self.mem_db, "ghost_table")


    # Tests for schemas_match()

    def test_same_structure_returns_true(self):
        # Same columns and types -> match -> safe to append rows
        self.load_population_data()
        schema = get_table_schema(self.mem_db, "population_by_race")
        self.assertTrue(schemas_match(schema, self.pop_df))

    def test_different_columns_returns_false(self):
        # Different columns -> no match -> need to create a new table
        self.load_population_data()
        schema = get_table_schema(self.mem_db, "population_by_race")
        different_df = pd.DataFrame({"product": ["Widget"], "price": [9.99]})
        self.assertFalse(schemas_match(schema, different_df))


    # Tests for describe_schema()

    def test_empty_database_says_empty(self):
        result = describe_schema(self.mem_db)
        self.assertIn("empty", result.lower())

    def test_shows_table_name_and_columns(self):
        self.load_population_data()
        result = describe_schema(self.mem_db)
        self.assertIn("population_by_race", result)
        self.assertIn("Description", result)


# Tests for pandas_dtype_to_sql()

class TestPandasDtypeToSQL(unittest.TestCase):

    def test_int_maps_to_integer(self):
        self.assertEqual(pandas_dtype_to_sql("int64"), "INTEGER")

    def test_float_maps_to_real(self):
        self.assertEqual(pandas_dtype_to_sql("float64"), "REAL")

    def test_string_maps_to_text(self):
        self.assertEqual(pandas_dtype_to_sql("object"), "TEXT")

    def test_unknown_type_defaults_to_text(self):
        self.assertEqual(pandas_dtype_to_sql("something_unknown"), "TEXT")


if __name__ == '__main__':
    unittest.main()
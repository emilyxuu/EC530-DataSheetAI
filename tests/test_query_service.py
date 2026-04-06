# Tests for query_service/query_service.py
# Run with: py tests/test_query_service.py

import sqlite3
import pandas as pd
import unittest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loader.csv_loader import insert_rows
from schema.schema_manager import create_table_from_df
from query_service.query_service import QueryService


class BaseDBSetup(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        df = pd.DataFrame({
            "Description":      ["U.S.", "Alabama", "Alaska"],
            "Year":             [2020, 2020, 2020],
            "Total_Population": [331449281, 5024279, 733391],
            "Hispanic":         [62080044, 228669, 55279],
        })
        create_table_from_df(self.conn, "population_by_race", df)
        insert_rows(self.conn, "population_by_race", df)

    def tearDown(self):
        self.conn.close()


# Fake LLMs used in tests that replace the real Claude so tests need no API key for now

class MockLLM:
    # Returns a known safe SQL query that we can test the full pipeline with
    def translate(self, question):
        return (
            "SELECT Description, Total_Population "
            "FROM population_by_race "
            "ORDER BY Total_Population DESC LIMIT 1"
        )

class BadLLM:
    # Returns dangerous SQL that should be blocked by the validator
    def translate(self, question):
        return "DROP TABLE population_by_race"


# Tests for run_sql()

class TestRunSQL(BaseDBSetup):

    def test_valid_query_returns_success(self):
        svc = QueryService(conn=self.conn)
        result = svc.run_sql(
            "SELECT Description, Total_Population FROM population_by_race ORDER BY Total_Population DESC"
        )
        self.assertTrue(result["success"])
        self.assertEqual(result["columns"], ["Description", "Total_Population"])
        self.assertEqual(len(result["rows"]), 3)
        self.assertEqual(result["rows"][0][0], "U.S.")  # largest population first

    def test_invalid_query_returns_error(self):
        svc = QueryService(conn=self.conn)
        result = svc.run_sql("DROP TABLE population_by_race")
        self.assertFalse(result["success"])
        self.assertNotEqual(result["error"], "")

    def test_columns_match_what_was_selected(self):
        svc = QueryService(conn=self.conn)
        result = svc.run_sql("SELECT Description, Hispanic FROM population_by_race")
        self.assertEqual(result["columns"], ["Description", "Hispanic"])

    def test_no_matching_rows_returns_empty_list(self):
        # A valid query that finds nothing should succeed but return an empty list of rows
        svc = QueryService(conn=self.conn)
        result = svc.run_sql("SELECT Description FROM population_by_race WHERE Year = 1850")
        self.assertTrue(result["success"])
        self.assertEqual(result["rows"], [])


# Tests for ask()

class TestAsk(BaseDBSetup):

    def test_ask_without_llm_returns_clear_error(self):
        # If no LLM is configured, ask() should fail with a useful message
        svc = QueryService(conn=self.conn, llm_adapter=None)
        result = svc.ask("Which state has the most people?")
        self.assertFalse(result["success"])
        self.assertTrue("LLM" in result["error"] or "llm" in result["error"].lower())

    def test_ask_with_mock_llm_returns_correct_data(self):
        # Full pipeline: question -> MockLLM -> validator -> database -> result
        svc = QueryService(conn=self.conn, llm_adapter=MockLLM())
        result = svc.ask("Which place has the largest population?")
        self.assertTrue(result["success"])
        self.assertEqual(result["rows"][0][0], "U.S.")
        self.assertEqual(result["rows"][0][1], 331449281)

    def test_dangerous_llm_sql_is_blocked(self):
        # Even if the LLM returns dangerous SQL, the validator must stop it
        svc = QueryService(conn=self.conn, llm_adapter=BadLLM())
        result = svc.ask("Delete everything")
        self.assertFalse(result["success"])

        # Table should still exist with all 3 rows — DROP never ran
        check = svc.run_sql("SELECT COUNT(*) FROM population_by_race")
        self.assertTrue(check["success"])
        self.assertEqual(check["rows"][0][0], 3)


# Tests for list_tables()

class TestListTables(BaseDBSetup):

    def test_returns_loaded_table(self):
        svc = QueryService(conn=self.conn)
        self.assertIn("population_by_race", svc.list_tables())


if __name__ == '__main__':
    unittest.main()
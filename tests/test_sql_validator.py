# Tests for query_service/sql_validator.py
# Run with: py tests/test_sql_validator.py

import sqlite3
import pandas as pd
import unittest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loader.csv_loader import insert_rows
from schema.schema_manager import create_table_from_df
from query_service.sql_validator import validate_query


class BaseDBSetup(unittest.TestCase):
    """This handles the setup for all test classes below so we don't repeat code."""
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        
        # Build a small population table to test queries against
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


# Allowed queries

class TestValidQueries(BaseDBSetup):

    def test_select_star(self):
        ok, _ = validate_query("SELECT * FROM population_by_race", self.conn)
        self.assertTrue(ok)

    def test_select_specific_columns(self):
        ok, _ = validate_query("SELECT Description, Year FROM population_by_race", self.conn)
        self.assertTrue(ok)

    def test_select_with_where(self):
        ok, _ = validate_query(
            "SELECT Description FROM population_by_race WHERE Year = 2020",
            self.conn
        )
        self.assertTrue(ok)

    def test_select_with_order_and_limit(self):
        ok, _ = validate_query(
            "SELECT Description FROM population_by_race ORDER BY Total_Population DESC LIMIT 3",
            self.conn
        )
        self.assertTrue(ok)


# Blocked queries

class TestBlockedQueries(BaseDBSetup):

    def test_rejects_empty_string(self):
        ok, _ = validate_query("", self.conn)
        self.assertFalse(ok)

    def test_rejects_whitespace_only(self):
        ok, _ = validate_query("   ", self.conn)
        self.assertFalse(ok)

    def test_rejects_drop(self):
        ok, _ = validate_query("DROP TABLE population_by_race", self.conn)
        self.assertFalse(ok)

    def test_rejects_delete(self):
        ok, _ = validate_query("DELETE FROM population_by_race", self.conn)
        self.assertFalse(ok)

    def test_rejects_insert(self):
        ok, _ = validate_query("INSERT INTO population_by_race VALUES (1)", self.conn)
        self.assertFalse(ok)

    def test_rejects_update(self):
        ok, _ = validate_query("UPDATE population_by_race SET Total_Population=0", self.conn)
        self.assertFalse(ok)

    def test_rejects_pragma(self):
        # PRAGMA can change database settings — must be blocked
        ok, _ = validate_query("PRAGMA journal_mode=DELETE", self.conn)
        self.assertFalse(ok)

    def test_rejects_line_comment(self):
        # -- is used in SQL injection attacks to hide extra code
        ok, _ = validate_query("SELECT * FROM population_by_race -- comment", self.conn)
        self.assertFalse(ok)

    def test_rejects_block_comment(self):
        # /* */ is another comment style used in injection attacks
        ok, _ = validate_query("SELECT /* bad */ * FROM population_by_race", self.conn)
        self.assertFalse(ok)

    def test_rejects_multiple_statements(self):
        # Semicolon in the middle means two commands chained together
        ok, _ = validate_query(
            "SELECT * FROM population_by_race; DROP TABLE population_by_race",
            self.conn
        )
        self.assertFalse(ok)

    def test_rejects_null_byte(self):
        # Null bytes can trick string parsers
        ok, _ = validate_query("SELECT * FROM population_by_race\x00", self.conn)
        self.assertFalse(ok)

    def test_rejects_unknown_table(self):
        ok, msg = validate_query("SELECT * FROM ghost_table", self.conn)
        self.assertFalse(ok)
        self.assertIn("ghost_table", msg)  # error message should name the bad table


# LLM hallucination demo according to assignment requirements
#
# Steps I took:
#   I asked Claude: "Show me the population breakdown for each state in 2020"
#   Claude generated: SELECT state_name, Total_Population FROM population_by_race
#
#   "state_name" does not exist; the real column is "Description".
#   The test below caught this. I then updated the LLM prompt to say
#   "Description is the place name", which fixed the hallucination.

class TestLLMHallucination(BaseDBSetup):

    def test_catches_hallucinated_column(self):
        # LLM invented "state_name" but real column is "Description"
        fake_sql = "SELECT state_name, Total_Population FROM population_by_race WHERE Year = 2020"
        ok, msg = validate_query(fake_sql, self.conn)
        self.assertFalse(ok)
        self.assertTrue("state_name" in msg.lower() or "column" in msg.lower())

    def test_catches_hallucinated_table(self):
        # LLM used "population" but real table is "population_by_race"
        fake_sql = "SELECT Description FROM population WHERE Year = 2020"
        ok, msg = validate_query(fake_sql, self.conn)
        self.assertFalse(ok)
        self.assertIn("population", msg)


if __name__ == '__main__':
    unittest.main()
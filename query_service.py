import sqlite3
import logging
from typing import Any

logger = logging.getLogger(__name__)

class QueryService:
    # This class takes a user's question and makes sure it runs safely on database

    def __init__(self, conn: sqlite3.Connection, llm_adapter=None):
        # Store database connection and AI 'translator'
        self.conn = conn
        self.llm_adapter = llm_adapter

    def ask(self, user_question: str) -> dict[str, Any]:
        # Takes a plain English question and turns it into data
        # Safety check: Make sure AI part is set up
        if self.llm_adapter is None:
            return self._error("", "LLM Adapter is not configured.")

        try:
            # Send the English question to AI to get SQL back
            sql = self.llm_adapter.translate(user_question)
        except Exception as e:
            logger.error(f"LLM failed: {e}")
            return self._error("", f"The AI couldn't understand that: {e}")

        # Validate SQL and then run it
        return self._validate_and_run(sql)

    def run_sql(self, sql: str) -> dict[str, Any]:
        # Use this if the user types a SQL command directly instead of English
        return self._validate_and_run(sql)

    def list_tables(self) -> list[str]:
        # Uses schema_manager file to see what tables we have
        from schema.schema_manager import list_tables
        return list_tables(self.conn)

    def _validate_and_run(self, sql: str) -> dict[str, Any]:
        # Checks if SQL is safe before running it
        from query_service.sql_validator import validate_query

        # Check if query is allowed (e.g., no 'DELETE' commands)
        is_valid, reason = validate_query(sql, self.conn)
        if not is_valid:
            return self._error(sql, f"Query rejected: {reason}")

        try:
            # If safe, run query on database
            cursor = self.conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            
            # Get column names so user knows what the numbers mean
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            
            # Return a 'Success Package' with all info
            return {
                "success": True, 
                "sql": sql, 
                "columns": columns, 
                "rows": rows, 
                "error": ""
            }

        except sqlite3.Error as e:
            logger.error(f"Database error: {e}")
            return self._error(sql, f"Database error: {e}")

    @staticmethod
    def _error(sql: str, message: str) -> dict[str, Any]:
        # Helper to send back a consistent 'Error Package' if something breaks
        return {
            "success": False, 
            "sql": sql, 
            "columns": [], 
            "rows": [], 
            "error": message
        }
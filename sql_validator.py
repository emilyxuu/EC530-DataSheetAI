import re
import sqlite3
import logging

logger = logging.getLogger(__name__)

def validate_query(sql: str, conn: sqlite3.Connection) -> tuple[bool, str]:
    # Checks if SQL command is safe and runs through tests
    # 'False' and stops if test fails, 'True' if all tests pass
    
    # A list of functions/tests to run in order
    checks = [
        lambda: _check_not_empty(sql),
        lambda: _check_no_dangerous_chars(sql),
        lambda: _check_only_select(sql),             # Prevents modifying data
        lambda: _check_no_multiple_statements(sql),  # Prevents chaining commands
        lambda: _check_tables_exist(sql, conn),      # Checks if table is real
        lambda: _check_columns_exist(sql, conn),     # Checks if columns are real
        lambda: _check_sqlite_syntax(sql, conn),     # Asks SQLite if grammar is correct
    ]

    # Run each test one by one
    for check in checks:
        is_safe, message = check()
        if not is_safe:
            logger.warning(f"Validator rejected query: {message}")
            return False, message

    return True, "OK"

# Test 1: Did user/AI actually send anything?
def _check_not_empty(sql: str) -> tuple[bool, str]:
    if not sql or not sql.strip():
        return False, "Query is empty."
    return True, "OK"

# Test 2: Blocks characters often used in 'SQL Injection' attacks to prevention in bad code
def _check_no_dangerous_chars(sql: str) -> tuple[bool, str]:
    if "\x00" in sql: # Null byte
        return False, "Query contains illegal characters (null bytes)."
    if "--" in sql or "/*" in sql or "*/" in sql: # SQL Comments
        return False, "Query contains comments, which are blocked for security."
    return True, "OK"

# Test 3: Ensures query only reads data (SELECT) and never changes it (DROP, DELETE)
def _check_only_select(sql: str) -> tuple[bool, str]:
    # The very first word must be SELECT
    first_word = sql.strip().split()[0].upper()
    if first_word != "SELECT":
        return False, f"Only SELECT queries are allowed. Got: '{first_word}'."

    # Look for forbidden words hidden anywhere in code
    forbidden_words = [
        "INSERT", "UPDATE", "DELETE", "DROP", "CREATE",
        "ALTER", "REPLACE", "TRUNCATE", "ATTACH", "DETACH", "PRAGMA",
    ]
    for keyword in forbidden_words:
        # re.search looks for exact word (so 'SELECTED' is fine, but 'SELECT' is caught)
        if re.search(rf"\b{keyword}\b", sql.upper()):
            return False, f"Forbidden keyword found: '{keyword}'."

    return True, "OK"

# Test 4: This blocks semicolons in the middle of a command
def _check_no_multiple_statements(sql: str) -> tuple[bool, str]:
    # Remove a normal semicolon at the very end, then check if any are left
    if ";" in sql.strip().rstrip(";"):
        return False, "Multiple statements are not allowed."
    return True, "OK"

# Functions to extract names from SQL string

 # Looks for words coming immediately after 'FROM' or 'JOIN'
def _extract_tables(sql: str) -> list[str]:
    matches = re.findall(r'\b(?:FROM|JOIN)\s+["`]?(\w+)["`]?', sql, re.IGNORECASE)
    return [m.lower() for m in matches]

# Looks for words between 'SELECT' and 'FROM'
def _extract_columns(sql: str) -> list[str]:
    match = re.search(r'\bSELECT\s+(.*?)\s+FROM\b', sql, re.IGNORECASE | re.DOTALL)
    if not match:
        return []

    clause = match.group(1).strip()
    if clause == "*":
        return []  # Can't check specific columns if they use *

    columns = []
    # Break apart column list (e.g., 'name, age')
    for part in clause.split(","):
        part = part.strip()
        if not part or re.match(r'\w+\s*\(', part):  # Skip complex math like COUNT()
            continue
        col = part.split()[0].strip('"`\'')
        if col and col.isidentifier():
            columns.append(col.lower())
    return columns


# Test 5: Does AI's requested table actually exist in database?
def _check_tables_exist(sql: str, conn: sqlite3.Connection) -> tuple[bool, str]:
    from schema.schema_manager import list_tables
    existing_tables = [t.lower() for t in list_tables(conn)]
    
    for table in _extract_tables(sql):
        if table not in existing_tables:
            return False, f"Table '{table}' does not exist. Available: {existing_tables}"
    return True, "OK"

# Test 6: Do AI's requested columns exist in those tables?
def _check_columns_exist(sql: str, conn: sqlite3.Connection) -> tuple[bool, str]:
    from schema.schema_manager import get_table_schema

    tables  = _extract_tables(sql)
    columns = _extract_columns(sql)

    if not columns or not tables:
        return True, "OK" 

    # Make a list of all real columns in requested tables
    known_columns = set()
    for table in tables:
        try:
            for col in get_table_schema(conn, table):
                known_columns.add(col["name"].lower())
        except ValueError:
            pass  # If table doesn't exist, Test 5 already caught it

    # Check if the requested columns are in the 'real' list
    for col in columns:
        if col not in known_columns:
            return False, f"Column '{col}' does not exist. Known columns: {sorted(known_columns)}"

    return True, "OK"

# Test 7: Uses 'EXPLAIN' to ask the database to read the code without actually running it
def _check_sqlite_syntax(sql: str, conn: sqlite3.Connection) -> tuple[bool, str]:
    try:
        conn.execute(f"EXPLAIN {sql}")
        return True, "OK"
    except sqlite3.Error as e:
        return False, f"SQL syntax error: {e}"
import sqlite3
import pandas as pd
import logging

logger = logging.getLogger(__name__)

# Translates Python/Pandas types into SQL types
# Example: If Pandas sees a whole number (int64), SQL calls it an "integer"
DTYPE_MAP = {
    "int64":           "INTEGER",
    "int32":           "INTEGER",
    "float64":         "REAL",
    "float32":         "REAL",
    "bool":            "INTEGER", # SQL uses 0 or 1 for true/false
    "object":          "TEXT", # Pandas calls text "object"
    "datetime64[ns]":  "TEXT",
}

def pandas_dtype_to_sql(dtype) -> str: # Picks the right SQL type and defaults to text
    return DTYPE_MAP.get(str(dtype), "TEXT")

def list_tables(conn: sqlite3.Connection) -> list[str]: # Checks database and returns a list of all table names
    cursor = conn.cursor()
    # 'sqlite_master' is the database's Table of Contents
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
    return [row[0] for row in cursor.fetchall()]

def get_table_schema(conn: sqlite3.Connection, table_name: str) -> list[dict]:
    # What columns does this specific table have
    if table_name not in list_tables(conn):
        raise ValueError(f"Table '{table_name}' does not exist.")

    cursor = conn.cursor()
    # PRAGMA is a special command to get info about table columns
    cursor.execute(f'PRAGMA table_info("{table_name}");')
    return [{"name": row[1], "type": row[2]} for row in cursor.fetchall()]

def create_table_from_df(conn: sqlite3.Connection, table_name: str, df: pd.DataFrame) -> None:
    # Builds a new table based on the columns in the CSV file.
    # Every table gets an id column that counts up automatically
    column_defs = ["id INTEGER PRIMARY KEY AUTOINCREMENT"]

    # Loop through CSV columns and find matching SQL type for each
    for col_name, dtype in zip(df.columns, df.dtypes):
        sql_type = pandas_dtype_to_sql(dtype)
        column_defs.append(f'"{col_name}" {sql_type}')

    # Create final SQL command: CREATE TABLE "my_data" (id, name, age...)
    create_sql = f'CREATE TABLE "{table_name}" ({", ".join(column_defs)});'

    try:
        conn.execute(create_sql)
        conn.commit()
        logger.info(f"Created table '{table_name}'.")
    except sqlite3.Error as e:
        logger.error(f"Could not create table: {e}")
        raise

def schemas_match(existing_schema: list[dict], df: pd.DataFrame) -> bool:
    # Checks if a new CSV file matches a table that's already in the database
    # List columns in existing table (ignoring id column)
    existing = {
        col["name"].lower(): col["type"].upper()
        for col in existing_schema
        if col["name"].lower() != "id"
    }

    # List columns in new CSV
    incoming = {
        col.lower(): pandas_dtype_to_sql(dtype).upper()
        for col, dtype in zip(df.columns, df.dtypes)
    }

    # If names or types are different, they don't match
    if set(existing.keys()) != set(incoming.keys()):
        return False

    for col_name in incoming:
        if existing[col_name] != incoming[col_name]:
            return False

    return True

def describe_schema(conn: sqlite3.Connection) -> str:
    # Creates a text summary of the whole database to show to the AI (LLM)
    tables = list_tables(conn)

    if not tables:
        return "The database is empty."

    lines = []
    for table in tables:
        lines.append(f"Table: {table}")
        for col in get_table_schema(conn, table):
            lines.append(f"  - {col['name']} ({col['type']})")
        lines.append("")

    return "\n".join(lines).strip()
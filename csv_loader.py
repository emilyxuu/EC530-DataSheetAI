"""
csv_loader.py:
1. load_csv()    — reads a CSV file and returns it as a pandas DataFrame
2. insert_rows() — inserts that data into SQLite, one row at a time

df.to_sql() is forbidden by the assignment, so I build the INSERT statement.
"""

import sqlite3
import pandas as pd
import logging

# Write log messages to error_log.txt and also print them to the terminal
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("error_log.txt"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Read a CSV file from disk and return a pandas DataFrame.
def load_csv(filepath: str) -> pd.DataFrame:
    
    try:
        df = pd.read_csv(filepath)
    except FileNotFoundError:
        logger.error(f"File not found: {filepath}")
        raise
    except Exception as e:
        logger.error(f"Could not read '{filepath}': {e}")
        raise ValueError(f"Could not parse CSV: {e}")

    if df.empty:
        raise ValueError(f"'{filepath}' is empty — nothing to load.")

    # Replace spaces in column names with underscores so SQL doesn't break
    df.columns = [col.strip().replace(" ", "_") for col in df.columns] # Example: "Emily Xu" becomes "Emily_Xu"

    logger.info(f"Loaded '{filepath}': {len(df):,} rows, columns: {list(df.columns)}")
    return df


# Insert every row from a DataFrame into an existing SQLite table and returns the number of rows inserted
def insert_rows(conn: sqlite3.Connection, table_name: str, df: pd.DataFrame) -> int: 
    
    cursor = conn.cursor()

    # "col1", "col2", "col3"
    columns = ", ".join(f'"{col}"' for col in df.columns)

    # ?, ?, ?  (one per column)
    placeholders = ", ".join(["?"] * len(df.columns)) # '?' is used as placeholders in the SQL because it prevents SQL injection


    insert_sql = f'INSERT INTO "{table_name}" ({columns}) VALUES ({placeholders})'

    rows_inserted = 0
    try:
        for _, row in df.iterrows():
            # Convert each row to a tuple and replace NaN with None (= NULL in SQLite)
            values = tuple(None if pd.isna(v) else v for v in row)
            cursor.execute(insert_sql, values)
            rows_inserted += 1

        conn.commit()  # save everything to disk at once
        logger.info(f"Inserted {rows_inserted:,} rows into '{table_name}'.")

    except sqlite3.Error as e:
        conn.rollback()  # undo all inserts if something goes wrong
        logger.error(f"Insert failed: {e}")
        raise

    return rows_inserted
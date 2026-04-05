import sqlite3
import pandas as pd
import logging

# Setup: This tells computer where to save status messages
# Saves messages to 'error_log.txt' and shows them on screen
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("error_log.txt"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Step 1: Open CSV file and turn it into a table (DataFrame)
def load_csv(filepath: str) -> pd.DataFrame:
    try:
        # Try to read file
        df = pd.read_csv(filepath)
    except FileNotFoundError:
        logger.error(f"File not found: {filepath}")
        raise
    except Exception as e:
        logger.error(f"Could not read '{filepath}': {e}")
        raise ValueError(f"Could not parse CSV: {e}")

    # Check if file is actually empty
    if df.empty:
        raise ValueError(f"'{filepath}' is empty — nothing to load.")

    # Remove spaces from column names (SQL prefers underscores _)
    df.columns = [col.strip().replace(" ", "_") for col in df.columns]

    logger.info(f"Loaded '{filepath}': {len(df):,} rows.")
    return df

# Step 2: Put table into database one row at a time
def insert_rows(conn: sqlite3.Connection, table_name: str, df: pd.DataFrame) -> int: 
    cursor = conn.cursor()

    # Create column list: "col1", "col2", "col3"
    columns = ", ".join(f'"{col}"' for col in df.columns)

    # Create placeholders: ?, ?, ? (safety feature against hackers)
    placeholders = ", ".join(["?"] * len(df.columns))

    # Insert into "my_table" (col1, col2) VALUES (?, ?)
    insert_sql = f'INSERT INTO "{table_name}" ({columns}) VALUES ({placeholders})'

    rows_inserted = 0
    try:
        # Loop through every row in table
        for _, row in df.iterrows():
            # Convert row to a list of values; change "NaN" (empty) to "None" for SQL
            values = tuple(None if pd.isna(v) else v for v in row)
            cursor.execute(insert_sql, values)
            rows_inserted += 1

        # Save all changes to database at once
        conn.commit()
        logger.info(f"Inserted {rows_inserted:,} rows into '{table_name}'.")

    except sqlite3.Error as e:
        # Undo if any error happens so database doesn't get messy
        conn.rollback()
        logger.error(f"Insert failed: {e}")
        raise

    return rows_inserted
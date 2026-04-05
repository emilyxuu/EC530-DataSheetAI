import sqlite3
import os
import sys

from loader.csv_loader import load_csv, insert_rows
from schema.schema_manager import (
    create_table_from_df, schemas_match,
    get_table_schema, describe_schema, list_tables
)
from query_service.query_service import QueryService
from llm.llm_adapter import LLMAdapter


def print_results(columns: list, rows: list) -> None:
    # Prints query results as a simple table in terminal
    if not rows:
        print("  (no results)")
        return

    # See how wide each column needs to be
    widths = [len(str(col)) for col in columns]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell) if cell is not None else "NULL"))

    # Print header
    header = " | ".join(str(col).ljust(widths[i]) for i, col in enumerate(columns))
    divider = "-+-".join("-" * w for w in widths)
    print(f"\n  {header}")
    print(f"  {divider}")

    # Print each row
    for row in rows:
        cells = [(str(c) if c is not None else "NULL").ljust(widths[i]) for i, c in enumerate(row)]
        print(f"  {' | '.join(cells)}")

    print(f"\n  {len(rows):,} row(s) returned.")


def handle_load(conn: sqlite3.Connection, filepath: str) -> None:
    # Loads CSV file into database and decides to create a new table or use an existing one

    # Step 1: Read CSV file
    try:
        df = load_csv(filepath)
    except FileNotFoundError:
        print(f"  Error: File not found — '{filepath}'")
        return
    except ValueError as e:
        print(f"  Error: {e}")
        return

    print(f"  Found {len(df):,} rows with columns: {list(df.columns)}")

    # Step 2: Ask what to name the table (default = filename without .csv)
    default_name = os.path.splitext(os.path.basename(filepath))[0].replace(" ", "_")
    raw = input(f"  Table name [{default_name}]: ").strip()
    table_name = raw if raw else default_name

    existing_tables = list_tables(conn)

    if table_name in existing_tables:
        # If table already exists, then check if CSV columns match
        existing_schema = get_table_schema(conn, table_name)

        if schemas_match(existing_schema, df):
            # If columns match, then it's safe to add rows to the existing table
            print(f"  Columns match — appending to '{table_name}'.")
        else:
            # If columns don't match, then ask user what to do
            print(f"  Warning: CSV columns don't match existing '{table_name}' table.")
            choice = input("  [o] overwrite   [r] rename to new table   [s] skip: ").strip().lower()

            if choice == "o":
                # Delete old table and recreate it
                conn.execute(f'DROP TABLE "{table_name}";')
                conn.commit()
                print(f"  Dropped '{table_name}'. Recreating...")
                create_table_from_df(conn, table_name, df)
            elif choice == "r":
                table_name = input("  New table name: ").strip()
                if not table_name:
                    print("  Error: Table name cannot be empty.")
                    return
                create_table_from_df(conn, table_name, df)
            else:
                print("  Skipped.")
                return
    else:
        # Create new table from CSV columns
        create_table_from_df(conn, table_name, df)

    # Step 3: Insert all rows into table
    try:
        count = insert_rows(conn, table_name, df)
        print(f"  Done. {count:,} rows inserted into '{table_name}'.")
    except Exception as e:
        print(f"  Error inserting rows: {e}")


def main():
    print("=" * 50)
    print("  DataSheetAI — US Population Explorer")
    print("=" * 50)

    # Open (or create) SQLite database file
    conn = sqlite3.connect("datasheetai.db")
    print(f"\n  Database: datasheetai.db")

    # Try to connect LLM (needs ANTHROPIC_API_KEY in .env)
    llm = None
    try:
        llm = LLMAdapter(schema_description=describe_schema(conn))
        print("  AI: Claude connected")
    except EnvironmentError as e:
        print(f"  AI: Not connected — {e}")
        print("  You can still use 'sql' to run queries directly.")

    # CLI never touches database directly and always goes through QueryService
    service = QueryService(conn=conn, llm_adapter=llm)

    print("\n  Commands: load <file>  |  ask <question>  |  sql <query>")
    print("            tables  |  schema  |  help  |  exit\n")

    # Main command loop
    while True:
        try:
            raw = input("datasheetai> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n  Goodbye!")
            break

        if not raw:
            continue

        # Split input into command and the rest
        parts = raw.split(maxsplit=1)
        command = parts[0].lower()
        arg = parts[1] if len(parts) > 1 else ""

        if command in ("exit", "quit"):
            print("  Goodbye!")
            break

        elif command == "help":
            print("""
  load <path/to/file.csv>   Load a CSV into the database
  ask  <your question>      Ask in plain English (AI converts to SQL)
  sql  <SELECT ...>         Run a SQL query directly
  tables                    List all tables in the database
  schema                    Show all tables and their columns
  exit                      Quit
""")

        elif command == "load":
            if not arg:
                print("  Usage: load <path/to/file.csv>")
            else:
                handle_load(conn, arg)
                # Refresh the LLM's schema after loading new data
                if llm is not None:
                    llm.schema_description = describe_schema(conn)

        elif command == "ask":
            # User typed a plain English question and send to AI
            if not arg:
                print("  Usage: ask <your question>")
            elif llm is None:
                print("  AI is not connected. Set ANTHROPIC_API_KEY in your .env file.")
            else:
                print("  Thinking...")
                result = service.ask(arg)
                if result["success"]:
                    print(f"  SQL used: {result['sql']}")
                    print_results(result["columns"], result["rows"])
                else:
                    print(f"  Error: {result['error']}")

        elif command == "sql":
            # User typed SQL directly and validate and run it
            if not arg:
                print("  Usage: sql SELECT ...")
            else:
                result = service.run_sql(arg)
                if result["success"]:
                    print_results(result["columns"], result["rows"])
                else:
                    print(f"  Error: {result['error']}")

        elif command == "tables":
            tables = service.list_tables()
            if tables:
                for t in tables:
                    print(f"  - {t}")
            else:
                print("  No tables yet. Use 'load' to import a CSV.")

        elif command == "schema":
            print(describe_schema(conn))

        else:
            print(f"  Unknown command: '{command}'. Type 'help' for options.")

    conn.close()


if __name__ == "__main__":
    main()
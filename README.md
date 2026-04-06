# DataSheetAI — US Population Data Explorer

A command-line tool that loads US population data into a SQLite database and lets you ask questions about it in plain English. An AI (Claude) translates your question into SQL, a strict validator ensures it is safe, and the results are printed right in your terminal.

**Dataset:** `Population_by_Race_-_US__States__Counties.csv`
111,815 rows of US population data by race for every US state and county, from 1990 to 2024.

---

## System Overview

The system is built on a modular architecture where responsibilities are strictly separated:

```
User
 └── CLI   (Entry point: never touches the database directly)
      ├── Query Service   (Coordinates flow: LLM → Validate → Execute)
      │    ├── LLM Adapter   (Translates English to SQL)
      │    ├── SQL Validator   (Blocks unsafe queries before execution)
      │    └── SQLite DB
      └── Loader & Schema   (Handles data ingestion and dynamic table creation)
```

| Module | File | Job |
|---|---|---|
| CSV Loader | `loader/csv_loader.py` | Reads CSV, inserts rows manually |
| Schema Manager | `schema/schema_manager.py` | Tracks tables and columns; decides append vs create |
| SQL Validator | `query_service/sql_validator.py` | Blocks unsafe queries before they reach the database |
| Query Service | `query_service/query_service.py` | Coordinates LLM → Validate → Execute |
| LLM Adapter | `llm/llm_adapter.py` | Sends question + schema to Claude, returns SQL string |
| CLI | `cli/cli.py` | User-facing commands, no direct database access |

---

## How to Run

**1. Clone the repo and install dependencies**
```bash
git clone https://github.com/emilyxuu/EC530-DataSheetAI.git
cd EC530-DataSheetAI
pip install -r requirements.txt
```

**2. Add your API key**

Copy `.env.example` to `.env` and fill in your key from [console.anthropic.com](https://console.anthropic.com). Set a $5 budget limit in the dashboard.
```bash
cp .env.example .env
# Open .env and replace: your-key-here -> your real key
```

**3. Start the app**
```bash
python cli/cli.py
```

**4. Load data and ask questions**
```
datasheetai> load sample_data/population_by_race.csv
datasheetai> ask Which state had the highest Hispanic population in 2020?
datasheetai> ask Show Alabama total population from 2010 to 2020
datasheetai> sql SELECT Description, Total_Population FROM population_by_race WHERE Year = 2020 LIMIT 5
datasheetai> tables
datasheetai> schema
datasheetai> exit
```

---

## How to Run Tests

No API key needed. Tests use a MockLLM so no real API calls are made.

```bash
# Run all tests
python -m unittest discover -s tests

# Run one file at a time
python tests/test_csv_loader.py
python tests/test_schema_manager.py
python tests/test_sql_validator.py
python tests/test_query_service.py
```

---

## Design Decisions

**Why does the CLI go through QueryService instead of touching the database directly?**
Separation of concerns. The CLI only handles input and output. The QueryService handles all logic. This means if we ever add a different interface, it reuses QueryService with no changes to validation or execution.

**Why is LLM output treated as untrusted?**
LLMs hallucinate. During development, Claude generated `SELECT state_name FROM population_by_race` — but the real column is `Description`. The SQL Validator caught this before it reached the database. Every piece of SQL the LLM returns is validated before it runs, no exceptions.

**Why build INSERT statements manually instead of df.to_sql()?**
The assignment requires it. It also forces understanding of schema creation and type mapping, and lets us handle NaN values explicitly so they become NULL in SQLite instead of crashing.

**Why use PRAGMA table_info() in the Schema Manager?**
It is SQLite's built-in command for reading a table's column structure. It requires no third-party library and always returns accurate results.

---

## LLM Integration

The LLM Adapter sends two things to Claude: the user's question and the full database schema. Claude returns SQL in a code block. The adapter extracts the SQL string and returns it — it never executes anything itself.

That SQL then goes through the SQL Validator which checks:
- Is it a SELECT? (blocks INSERT, DROP, DELETE, UPDATE, etc.)
- Do the referenced tables exist?
- Do the referenced columns exist?
- Are there injection patterns (comments, null bytes, multiple statements)?
- Does SQLite's own parser accept the syntax?

Only if all checks pass does the query run.

**Demonstrated LLM failure (required by assignment):**
Claude generated `state_name` instead of `Description`. The test `test_catches_hallucinated_column` in `tests/test_sql_validator.py` documents this. After the test caught it, I updated the LLM prompt to explicitly name the columns, which fixed the hallucination.

---

## Where AI Was Used

| File | How AI was used |
|---|---|
| `llm/llm_adapter.py` | Claude translates questions to SQL |
| `query_service/sql_validator.py` | Used Claude to help implement the validator after I designed the API and tests |
| `schema/schema_manager.py` | Used Claude to suggest the pandas to SQLite type mapping |
| `README.md` | Used Claude double check README included all my files, clear formatting, and no missing steps for a finalized assignment summary |

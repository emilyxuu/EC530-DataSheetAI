import os
import re
import logging
import anthropic
from dotenv import load_dotenv # Imports the tool to read .env files

logger = logging.getLogger(__name__)

# This automatically looks for a .env file and loads variables inside it
load_dotenv()

# Sends user's plain English question to Claude and gets SQL code back
class LLMAdapter:

    def __init__(self, schema_description: str):
        # The schema_description is the map of the database I give to Claude
        self.schema_description = schema_description

        # Look for API key that load_dotenv() just loaded
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise EnvironmentError("ANTHROPIC_API_KEY is not set. Please check the .env file.")

        # Connect to Claude
        self.client = anthropic.Anthropic(api_key=api_key)

    # Takes an English question, asks Claude for SQL, and returns the SQL string
    def translate(self, user_question: str) -> str:
        
        prompt = self._build_prompt(user_question)
        logger.info(f"Asking Claude: '{user_question}'")

        # Send prompt to Claude
        response = self.client.messages.create(
            model="claude-opus-4-5",
            max_tokens=512,
            messages=[{"role": "user", "content": prompt}]
        )

        # Get the text part of Claude's reply
        raw_text = response.content[0].text
        logger.info(f"Claude responded: {raw_text[:200]}")

        # Extract only the SQL code from Claude's full text reply
        sql = self._extract_sql(raw_text)
        if not sql:
            raise ValueError(f"No SQL found in Claude's response: {raw_text}")

        logger.info(f"Extracted SQL: {sql}")
        return sql

    # Creates the specific instructions I send to Claude
    def _build_prompt(self, user_question: str) -> str:
        return f"""You are a SQL assistant for a SQLite database containing US population data by race, state, and county.

Database schema:
{self.schema_description}

Column notes:
- "Description" is the place name (e.g. "U.S.", "Alabama", "Autauga County, AL")
- "Year" ranges from 1990 to 2024
- "Statefips" = 0 means national total; "Countyfips" = 0 means state total
- Some race columns may be NULL for earlier years

User question: "{user_question}"

Write a single SELECT query that answers the question.
Use only table and column names from the schema above.
Wrap your SQL in a ```sql code block. Do not write INSERT, UPDATE, DELETE, or DROP.

```sql
-- your query here
```"""

    # Finds the actual SQL code inside Claude's response and cleans it up
    def _extract_sql(self, text: str) -> str:
        
        # Look for the SQL hidden inside the markdown code block (```sql ... ```)
        match = re.search(r'```sql\s*(.*?)\s*```', text, re.DOTALL | re.IGNORECASE)
        if match:
            # Remove any comment lines (--) that Claude might have added
            lines = [
                line for line in match.group(1).strip().splitlines()
                if not line.strip().startswith("--")
            ]
            return " ".join(lines).strip()

        # Backup: If Claude forgot the code block, just look for the word SELECT
        match = re.search(r'(SELECT\s+.+?)(?:;|$)', text, re.IGNORECASE | re.DOTALL)
        if match:
            return match.group(1).strip()

        return ""
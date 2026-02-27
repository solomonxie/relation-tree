import os
import json
import sqlite3
import requests
import argparse
from dotenv import load_dotenv

load_dotenv()

LLM_API_BASE = os.getenv("LLM_API_BASE", "http://localhost:11434/v1")
LLM_API_KEY = os.getenv("LLM_API_KEY", "ollama")
MODEL_NAME = os.getenv("LLM_MODEL", "llama3")

DB_PATH = "data/db/database.sqlite"

PROMPT_TEMPLATE = """
Refine the information for the following person record.

Current Data:
{person_json}

Your task:
1. Normalize the "name": Strip any group names, titles (Pastor, Dr, etc.), or parenthetical info.
2. If the name contains both Chinese and English (e.g. "Zhang San (John)"), separate them.
3. Identify if the current name is actually a group or a nickname rather than a real name.
4. Extract missing fields from "notes" or "display_name": gender, birthdate, ethnicity, origins.
5. Categorize any hints in the notes into "brief" or additional "notes".

Return a JSON object with the following fields (only include fields that should be updated):
{{
    "name": "Clean Real Name",
    "display_name": "Full name or how they are addressed",
    "nick_name": "Short name",
    "title": "Professional title",
    "gender": "male/female",
    "birthdate": "YYYY-MM-DD",
    "brief": "One sentence summary",
    "origins": "Origin info",
    "ethnicity": "Ethnicity info",
    "notes": "Remaining notes"
}}

If no changes are needed, return an empty JSON object {{}}.
Only return the JSON object.
"""

def query_llm(person_data):
    payload = {
        "model": MODEL_NAME,
        "messages": [
            {"role": "system", "content": "You are an expert at data cleansing and entity attribute extraction."},
            {"role": "user", "content": PROMPT_TEMPLATE.format(person_json=json.dumps(person_data, indent=2, ensure_ascii=False))}
        ],
        "temperature": 0.1,
        "response_format": {"type": "json_object"}
    }
    
    try:
        response = requests.post(f"{LLM_API_BASE}/chat/completions", 
                                 headers={"Authorization": f"Bearer {LLM_API_KEY}"},
                                 json=payload)
        response.raise_for_status()
        content = response.json()['choices'][0]['message']['content']
        return json.loads(content)
    except Exception as e:
        print(f"Error querying LLM for person {person_data.get('id')}: {e}")
        return {}

def get_persons(limit=None):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    query = "SELECT * FROM persons"
    if limit:
        query += f" LIMIT {limit}"
    cursor.execute(query)
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows

def update_person(person_id, updates):
    if not updates:
        return
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    fields = []
    params = []
    for key, value in updates.items():
        if key in ["name", "display_name", "nick_name", "title", "gender", "birthdate", "brief", "origins", "ethnicity", "notes"]:
            fields.append(f"{key} = ?")
            params.append(value)
    
    if fields:
        params.append(person_id)
        cursor.execute(f"UPDATE persons SET {', '.join(fields)} WHERE id = ?", params)
        conn.commit()
    conn.close()

def main():
    parser = argparse.ArgumentParser(description="Refine person information using LLM.")
    parser.add_argument("--limit", type=int, help="Limit number of persons to process.")
    parser.add_argument("--dry-run", action="store_true", help="Don't update the database.")
    args = parser.parse_args()

    persons = get_persons(args.limit)
    print(f"Processing {len(persons)} persons...")

    for i, person in enumerate(persons):
        print(f"[{i+1}/{len(persons)}] Refining {person['name']} (ID: {person['id']})...")
        suggestions = query_llm(person)
        
        if suggestions:
            print(f"  Suggestions: {suggestions}")
            if not args.dry_run:
                update_person(person['id'], suggestions)
        else:
            print("  No suggestions.")

if __name__ == "__main__":
    main()

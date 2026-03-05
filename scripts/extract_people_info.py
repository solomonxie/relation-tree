import os
import json
import sqlite3
import argparse
import requests
from dotenv import load_dotenv

load_dotenv()

# Configuration for Local LLM (OpenAI-compatible API)
LLM_API_BASE = os.getenv(
    "LLM_API_BASE", "http://localhost:11434/v1"
)  # Default to Ollama
LLM_API_KEY = os.getenv("LLM_API_KEY", "ollama")
MODEL_NAME = os.getenv("LLM_MODEL", "llama3")

DB_PATH = "data/db/database.sqlite"

PROMPT_TEMPLATE = """
Extract information about people from the following text and return it as a JSON object with "people" and "relationships" keys.

CRITICAL RULES FOR PEOPLE:
1. ONE PERSON PER OBJECT. NEVER group people. 
   - "John & Mary" -> TWO objects: {{"name": "John"}}, {{"name": "Mary"}}.
   - "Liu Yang's family" -> extract EVERY person mentioned: Liu Yang, his wife, his children, etc.
   - If a husband and wife are mentioned, create TWO separate objects.
   - If a name contains "and", "&", or "family", you are likely failing this rule. Split them!
2. NO TITLES IN NAME. Strip "Pastor", "Dr.", "Rev.", "Tita", "Tito", "Tita:", "Tito:", etc.
3. LEGAL NAMES. Use full names. For unnamed relatives, use "Name of person (relationship)", e.g. "John Doe (wife)".
4. GROUP CATEGORIZATION:
   - "FBC" or "First Baptist Church" -> type: "church group", name: "First Baptist Church".
   - "YA" or "Young Adults" -> type: "church group", name: "FBC Young Adults".
   - Categorize other groups (family, sport, etc.) logically.

CRITICAL RULES FOR RELATIONSHIPS:
1. Identify connections between people extracted from THIS text chunk.
2. Supported types: 'spouse', 'parent', 'child', 'sibling', 'friend', 'colleague'.
3. Always list person1_name and person2_name (matching the "name" field in the "people" list).

JSON Schema:
{{
    "people": [
        {{
            "name": "Legal Full Name (No titles)",
            "title": "Professional title (Pastor, Dr, etc)",
            "display_name": "How they are usually called",
            "nick_name": "Short name or informal name",
            "other_names": "country:name;country:name",
            "gender": "male/female/other",
            "brief": "One sentence summary",
            "origins": "Where they are from originally",
            "ethnicity": "Their ethnicity",
            "notes": "Any other miscellaneous information",
            "contacts": [
                {{"type": "phone/email/address/social", "value": "..."}}
            ],
            "education": [
                {{"school": "...", "degree": "...", "major": "...", "start_date": "...", "end_date": "...", "notes": "..."}}
            ],
            "financial_information": [
                {{"type": "interact/etransfer/bank account/etc", "country": "...", "details": "..."}}
            ],
            "career": [
                {{"company": "...", "role": "...", "start_date": "...", "end_date": "...", "notes": "..."}}
            ],
            "groups": [
                {{"type": "church group/family group/sport group/etc", "name": "...", "role": "..."}}
            ],
            "positions": [
                {{"name": "Specific role name", "organization": "e.g. FBC", "notes": "..."}}
            ],
            "property": [
                {{"type": "house/car/bike/game console/etc", "details": "address or description"}}
            ]
        }}
    ],
    "relationships": [
        {{
            "person1_name": "...",
            "person2_name": "...",
            "type": "spouse/parent/child/etc",
            "notes": "..."
        }}
    ]
}}

Only return the JSON object. Do not include any other text.

Text to extract from:
---
{text}
---
"""


def query_llm(text):
    payload = {
        "model": MODEL_NAME,
        "messages": [
            {
                "role": "system",
                "content": "You are a helpful assistant that extracts structured data from text into JSON format.",
            },
            {"role": "user", "content": PROMPT_TEMPLATE.format(text=text)},
        ],
        "temperature": 0.1,
        "response_format": {"type": "json_object"},
    }

    try:
        response = requests.post(
            f"{LLM_API_BASE}/chat/completions",
            headers={"Authorization": f"Bearer {LLM_API_KEY}"},
            json=payload,
        )
        response.raise_for_status()
        content = response.json()["choices"][0]["message"]["content"]
        # Some LLMs might wrap JSON in markdown blocks
        if content.startswith("```json"):
            content = content.split("```json")[1].split("```")[0].strip()
        elif content.startswith("```"):
            content = content.split("```")[1].split("```")[0].strip()

        data = json.loads(content)
        return data
    except Exception as e:
        print(f"Error querying LLM: {e}")
        return {}


def insert_into_db(data):
    people_data = data.get("people", [])
    relationships_data = data.get("relationships", [])

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    name_to_id = {}

    for person in people_data:
        try:
            name = person.get("name") or person.get("display_name")
            if not name:
                print(f"Skipping entry with no name: {person}")
                continue

            def to_str(val):
                if val is None:
                    return None
                if isinstance(val, (list, dict)):
                    return json.dumps(val)
                s = str(val).strip()
                return s if s else None

            # Insert into persons
            cursor.execute(
                """
                INSERT INTO persons (name, title, display_name, nick_name, other_names, gender, birthdate, brief, origins, ethnicity, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    name,
                    to_str(person.get("title")),
                    to_str(person.get("display_name")),
                    to_str(person.get("nick_name")),
                    to_str(person.get("other_names")),
                    to_str(person.get("gender")),
                    to_str(person.get("birthdate")),
                    to_str(person.get("brief")),
                    to_str(person.get("origins")),
                    to_str(person.get("ethnicity")),
                    to_str(person.get("notes")),
                ),
            )
            person_id = cursor.lastrowid
            name_to_id[name] = person_id

            # Insert contacts
            for contact in person.get("contacts", []):
                val = to_str(contact.get("value"))
                if val:
                    cursor.execute(
                        "INSERT INTO contacts (person_id, type, value) VALUES (?, ?, ?)",
                        (person_id, contact.get("type"), val),
                    )

            # Insert positions
            for pos in person.get("positions", []):
                pos_name = pos.get("name")
                if not pos_name:
                    continue

                cursor.execute(
                    "INSERT OR IGNORE INTO positions (name) VALUES (?)", (pos_name,)
                )
                cursor.execute("SELECT id FROM positions WHERE name = ?", (pos_name,))
                pos_id = cursor.fetchone()[0]

                cursor.execute(
                    """
                    INSERT INTO person_positions (person_id, position_id, organization, notes)
                    VALUES (?, ?, ?, ?)
                """,
                    (person_id, pos_id, pos.get("organization"), pos.get("notes")),
                )

            # Insert education
            for edu in person.get("education", []):
                cursor.execute(
                    """
                    INSERT INTO education (person_id, school, degree, major, start_date, end_date, notes)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        person_id,
                        edu.get("school"),
                        edu.get("degree"),
                        edu.get("major"),
                        edu.get("start_date"),
                        edu.get("end_date"),
                        edu.get("notes"),
                    ),
                )

            # Insert financial info
            for fin in person.get("financial_information", []):
                cursor.execute(
                    """
                    INSERT INTO financial_information (person_id, type, country, details)
                    VALUES (?, ?, ?, ?)
                """,
                    (
                        person_id,
                        fin.get("type"),
                        fin.get("country"),
                        fin.get("details"),
                    ),
                )

            # Insert career
            for job in person.get("career", []):
                cursor.execute(
                    """
                    INSERT INTO career (person_id, company, role, start_date, end_date, notes)
                    VALUES (?, ?, ?, ?, ?, ?)
                """,
                    (
                        person_id,
                        job.get("company"),
                        job.get("role"),
                        job.get("start_date"),
                        job.get("end_date"),
                        job.get("notes"),
                    ),
                )

            # Insert groups
            for grp in person.get("groups", []):
                group_name = grp.get("name")
                if not group_name:
                    continue

                # Check if group exists or create it
                cursor.execute(
                    "SELECT id FROM groups WHERE name = ? AND type = ?",
                    (group_name, grp.get("type")),
                )
                row = cursor.fetchone()
                if row:
                    group_id = row[0]
                else:
                    cursor.execute(
                        "INSERT INTO groups (name, type) VALUES (?, ?)",
                        (grp.get("name"), grp.get("type")),
                    )
                    group_id = cursor.lastrowid

                cursor.execute(
                    "INSERT INTO person_groups (person_id, group_id, role) VALUES (?, ?, ?)",
                    (person_id, group_id, grp.get("role")),
                )

            # Insert property
            for prop in person.get("property", []):
                cursor.execute(
                    "INSERT INTO property (person_id, type, details) VALUES (?, ?, ?)",
                    (person_id, prop.get("type"), prop.get("details")),
                )

            conn.commit()
            print(f"Successfully inserted: {name}")
        except Exception as e:
            conn.rollback()
            print(f"Error inserting {name}: {e}")

    # Insert relationships
    for rel in relationships_data:
        try:
            p1_name = rel.get("person1_name")
            p2_name = rel.get("person2_name")
            p1_id = name_to_id.get(p1_name)
            p2_id = name_to_id.get(p2_name)

            if p1_id and p2_id:
                cursor.execute(
                    """
                    INSERT INTO relationships (person1_id, person2_id, type, notes)
                    VALUES (?, ?, ?, ?)
                """,
                    (p1_id, p2_id, rel.get("type"), rel.get("notes")),
                )
                conn.commit()
                print(
                    f"Successfully inserted relationship: {p1_name} -> {p2_name} ({rel.get('type')})"
                )
        except Exception as e:
            conn.rollback()
            print(f"Error inserting relationship: {e}")

    conn.close()


def chunk_text(text, max_chars=4000):
    """Simple chunking by character count, attempting to split on double newlines."""
    chunks = []
    while text:
        if len(text) <= max_chars:
            chunks.append(text)
            break

        # Find the last double newline before max_chars
        split_at = text.rfind("\n\n", 0, max_chars)
        if split_at == -1:
            split_at = text.rfind("\n", 0, max_chars)

        if split_at == -1:
            split_at = max_chars

        chunks.append(text[:split_at].strip())
        text = text[split_at:].strip()
    return chunks


def main():
    parser = argparse.ArgumentParser(
        description="Extract people information using LLM and insert into SQLite."
    )
    parser.add_argument("file", help="Path to the text file containing people notes.")
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=4000,
        help="Maximum characters per LLM request.",
    )

    args = parser.parse_args()

    if not os.path.exists(args.file):
        print(f"File not found: {args.file}")
        return

    with open(args.file, "r", encoding="utf-8") as f:
        content = f.read()

    chunks = chunk_text(content, args.chunk_size)
    print(f"Processing {len(chunks)} chunks...")

    for i, chunk in enumerate(chunks):
        print(f"Processing chunk {i + 1}/{len(chunks)}...")
        extracted_data = query_llm(chunk)
        if extracted_data:
            insert_into_db(extracted_data)
        else:
            print(f"No data extracted from chunk {i + 1}")


if __name__ == "__main__":
    main()

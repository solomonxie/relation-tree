import sqlite3
import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

DB_PATH = "data/db/database.sqlite"
LLM_API_BASE = os.getenv("LLM_API_BASE", "http://localhost:11434/v1")
LLM_API_KEY = os.getenv("LLM_API_KEY", "ollama")
MODEL_NAME = os.getenv("LLM_MODEL", "llama3:latest")

SPLIT_PROMPT = """
The following JSON object represents multiple people grouped together. 
Split it into a list of separate JSON objects, one for each individual person. 
Ensure all shared information (like notes, origins, ethnicity, or groups) is duplicated for each person as appropriate.
Strip titles like "Pastor", "Dr", etc from the name field and put them in the title field.

Object to split:
{data}

Return ONLY a JSON list of objects.
"""


def split_mixed_person(person_id):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # 1. Fetch person data
    cursor.execute("SELECT * FROM persons WHERE id = ?", (person_id,))
    person = dict(cursor.fetchone())

    # 2. Fetch related data
    cursor.execute("SELECT type, value FROM contacts WHERE person_id = ?", (person_id,))
    person["contacts"] = [dict(r) for r in cursor.fetchall()]

    cursor.execute(
        "SELECT type, details FROM property WHERE person_id = ?", (person_id,)
    )
    person["property"] = [dict(r) for r in cursor.fetchall()]

    cursor.execute(
        "SELECT g.type, g.name, pg.role FROM person_groups pg JOIN groups g ON pg.group_id = g.id WHERE pg.person_id = ?",
        (person_id,),
    )
    person["groups"] = [dict(r) for r in cursor.fetchall()]

    cursor.execute(
        "SELECT pos.name, pp.organization, pp.notes FROM person_positions pp JOIN positions pos ON pp.position_id = pos.id WHERE pp.person_id = ?",
        (person_id,),
    )
    person["positions"] = [dict(r) for r in cursor.fetchall()]

    # 3. Ask LLM to split
    print(f"Splitting: {person['name']}...")
    payload = {
        "model": MODEL_NAME,
        "messages": [
            {"role": "user", "content": SPLIT_PROMPT.format(data=json.dumps(person))}
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
        content = response.json()["choices"][0]["message"]["content"]
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0].strip()
        new_people = json.loads(content)
        if isinstance(new_people, dict) and "people" in new_people:
            new_people = new_people["people"]
        if isinstance(new_people, dict):
            new_people = [new_people]

        return new_people, person_id
    except Exception as e:
        print(f"Error splitting {person['name']}: {e}")
        return [], None


def insert_person(cursor, person):
    def to_str(val):
        if val is None:
            return None
        if isinstance(val, (list, dict)):
            return json.dumps(val)
        s = str(val).strip()
        return s if s else None

    cursor.execute(
        """
        INSERT INTO persons (name, title, display_name, nick_name, other_names, gender, birthdate, brief, origins, ethnicity, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        (
            person.get("name"),
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

    for contact in person.get("contacts", []):
        cursor.execute(
            "INSERT INTO contacts (person_id, type, value) VALUES (?, ?, ?)",
            (person_id, contact.get("type"), contact.get("value")),
        )

    for pos in person.get("positions", []):
        name = pos.get("name")
        if not name:
            continue
        cursor.execute("INSERT OR IGNORE INTO positions (name) VALUES (?)", (name,))
        cursor.execute("SELECT id FROM positions WHERE name = ?", (name,))
        pos_id = cursor.fetchone()[0]
        cursor.execute(
            "INSERT INTO person_positions (person_id, position_id, organization, notes) VALUES (?, ?, ?, ?)",
            (person_id, pos_id, pos.get("organization"), pos.get("notes")),
        )

    for grp in person.get("groups", []):
        name = grp.get("name")
        if not name:
            continue
        cursor.execute(
            "SELECT id FROM groups WHERE name = ? AND type = ?",
            (name, grp.get("type")),
        )
        row = cursor.fetchone()
        if row:
            group_id = row[0]
        else:
            cursor.execute(
                "INSERT INTO groups (name, type) VALUES (?, ?)",
                (name, grp.get("type")),
            )
            group_id = cursor.lastrowid
        cursor.execute(
            "INSERT INTO person_groups (person_id, group_id, role) VALUES (?, ?, ?)",
            (person_id, group_id, grp.get("role")),
        )

    for prop in person.get("property", []):
        cursor.execute(
            "INSERT INTO property (person_id, type, details) VALUES (?, ?, ?)",
            (person_id, prop.get("type"), prop.get("details")),
        )

    return person_id


def main():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute(
        "SELECT id FROM persons WHERE name LIKE '%&%' OR name LIKE '% and %'"
    )
    ids = [row[0] for row in cursor.fetchall()]

    for pid in ids:
        new_people, old_id = split_mixed_person(pid)
        if new_people:
            try:
                for p in new_people:
                    insert_person(cursor, p)

                # Delete old records
                cursor.execute("DELETE FROM contacts WHERE person_id = ?", (old_id,))
                cursor.execute(
                    "DELETE FROM person_positions WHERE person_id = ?", (old_id,)
                )
                cursor.execute(
                    "DELETE FROM person_groups WHERE person_id = ?", (old_id,)
                )
                cursor.execute("DELETE FROM property WHERE person_id = ?", (old_id,))
                cursor.execute("DELETE FROM education WHERE person_id = ?", (old_id,))
                cursor.execute(
                    "DELETE FROM financial_information WHERE person_id = ?", (old_id,)
                )
                cursor.execute("DELETE FROM career WHERE person_id = ?", (old_id,))
                cursor.execute("DELETE FROM persons WHERE id = ?", (old_id,))

                conn.commit()
                print(f"Successfully split and replaced ID {old_id}")
            except Exception as e:
                conn.rollback()
                print(f"Error updating database for ID {old_id}: {e}")

    conn.close()


if __name__ == "__main__":
    main()

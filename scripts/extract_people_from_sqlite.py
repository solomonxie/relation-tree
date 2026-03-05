import sqlite3
import json
import os
import argparse
import hashlib
from datetime import datetime

DB_PATH = "data/db/database.sqlite"
BLOBS_DIR = "blobs"


def get_db_connection(path):
    if not os.path.exists(path):
        return None
    try:
        return sqlite3.connect(path)
    except sqlite3.Error as e:
        print(f"Error connecting to {path}: {e}")
        return None


def insert_person(cursor, person_data):
    """
    Inserts a person and their contacts into the main database.
    """
    # Check if person already exists by name (very basic check)
    cursor.execute("SELECT id FROM persons WHERE name = ?", (person_data["name"],))
    row = cursor.fetchone()
    if row:
        person_id = row[0]
        # Update existing record if needed
        cursor.execute(
            "SELECT display_name, nick_name, birthdate, notes FROM persons WHERE id = ?",
            (person_id,),
        )
        curr_dn, curr_nn, curr_bd, curr_notes = cursor.fetchone()

        updates = []
        params = []
        if not curr_dn and person_data.get("display_name"):
            updates.append("display_name = ?")
            params.append(person_data["display_name"])
        if not curr_nn and person_data.get("nick_name"):
            updates.append("nick_name = ?")
            params.append(person_data["nick_name"])
        if not curr_bd and person_data.get("birthdate"):
            updates.append("birthdate = ?")
            params.append(person_data["birthdate"])
        if not curr_notes and person_data.get("notes"):
            updates.append("notes = ?")
            params.append(person_data["notes"])

        if updates:
            params.append(person_id)
            cursor.execute(
                f"UPDATE persons SET {', '.join(updates)} WHERE id = ?", params
            )
    else:
        # Generate folder_hash for new person
        folder_hash = hashlib.md5(
            f"{person_data.get('name')}_{datetime.now().timestamp()}".encode()
        ).hexdigest()[:16]
        cursor.execute(
            """
            INSERT INTO persons (name, display_name, nick_name, birthdate, notes, folder_hash)
            VALUES (?, ?, ?, ?, ?, ?)
        """,
            (
                person_data.get("name"),
                person_data.get("display_name"),
                person_data.get("nick_name"),
                person_data.get("birthdate"),
                person_data.get("notes"),
                folder_hash,
            ),
        )
        person_id = cursor.lastrowid

    for contact in person_data.get("contacts", []):
        if not contact.get("value"):
            continue
        # Avoid duplicate contacts for this person
        cursor.execute(
            """
            SELECT id FROM contacts WHERE person_id = ? AND type = ? AND value = ?
        """,
            (person_id, contact["type"], contact["value"]),
        )
        if not cursor.fetchone():
            cursor.execute(
                """
                INSERT INTO contacts (person_id, type, value)
                VALUES (?, ?, ?)
            """,
                (person_id, contact["type"], contact["value"]),
            )

    return person_id


def extract_old_contacts():
    path = os.path.join(BLOBS_DIR, "old_contacts.sqlite")
    conn = get_db_connection(path)
    if not conn:
        return []

    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT full_name, first_name, last_name, nickname, phones, emails, notes, birthday FROM contacts"
        )
    except sqlite3.Error:
        print(f"Table 'contacts' not found in {path}")
        return []

    people = []
    for row in cursor.fetchall():
        (
            full_name,
            first_name,
            last_name,
            nickname,
            phones_json,
            emails_json,
            notes,
            birthday,
        ) = row

        try:
            phones = json.loads(phones_json) if phones_json else []
        except Exception:
            phones = []
        try:
            emails = json.loads(emails_json) if emails_json else []
        except Exception:
            emails = []

        contacts = []
        for p in phones:
            if p:
                contacts.append({"type": "phone", "value": str(p)})
        for e in emails:
            if e:
                contacts.append({"type": "email", "value": str(e)})

        name = full_name or (f"{first_name or ''} {last_name or ''}").strip()
        if not name:
            continue

        people.append(
            {
                "name": name,
                "display_name": full_name,
                "nick_name": nickname,
                "birthdate": birthday,
                "notes": notes,
                "contacts": contacts,
            }
        )
    conn.close()
    return people


def extract_old_sms():
    path = os.path.join(BLOBS_DIR, "old_sms.sqlite")
    conn = get_db_connection(path)
    if not conn:
        return []

    cursor = conn.cursor()
    try:
        cursor.execute("SELECT name, phone_number FROM contacts")
    except sqlite3.Error:
        print(f"Table 'contacts' not found in {path}")
        return []

    people = []
    for row in cursor.fetchall():
        name, phone = row
        if not name and not phone:
            continue

        people.append(
            {
                "name": name or phone,
                "contacts": [{"type": "phone", "value": str(phone)}] if phone else [],
            }
        )
    conn.close()
    return people


def extract_old_wechat():
    path = os.path.join(BLOBS_DIR, "old_wechat.sqlite")
    conn = get_db_connection(path)
    if not conn:
        return []

    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT username, nickname FROM contacts WHERE nickname != '' AND nickname IS NOT NULL"
        )
    except sqlite3.Error:
        print(f"Table 'contacts' not found in {path}")
        return []

    people = []
    for row in cursor.fetchall():
        username, nickname = row
        if not nickname:
            continue

        people.append(
            {
                "name": nickname,
                "nick_name": nickname,
                "contacts": [{"type": "wechat", "value": str(username)}],
            }
        )
    conn.close()
    return people


def extract_vcf():
    vcf_files = [f for f in os.listdir(BLOBS_DIR) if f.endswith(".vcf")]
    people = []

    for vcf_file in vcf_files:
        path = os.path.join(BLOBS_DIR, vcf_file)
        print(f"Extracting from {vcf_file}...")
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()

        vcards = content.split("BEGIN:VCARD")
        for vcard in vcards:
            if "END:VCARD" not in vcard:
                continue

            lines = vcard.splitlines()
            person = {"name": "", "contacts": [], "notes": ""}
            for line in lines:
                if line.startswith("FN:"):
                    person["name"] = line[3:].strip()
                elif line.startswith("N:"):
                    if not person["name"]:
                        parts = line[2:].split(";")
                        full_name = " ".join(
                            [p.strip() for p in parts[::-1] if p.strip()]
                        )
                        person["name"] = full_name.strip()
                elif line.startswith("TEL"):
                    val = line.split(":")[-1].strip()
                    if val:
                        person["contacts"].append({"type": "phone", "value": val})
                elif line.startswith("EMAIL"):
                    val = line.split(":")[-1].strip()
                    if val:
                        person["contacts"].append({"type": "email", "value": val})
                elif line.startswith("ADR"):
                    val = line.split(":")[-1].strip().replace(";", " ").strip()
                    if val:
                        person["contacts"].append({"type": "address", "value": val})
                elif line.startswith("NOTE"):
                    person["notes"] += line.split(":", 1)[-1].strip() + "\n"
                elif line.startswith("BDAY"):
                    person["birthdate"] = line.split(":")[-1].strip()

            if person["name"]:
                person["notes"] = person["notes"].strip()
                people.append(person)
    return people


def main():
    parser = argparse.ArgumentParser(
        description="Extract people information from various sqlite and vcf files in blobs/"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print extracted data instead of inserting into DB",
    )
    parser.add_argument(
        "--limit", type=int, default=None, help="Limit number of records to process"
    )
    args = parser.parse_args()

    all_people = []

    print("Extracting from old_contacts.sqlite...")
    all_people.extend(extract_old_contacts())

    print("Extracting from old_sms.sqlite...")
    all_people.extend(extract_old_sms())

    print("Extracting from old_wechat.sqlite...")
    all_people.extend(extract_old_wechat())

    print("Extracting from VCF files...")
    all_people.extend(extract_vcf())

    print(f"Total extracted: {len(all_people)} people records.")

    if args.limit:
        all_people = all_people[: args.limit]

    if args.dry_run:
        for p in all_people[:10]:
            print(json.dumps(p, indent=2, ensure_ascii=False))
        if len(all_people) > 10:
            print(f"... and {len(all_people) - 10} more.")
    else:
        if not os.path.exists(DB_PATH):
            print(f"Error: Destination database {DB_PATH} does not exist.")
            return

        dest_conn = sqlite3.connect(DB_PATH)
        dest_cursor = dest_conn.cursor()
        print(f"Inserting into {DB_PATH}...")

        count = 0
        for p in all_people:
            try:
                insert_person(dest_cursor, p)
                count += 1
                if count % 100 == 0:
                    print(f"Processed {count}/{len(all_people)}...")
            except Exception as e:
                print(f"Error inserting {p['name']}: {e}")

        dest_conn.commit()
        dest_conn.close()
        print(f"Import completed. {count} records processed.")


if __name__ == "__main__":
    main()

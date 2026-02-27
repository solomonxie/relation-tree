import sys
import sqlite3
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DB_PATH = "data/db/database.sqlite"


def lookup_person(search_term):
    """Fuzzy search for a person and print their details."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Fuzzy search by Name, ID, or Folder Hash
        query = """
            SELECT id, name, folder_hash, gender, birthdate, notes, created_at
            FROM persons
            WHERE name LIKE ? 
               OR id = ?
               OR folder_hash LIKE ?
        """
        search_pattern = f"%{search_term}%"

        # Try to see if search_term is an integer for ID search
        try:
            person_id = int(search_term)
        except ValueError:
            person_id = -1

        cursor.execute(query, (search_pattern, person_id, search_pattern))
        results = cursor.fetchall()

        if not results:
            print(f"No person found matching '{search_term}'")
            return

        print(f"Found {len(results)} matches:\n")
        for row in results:
            p_id, name, f_hash, gender, birth, notes, created = row

            # Get contact info
            cursor.execute(
                "SELECT type, value FROM contacts WHERE person_id = ?", (p_id,)
            )
            contacts = cursor.fetchall()

            # Get media count
            cursor.execute("SELECT COUNT(*) FROM media WHERE person_id = ?", (p_id,))
            media_count = cursor.fetchone()[0]

            print(f"ID: {p_id}")
            print(f"Name: {name}")
            print(f"Gender: {gender or 'N/A'}")
            print(f"Birthdate: {birth or 'N/A'}")
            print(f"Folder Hash: {f_hash}")
            print(f"Media Count: {media_count}")
            print(f"Notes: {notes or 'N/A'}")

            if contacts:
                print("Contacts:")
                for c_type, c_value in contacts:
                    print(f"  - {c_type}: {c_value}")

            print("-" * 30)

        conn.close()

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        target = " ".join(sys.argv[1:])
    else:
        target = input("Enter person name, ID, or hash: ").strip()

    lookup_person(target)

import os
import sys
import sqlite3
import subprocess
import random
from pathlib import Path
from cryptography.fernet import Fernet
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY")
DB_PATH = "data/db/database.sqlite"


def decrypt_and_open(target_str):
    """Flexible lookup, decryption, and opening of media."""
    if not ENCRYPTION_KEY:
        print("Error: ENCRYPTION_KEY not found in .env")
        sys.exit(1)

    # Extract filename if a path was provided
    input_filename = Path(target_str).name

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Broad fuzzy search:
        # 1. Match the filename from the input path
        # 2. Match the full input string against file_path, original_filename,
        #    person name, person ID, or folder_hash
        query = """
            SELECT m.file_path, m.file_type, m.original_filename, p.name
            FROM media m
            JOIN persons p ON m.person_id = p.id
            WHERE m.file_path LIKE ? 
               OR m.file_path LIKE ?
               OR m.original_filename LIKE ? 
               OR p.name LIKE ?
               OR p.id = ?
               OR p.folder_hash LIKE ?
        """
        search_pattern = f"%{target_str}%"
        filename_pattern = f"%{input_filename}%"

        # Try to see if target_str is an integer for ID search
        try:
            person_id = int(target_str)
        except ValueError:
            person_id = -1

        cursor.execute(
            query,
            (
                filename_pattern,
                search_pattern,
                search_pattern,
                search_pattern,
                person_id,
                search_pattern,
            ),
        )

        results = cursor.fetchall()
        conn.close()

        if not results:
            print(f"Error: No media found matching '{target_str}'")
            sys.exit(1)

        # If multiple results, pick a random one as requested
        if len(results) > 1:
            print(f"Found {len(results)} matches. Choosing a random one...")

        match = random.choice(results)
        target_path_str, extension, original_name, p_name = match
        target_path = Path(target_path_str)

        print("Match found!")
        print(f"  Person: {p_name}")
        print(f"  Original Name: {original_name}")
        print(f"  Storage Path: {target_path}")

        if not target_path.exists():
            print(f"Error: Physical file missing at {target_path}")
            sys.exit(1)

        # Setup decryption
        cipher_suite = Fernet(ENCRYPTION_KEY.encode())

        # Define temp path
        temp_dir = Path("/tmp")
        if not temp_dir.exists():
            import tempfile

            temp_dir = Path(tempfile.gettempdir())

        decrypted_path = temp_dir / f"decrypted_{target_path.name}{extension}"

        # Read, decrypt, and write
        with open(target_path, "rb") as f:
            decrypted_data = cipher_suite.decrypt(f.read())

        with open(decrypted_path, "wb") as f:
            f.write(decrypted_data)

        print(f"Successfully decrypted to: {decrypted_path}")

        # Open file
        if sys.platform == "darwin":
            subprocess.run(["open", str(decrypted_path)])
        elif sys.platform == "win32":
            os.startfile(decrypted_path)
        else:
            subprocess.run(["xdg-open", str(decrypted_path)])

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Join all arguments to handle spaces in names
        target = " ".join(sys.argv[1:])
    else:
        target = input("Enter search term (path, name, ID, or hash): ").strip()

    decrypt_and_open(target)

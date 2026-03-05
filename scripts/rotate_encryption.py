import os
import sys
import sqlite3
from pathlib import Path
from cryptography.fernet import Fernet
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

OLD_KEY = os.getenv("ENCRYPTION_KEY")
DB_PATH = "data/db/database.sqlite"
ENV_PATH = ".env"


def rotate_keys():
    """Rotates the encryption key for all media files."""
    if not OLD_KEY:
        print("Error: No existing ENCRYPTION_KEY found in .env")
        sys.exit(1)

    print(f"Current key: {OLD_KEY}")
    prompt = (
        "Enter new 32-byte base64 encryption key (or press Enter to generate one): "
    )
    new_key_input = input(prompt).strip()

    if not new_key_input:
        new_key = Fernet.generate_key().decode()
        print(f"Generated new key: {new_key}")
    else:
        try:
            # Try using it as a direct Fernet key first
            Fernet(new_key_input.encode())
            new_key = new_key_input
        except Exception:
            # If it's a random phrase, derive a 32-byte key using SHA-256
            import base64
            import hashlib

            hasher = hashlib.sha256()
            hasher.update(new_key_input.encode())
            new_key = base64.urlsafe_b64encode(hasher.digest()).decode()
            print(f"Derived valid Fernet key from your phrase: {new_key}")

    # Initialize ciphers
    old_cipher = Fernet(OLD_KEY.encode())
    new_cipher = Fernet(new_key.encode())

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT id, file_path FROM media WHERE encryption_status = 1")
        rows = cursor.fetchall()

        if not rows:
            print("No encrypted media files found to rotate.")
            return

        print(f"Found {len(rows)} files to re-encrypt...")

        for count, (media_id, file_path_str) in enumerate(rows, 1):
            file_path = Path(file_path_str)
            if not file_path.exists():
                print(f"Warning: File missing, skipping: {file_path_str}")
                continue

            # 1. Read and Decrypt
            with open(file_path, "rb") as f:
                decrypted_data = old_cipher.decrypt(f.read())

            # 2. Encrypt with new key
            re_encrypted_data = new_cipher.encrypt(decrypted_data)

            # 3. Write back
            with open(file_path, "wb") as f:
                f.write(re_encrypted_data)

            if count % 100 == 0:
                print(f"Processed {count} files...")

        conn.close()
        print("All files re-encrypted successfully.")

        # 4. Update .env
        from datetime import datetime

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        with open(ENV_PATH, "r") as f:
            lines = f.readlines()

        new_lines = []
        key_replaced = False
        for line in lines:
            if line.strip().startswith("ENCRYPTION_KEY="):
                # Comment out the exact original line
                new_lines.append(f"# {line.strip()} (rotated on {timestamp})\n")
                new_lines.append(f"ENCRYPTION_KEY={new_key}\n")
                key_replaced = True
            else:
                new_lines.append(line)

        if not key_replaced:
            new_lines.append(f"ENCRYPTION_KEY={new_key}\n")

        with open(ENV_PATH, "w") as f:
            f.writelines(new_lines)

        print(f"Updated {ENV_PATH} with the new key.")
        print("Rotation complete.")

    except Exception as e:
        print(f"Critical Error during rotation: {e}")
        print("Note: Some files might be in an inconsistent state.")
        sys.exit(1)


if __name__ == "__main__":
    rotate_keys()

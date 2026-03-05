import os
import sqlite3
import zipfile
import hashlib
import csv
from pathlib import Path
from cryptography.fernet import Fernet
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY")
if not ENCRYPTION_KEY:
    raise ValueError(
        "ENCRYPTION_KEY not found in .env. "
        "Please ensure it is set before running the script."
    )

cipher_suite = Fernet(ENCRYPTION_KEY.encode())

# Constants
DB_PATH = "data/db/database.sqlite"
SOURCE_DIR = "blobs/user_content_rldt"
MEDIA_DIR = "data/media"
CSV_PATH = os.path.join(SOURCE_DIR, "img-list.csv")


def init_db():
    conn = sqlite3.connect(DB_PATH)
    return conn


def get_file_hash(data):
    return hashlib.md5(data).hexdigest()[:16]


def encrypt_data(data):
    return cipher_suite.encrypt(data)


def load_person_map():
    person_map = {}
    if os.path.exists(CSV_PATH):
        with open(CSV_PATH, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader)  # Skip header
            for row in reader:
                if len(row) >= 2:
                    person_map[row[0]] = row[1]
    return person_map


def process_zips():
    if not os.path.exists(SOURCE_DIR):
        print(f"Source directory {SOURCE_DIR} does not exist.")
        return

    person_map = load_person_map()
    zip_files = list(Path(SOURCE_DIR).glob("*.zip"))
    print(f"Found {len(zip_files)} zip files.")

    conn = init_db()
    cursor = conn.cursor()

    for zip_file in zip_files:
        pid = zip_file.stem
        # Use name from CSV if available, otherwise "Person <pid>"
        actual_name = person_map.get(pid, f"Person {pid}")
        folder_hash = hashlib.md5(f"{actual_name}_{pid}".encode()).hexdigest()[:16]

        # Insert person (OR IGNORE to prevent unique constraint failures)
        cursor.execute(
            "INSERT OR IGNORE INTO persons (name, folder_hash) VALUES (?, ?)",
            (actual_name, folder_hash),
        )
        # If ignore occurred, we need to fetch the existing ID
        if cursor.rowcount == 0:
            cursor.execute("SELECT id FROM persons WHERE folder_hash = ?", (folder_hash,))
            person_id = cursor.fetchone()[0]
        else:
            person_id = cursor.lastrowid

        print(
            f"Processing {actual_name} (PID: {pid}, ID: {person_id}, Hash: {folder_hash})..."
        )

        # Process contents of zip
        with zipfile.ZipFile(zip_file, "r") as z:
            for file_info in z.infolist():
                if file_info.is_dir():
                    continue

                filename = os.path.basename(file_info.filename)
                if filename.startswith(".") or filename == "Thumbs.db":
                    continue

                final_original_name = filename

                with z.open(file_info) as f:
                    file_data = f.read()
                    file_hash_16 = get_file_hash(file_data)
                    
                    # Check if media already exists
                    cursor.execute(
                        "SELECT id FROM media WHERE file_hash = ? AND person_id = ?",
                        (file_hash_16, person_id)
                    )
                    if cursor.fetchone():
                        continue

                    encrypted_data = encrypt_data(file_data)

                    # Determine file type category
                    ext = os.path.splitext(filename)[1].lower()
                    if ext == ".json":
                        continue
                    
                    if ext in [".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp"]:
                        mtype = "image"
                    elif ext in [".mp4", ".mov", ".avi", ".mkv"]:
                        mtype = "video"
                    elif ext in [".mp3", ".wav", ".amr", ".silk", ".aud"]:
                        mtype = "audio"
                    else:
                        mtype = "documents"

                    cursor.execute(
                        """
                        INSERT INTO media (person_id, file_path, file_type, 
                        original_filename, file_hash, encryption_status)
                        VALUES (?, ?, ?, ?, ?, 1)
                    """,
                        (
                            person_id,
                            "PENDING",
                            ext if ext else "unknown",
                            final_original_name,
                            file_hash_16,
                        ),
                    )
                    media_id = cursor.lastrowid

                    # Save encrypted file
                    target_dir = Path(MEDIA_DIR) / folder_hash / mtype
                    target_dir.mkdir(parents=True, exist_ok=True)

                    target_path = target_dir / f"{file_hash_16}{ext}"
                    with open(target_path, "wb") as out_f:
                        out_f.write(encrypted_data)

                    # Update file_path in DB
                    relative_path = os.path.relpath(target_path, MEDIA_DIR)
                    cursor.execute(
                        "UPDATE media SET file_path = ? WHERE id = ?",
                        (relative_path, media_id),
                    )

        conn.commit()

    conn.close()
    print("Processing complete.")


if __name__ == "__main__":
    process_zips()

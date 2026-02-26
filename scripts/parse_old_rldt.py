import os
import sqlite3
import zipfile
import hashlib
import secrets
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


def encrypt_data(data):
    return cipher_suite.encrypt(data)


def get_hash(input_str, length=16):
    return hashlib.sha256(input_str.encode()).hexdigest()[:length]


def get_file_hash(data, length=16):
    return hashlib.sha256(data).hexdigest()[:length]


def load_img_list():
    img_map = {}  # (PID, ID) -> (Name, PictureName)
    person_map = {}  # PID -> Name
    if not os.path.exists(CSV_PATH):
        print(f"Warning: {CSV_PATH} not found.")
        return img_map, person_map

    with open(CSV_PATH, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            pid = row["PID"]
            img_id = row["ID"]
            name = row["Name"]
            pic_name = row["PictureName"]
            img_map[(pid, img_id)] = (name, pic_name)
            if pid not in person_map:
                person_map[pid] = name
    return img_map, person_map


def process_zips():
    img_map, person_map = load_img_list()

    conn = init_db()
    cursor = conn.cursor()

    source_path = Path(SOURCE_DIR)
    media_root = Path(MEDIA_DIR)

    # Clean up previous media and db if re-running
    if media_root.exists():
        import shutil

        shutil.rmtree(media_root)
    media_root.mkdir(parents=True, exist_ok=True)

    zip_files = list(source_path.glob("*.zip"))
    print(f"Found {len(zip_files)} zip files.")

    for zip_file in zip_files:
        pid = zip_file.stem
        # Use name from CSV if available, otherwise "Person <pid>"
        actual_name = person_map.get(pid, f"Person {pid}")
        folder_hash = get_hash(f"{pid}_{secrets.token_hex(4)}")

        # Insert person
        cursor.execute(
            "INSERT INTO persons (name, folder_hash) VALUES (?, ?)",
            (actual_name, folder_hash),
        )
        person_id = cursor.lastrowid

        person_media_dir = media_root / folder_hash
        person_media_dir.mkdir(parents=True, exist_ok=True)

        print(
            f"Processing {actual_name} (PID: {pid}, ID: {person_id}, Hash: {folder_hash})..."
        )

        with zipfile.ZipFile(zip_file, "r") as z:
            for file_info in z.infolist():
                if file_info.is_dir():
                    continue

                original_path = file_info.filename
                filename = os.path.basename(original_path)
                # UUID part usually before extension
                img_uuid = os.path.splitext(filename)[0]

                if filename.startswith("__MACOSX") or filename.startswith("."):
                    continue

                # Lookup info from CSV
                csv_info = img_map.get((pid, img_uuid))
                if csv_info:
                    _, display_name = csv_info
                    # Reconstruct original filename if possible
                    ext = os.path.splitext(filename)[1]
                    final_original_name = f"{display_name}{ext}"
                else:
                    final_original_name = filename

                with z.open(file_info) as f:
                    file_data = f.read()
                    file_hash_16 = get_file_hash(file_data)
                    encrypted_data = encrypt_data(file_data)

                    # Determine file type (extension)
                    ext = os.path.splitext(filename)[1].lower()
                    file_type = ext if ext else "unknown"

                    cursor.execute(
                        """
                        INSERT INTO media (person_id, file_path, file_type, original_filename, file_hash, encryption_status)
                        VALUES (?, ?, ?, ?, ?, 1)
                    """,
                        (
                            person_id,
                            "PENDING",
                            file_type,
                            final_original_name,
                            file_hash_16,
                        ),
                    )

                    media_id = cursor.lastrowid

                    final_filename = f"{media_id}_{file_hash_16}"
                    target_file = person_media_dir / final_filename

                    with open(target_file, "wb") as ef:
                        ef.write(encrypted_data)

                    # Update with final path
                    relative_path = str(target_file)
                    cursor.execute(
                        "UPDATE media SET file_path = ? WHERE id = ?",
                        (relative_path, media_id),
                    )

        conn.commit()

    conn.close()
    print("Processing complete.")


if __name__ == "__main__":
    process_zips()

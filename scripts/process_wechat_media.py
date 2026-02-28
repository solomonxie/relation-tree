import os
import sqlite3
import logging
import hashlib
import subprocess
import base64
import shutil
from pathlib import Path
from cryptography.fernet import Fernet
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Paths
DB_PATH = "data/db/database.sqlite"
WECHAT_MEDIA_DIR = "data/media/wechat_media"
PERSONS_MEDIA_ROOT = "data/media/persons"

MODEL_PATH = os.path.expanduser("~/llm_models/modelscope/models/iic/SenseVoiceSmall")
PYTHON_WITH_FUNASR = "../sermon-voices/venv/bin/python"
OLLAMA_API = "http://localhost:11434/api/generate"
VISION_MODEL = "llava:7b"

ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY")
CIPHER = Fernet(ENCRYPTION_KEY.encode()) if ENCRYPTION_KEY else None


def get_wechat_person_mapping():
    """Returns a dict mapping wechat username/hash to person_id."""
    if not os.path.exists(DB_PATH):
        return {}
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT person_id, value FROM contacts WHERE type = 'wechat'")
    mapping = {row[1]: row[0] for row in cursor.fetchall()}
    conn.close()
    return mapping


def get_person_info(person_id):
    """Returns (name, folder_hash) for a person."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT name, folder_hash FROM persons WHERE id = ?", (person_id,))
    row = cursor.fetchone()
    if row and not row[1]:
        # Generate folder_hash if missing
        folder_hash = hashlib.md5(f"{row[0]}_{person_id}".encode()).hexdigest()[:16]
        cursor.execute(
            "UPDATE persons SET folder_hash = ? WHERE id = ?", (folder_hash, person_id)
        )
        conn.commit()
        row = (row[0], folder_hash)
    conn.close()
    return row


def copy_to_person_media(person_id, src_path, mtype):
    """Copies file to person's media folder and records in persons DB."""
    person_info = get_person_info(person_id)
    if not person_info:
        return None
    name, folder_hash = person_info

    file_ext = Path(src_path).suffix or ".bin"
    with open(src_path, "rb") as f:
        file_data = f.read()

    file_hash = hashlib.md5(file_data).hexdigest()[:16]
    dest_dir = os.path.join(PERSONS_MEDIA_ROOT, folder_hash, "media")
    os.makedirs(dest_dir, exist_ok=True)

    dest_filename = f"{file_hash}{file_ext}"
    dest_path = os.path.join(dest_dir, dest_filename)

    # Encrypt if cipher available
    encryption_status = 0
    if CIPHER:
        encrypted_data = CIPHER.encrypt(file_data)
        with open(dest_path, "wb") as f:
            f.write(encrypted_data)
        encryption_status = 1
    else:
        shutil.copy2(src_path, dest_path)

    # Record in persons DB
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    rel_path = os.path.relpath(dest_path, PERSONS_MEDIA_ROOT)

    cursor.execute(
        "SELECT id FROM media WHERE file_hash = ? AND person_id = ?",
        (file_hash, person_id),
    )
    if not cursor.fetchone():
        cursor.execute(
            """
            INSERT INTO media (person_id, file_path, file_type, original_filename, file_hash, encryption_status)
            VALUES (?, ?, ?, ?, ?, ?)
        """,
            (
                person_id,
                rel_path,
                file_ext,
                os.path.basename(src_path),
                file_hash,
                encryption_status,
            ),
        )
        conn.commit()
    conn.close()
    return rel_path


def get_audio_transcription(audio_abs_path):
    if not os.path.exists(audio_abs_path):
        return None
    temp_script = "temp_transcribe.py"
    with open(temp_script, "w") as f:
        f.write(
            f"""
import sys
import re
from funasr import AutoModel
model = AutoModel(model="{MODEL_PATH}", device="cpu", disable_update=True)
res = model.generate(input="{audio_abs_path}", cache={{}}, language="zh", use_itn=True)
text = re.sub(r'<\|.*?\|>', '', res[0]['text']).strip() if res else ""
# Strip FunASR version info if present
text = re.sub(r'funasr version: .*?\\n', '', text)
text = re.sub(r'funasr version: .*', '', text)
print(text.strip())
"""
        )
    try:
        result = subprocess.run(
            [PYTHON_WITH_FUNASR, temp_script], capture_output=True, text=True
        )
        return result.stdout.strip() if result.returncode == 0 else None
    except Exception as e:
        logging.error(f"Error calling funasr: {e}")
    finally:
        if os.path.exists(temp_script):
            os.remove(temp_script)
    return None


def get_image_info(image_abs_path):
    try:
        from PIL import Image

        with Image.open(image_abs_path) as img:
            width, height = img.size
            filesize = os.path.getsize(image_abs_path)
            return f"{filesize/1024:.1f}KB, {width}x{height}", image_abs_path
    except Exception:
        return "unknown size", None


def get_image_description(image_abs_path):
    """Use Ollama LLava to describe image."""
    try:
        with open(image_abs_path, "rb") as f:
            img_data = base64.b64encode(f.read()).decode("utf-8")

        import requests

        payload = {
            "model": VISION_MODEL,
            "prompt": "Describe this image in one short sentence in English.",
            "stream": False,
            "images": [img_data],
        }
        response = requests.post(OLLAMA_API, json=payload, timeout=60)
        if response.status_code == 200:
            return response.json().get("response", "").strip()
    except Exception as e:
        logging.error(f"Error getting image description: {e}")
    return ""


def get_video_info(video_abs_path):
    try:
        filesize = os.path.getsize(video_abs_path)
        return f"{filesize/(1024*1024):.1f}MB"
    except Exception:
        return "unknown size"


def process_media():
    if not os.path.exists(DB_PATH):
        return
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT id, username, type, relative_path, source FROM wechat_raw_media"
    )
    media_records = cursor.fetchall()

    wechat_person_map = get_wechat_person_mapping()

    total_processed = 0
    for mid, username, mtype, rel_path, source in media_records:
        abs_path = os.path.join(WECHAT_MEDIA_DIR, rel_path)
        if not os.path.exists(abs_path):
            continue

        cursor.execute(
            "SELECT content, media_id FROM wechat_raw_messages WHERE media_path = ?",
            (rel_path,),
        )
        row = cursor.fetchone()
        existing_content, existing_mid = row if row else (None, None)

        # Determine if we need to process
        needs_processing = False
        if not existing_content:
            needs_processing = True
        elif mtype == "audio" and (
            "[语音转文字]" in existing_content or "funasr version" in existing_content
        ):
            needs_processing = True
        elif mtype == "image" and ":" not in existing_content:  # No description yet
            needs_processing = True

        if not needs_processing:
            continue

        logging.info(f"Processing {mtype}: {rel_path}")
        content = existing_content

        if mtype == "audio":
            transcription = get_audio_transcription(abs_path)
            content = (
                f"[Audio Transcription]: {transcription}"
                if transcription
                else "[语音消息]"
            )

            # Copy to persons media if mapped
            person_id = wechat_person_map.get(username)
            if person_id:
                copy_to_person_media(person_id, abs_path, mtype)

        elif mtype == "image":
            info, valid_path = get_image_info(abs_path)
            description = get_image_description(valid_path) if valid_path else ""
            content = f"image({info}): {description}"
        elif mtype == "video":
            content = f"video({get_video_info(abs_path)})"
        else:
            content = f"[{mtype} file]"

        local_id = int(hashlib.md5(rel_path.encode()).hexdigest()[:7], 16) + 1000000000
        if existing_content or row:
            cursor.execute(
                "UPDATE wechat_raw_messages SET content = ?, media_id = ?, message_type = ? WHERE media_path = ?",
                (content, mid, mtype, rel_path),
            )
        else:
            cursor.execute(
                """
                INSERT INTO wechat_raw_messages (username, content, local_id, source, message_type, media_path, media_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (username, content, local_id, source, mtype, rel_path, mid),
            )
        total_processed += 1
        if total_processed % 10 == 0:
            conn.commit()
            logging.info(f"Progress: {total_processed} items...")

    conn.commit()
    conn.close()
    logging.info(f"Finished! Processed {total_processed} items.")


if __name__ == "__main__":
    process_media()

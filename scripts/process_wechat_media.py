import os
import sqlite3
import logging
import hashlib
import subprocess
import re
import json
import base64

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Paths
DB_PATH = "data/db/wechat.sqlite"
MEDIA_DIR = "data/media/wechat_media"
MODEL_PATH = os.path.expanduser("~/llm_models/modelscope/models/iic/SenseVoiceSmall")
PYTHON_WITH_FUNASR = "../sermon-voices/venv/bin/python"
OLLAMA_API = "http://localhost:11434/api/generate"
VISION_MODEL = "llava:7b"

def get_audio_transcription(audio_abs_path):
    if not os.path.exists(audio_abs_path): return None
    temp_script = "temp_transcribe.py"
    with open(temp_script, "w") as f:
        f.write(f"""
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
""")
    try:
        result = subprocess.run([PYTHON_WITH_FUNASR, temp_script], capture_output=True, text=True)
        return result.stdout.strip() if result.returncode == 0 else None
    except Exception as e:
        logging.error(f"Error calling funasr: {e}")
    finally:
        if os.path.exists(temp_script): os.remove(temp_script)
    return None

def get_image_info(image_abs_path):
    try:
        from PIL import Image
        with Image.open(image_abs_path) as img:
            width, height = img.size
            filesize = os.path.getsize(image_abs_path)
            return f"{filesize/1024:.1f}KB, {width}x{height}", image_abs_path
    except Exception: return "unknown size", None

def get_image_description(image_abs_path):
    """Use Ollama LLava to describe image."""
    try:
        with open(image_abs_path, "rb") as f:
            img_data = base64.b64encode(f.read()).decode('utf-8')
        
        import requests
        payload = {
            "model": VISION_MODEL,
            "prompt": "Describe this image in one short sentence in English.",
            "stream": False,
            "images": [img_data]
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
    except Exception: return "unknown size"

def process_media():
    if not os.path.exists(DB_PATH): return
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT id, username, type, relative_path, create_time, source FROM media")
    media_records = cursor.fetchall()

    total_processed = 0
    for mid, username, mtype, rel_path, ctime, source in media_records:
        abs_path = os.path.join(MEDIA_DIR, rel_path)
        if not os.path.exists(abs_path): continue

        cursor.execute("SELECT content, media_id FROM messages WHERE media_path = ?", (rel_path,))
        row = cursor.fetchone()
        existing_content, existing_mid = row if row else (None, None)
        
        # Determine if we need to process (or re-process to add description)
        needs_processing = False
        if not existing_content: needs_processing = True
        elif mtype == "audio" and ("[语音转文字]" in existing_content or "funasr version" in existing_content):
            needs_processing = True
        elif mtype == "image" and ":" not in existing_content: # No description yet
            needs_processing = True
        elif "unknown size" in existing_content:
            needs_processing = True

        if not needs_processing: continue

        logging.info(f"Processing {mtype}: {rel_path}")
        content = existing_content
        
        if mtype == "audio":
            transcription = get_audio_transcription(abs_path)
            content = f"[Audio Transcription]: {transcription}" if transcription else "[语音消息]"
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
                "UPDATE messages SET content = ?, media_id = ?, message_type = ? WHERE media_path = ?",
                (content, mid, mtype, rel_path)
            )
        else:
            cursor.execute(
                """
                INSERT INTO messages (username, create_time, content, local_id, source, message_type, media_path, media_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (username, ctime or 0, content, local_id, source, mtype, rel_path, mid)
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

"""
Group 7: WeChat iOS Backup 2016 Parser
-----------------------
Target: blobs/Wechat3/WechatBackup[2016-03-11]
Analysis: 2016 iOS backup folder.
Features:
1. Parses contacts, messages, moments, and media from iOS backup.
2. Uses local schema file for database initialization.
3. Deduplication based on message hash.
4. Converts WeChat Silk (.aud/.silk) to MP3 using pilk and ffmpeg.
5. Media path: data/media/wechat_media/<contact folder hash>/<fileID>.<ext>
6. Converts .video_thum to .jpg (they are JPEG files).
7. Converts various .pic* and .dftemp formats to .jpg or .png based on file content.
"""

import hashlib
import logging
import os
import shutil
import sqlite3
import sys
import subprocess
import re
from datetime import datetime

# Try to import pilk for Silk decoding
try:
    import pilk
    HAS_PILK = True
except ImportError:
    HAS_PILK = False

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Constants
OUTPUT_DB = "data/db/raw/group7_wechat_ios_2016.sqlite"
SCHEMA_FILE = "data/schema/raw/group7_wechat_ios_2016.sql"
MEDIA_ROOT = "data/media/wechat_media"
# Path to the actual backup folder inside the date-named folder
IOS_BACKUP_DIR = "blobs/Wechat3/WechatBackup[2016-03-11]/2016年03月11日02点24分43秒"


def init_db():
    """Initialize DB using the local schema file."""
    if os.path.exists(OUTPUT_DB):
        os.remove(OUTPUT_DB)
    os.makedirs(os.path.dirname(OUTPUT_DB), exist_ok=True)
    with open(SCHEMA_FILE, 'r') as f:
        schema_sql = f.read()
    conn = sqlite3.connect(OUTPUT_DB)
    conn.executescript(schema_sql)
    conn.commit()
    return conn


def convert_silk_to_mp3(src_path):
    """Converts WeChat Silk (.aud/.silk) to MP3."""
    if not HAS_PILK:
        return src_path
    if not os.path.exists(src_path):
        return src_path
    with open(src_path, "rb") as f:
        header = f.read(10)
    
    is_silk = False
    actual_silk_path = src_path
    temp_silk = None
    if header.startswith(b"#!SILK_V3"):
        is_silk = True
    elif header.startswith(b"\x02#!SILK_V3"):
        is_silk = True
        temp_silk = src_path + ".tmp.silk"
        with open(src_path, "rb") as f:
            f.seek(1)
            with open(temp_silk, "wb") as tf:
                tf.write(f.read())
        actual_silk_path = temp_silk
    if not is_silk:
        return src_path
    dest_path = os.path.splitext(src_path)[0] + ".mp3"
    pcm_path = src_path + ".pcm"
    try:
        pilk.decode(actual_silk_path, pcm_path)
        cmd = ["ffmpeg", "-y", "-loglevel", "error", "-f", "s16le", "-ar", "24000", "-ac", "1", "-i", pcm_path, dest_path]
        subprocess.run(cmd, check=True)
        if os.path.exists(dest_path):
            os.remove(src_path)
        return dest_path
    except Exception as e:
        logging.error(f"Error converting silk {src_path}: {e}")
        return src_path
    finally:
        if os.path.exists(pcm_path): os.remove(pcm_path)
        if temp_silk and os.path.exists(temp_silk): os.remove(temp_silk)


def convert_video_thum_to_jpg(src_path):
    """Renames .video_thum to .jpg."""
    if not src_path.lower().endswith(".video_thum"):
        return src_path
    dest_path = os.path.splitext(src_path)[0] + ".jpg"
    try:
        if not os.path.exists(dest_path):
            os.rename(src_path, dest_path)
        else:
            os.remove(src_path)
        return dest_path
    except Exception as e:
        logging.error(f"Error converting video_thum {src_path}: {e}")
        return src_path


def convert_image(src_path):
    """Converts weird image formats to standard ones."""
    weird_exts = (".pic", ".pic_hd", ".pic_thum", ".pic_mid", ".pic_cmid", ".dftemp")
    if not src_path.lower().endswith(weird_exts):
        return src_path
    if not os.path.exists(src_path):
        return src_path
    with open(src_path, "rb") as f:
        header = f.read(4)
    ext = ".jpg"
    if header.startswith(b"\x89PNG"): ext = ".png"
    elif header.startswith(b"\xff\xd8\xff"): ext = ".jpg"
    elif header.startswith(b"GIF8"): ext = ".gif"
    dest_path = os.path.splitext(src_path)[0] + ext
    try:
        if not os.path.exists(dest_path):
            os.rename(src_path, dest_path)
        else:
            os.remove(src_path)
        return dest_path
    except Exception as e:
        logging.error(f"Error converting image {src_path}: {e}")
        return src_path


def get_file_path(backup_dir, manifest_db, relative_path):
    """Map iOS relative path to fileID."""
    if not os.path.exists(manifest_db):
        return None
    try:
        conn = sqlite3.connect(manifest_db)
        cursor = conn.cursor()
        cursor.execute("SELECT fileID FROM Files WHERE relativePath = ?", (relative_path,))
        row = cursor.fetchone()
        conn.close()
        if row:
            file_id = row[0]
            path = os.path.join(backup_dir, file_id[:2], file_id)
            if os.path.exists(path): return path
            root_path = os.path.join(backup_dir, file_id)
            if os.path.exists(root_path): return root_path
    except Exception as e:
        logging.error(f"Error querying Manifest.db: {e}")
    return None


def clean_blob(blob_data):
    """Extract strings from blobs."""
    if not blob_data or not isinstance(blob_data, bytes): return None
    try:
        matches = re.findall(b'[\x20-\x7E\x80-\xFF]{2,}', blob_data)
        texts = []
        for m in matches:
            try:
                t = m.decode('utf-8', errors='ignore').strip()
                if len(t) > 1 and not all(c.isdigit() or c in '.-_ ' for c in t): texts.append(t)
            except: continue
        return " / ".join(texts) if texts else None
    except: return None


def compute_msg_hash(username, create_time, content):
    """Computes a unique hash."""
    base_str = f"{username}|{create_time}|{content}"
    return hashlib.md5(base_str.encode('utf-8', errors='replace')).hexdigest()


def parse_ios_backup(backup_dir, out_conn):
    """Main parsing logic."""
    logging.info(f"Parsing iOS backup: {backup_dir}")
    manifest_db = os.path.join(backup_dir, "Manifest.db")
    if not os.path.exists(manifest_db):
        logging.warning(f"Manifest.db not found in {backup_dir}")
        return

    out_cursor = out_conn.cursor()
    source_name = f"ios_backup_2016"

    # Identify user hashes
    user_hashes = []
    try:
        conn = sqlite3.connect(manifest_db)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT DISTINCT substr(relativePath, 11, 32) FROM Files "
            "WHERE domain LIKE '%com.tencent.xin%' "
            "AND relativePath LIKE 'Documents/________________________________/%'"
        )
        user_hashes = [row[0] for row in cursor.fetchall() if row[0] and len(row[0]) == 32]
        conn.close()
    except Exception as e:
        logging.error(f"Error finding user hashes: {e}")

    for user_hash in user_hashes:
        logging.info(f"Processing user hash: {user_hash}")
        
        # 1. Contacts
        contact_db_rel = f"Documents/{user_hash}/DB/WCDB_Contact.sqlite"
        contact_db_path = get_file_path(backup_dir, manifest_db, contact_db_rel)
        if contact_db_path:
            conn = sqlite3.connect(contact_db_path)
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT userName, type, dbContactLocal, dbContactRemark FROM Friend")
                for row in cursor.fetchall():
                    uname, utype = row[0], row[1]
                    unick, uremark = clean_blob(row[2]), clean_blob(row[3])
                    out_cursor.execute(
                        "INSERT OR IGNORE INTO group7_raw_contacts (username, type, nickname, remark) VALUES (?, ?, ?, ?)",
                        (uname, utype, unick, uremark)
                    )
                    if unick or uremark:
                        out_cursor.execute("UPDATE group7_raw_contacts SET nickname = ?, remark = ? WHERE username = ?", (unick, uremark, uname))
            except Exception as e: logging.error(f"Error parsing contacts: {e}")
            conn.close()

        # 2. Moments
        wc_db_rel = f"Documents/{user_hash}/wc/wc005_008.db"
        wc_db_path = get_file_path(backup_dir, manifest_db, wc_db_rel)
        if wc_db_path:
            conn = sqlite3.connect(wc_db_path)
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT Id, FromUser, from_nickname, CreateTime, content FROM MyWC_Message01")
                for row in cursor.fetchall():
                    m_hash = compute_msg_hash(row[1], row[3], row[4])
                    out_cursor.execute(
                        "INSERT OR IGNORE INTO group7_raw_moments (id, username, nickname, create_time, content, msg_hash) VALUES (?, ?, ?, ?, ?, ?)",
                        (*row, m_hash)
                    )
            except Exception as e: logging.error(f"Error parsing moments: {e}")
            conn.close()

        # 3. Messages
        fts_db_rel = f"Documents/{user_hash}/fts/fts_message.db"
        fts_db_path = get_file_path(backup_dir, manifest_db, fts_db_rel)
        if fts_db_path:
            conn = sqlite3.connect(fts_db_path)
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT usernameid, UsrName FROM fts_username_id")
                user_map = {row[0]: row[1] for row in cursor.fetchall()}
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'fts_message_table_%_content'")
                for table in [r[0] for r in cursor.fetchall()]:
                    cursor.execute(f"SELECT c0usernameid, c2CreateTime, c3Message, c1MesLocalID FROM {table}")
                    for row in cursor.fetchall():
                        username = user_map.get(row[0], f"unknown_{row[0]}")
                        m_hash = compute_msg_hash(username, row[1], row[2])
                        out_cursor.execute(
                            "INSERT OR IGNORE INTO group7_raw_messages (username, create_time, content, local_id, source, msg_hash) VALUES (?, ?, ?, ?, ?, ?)",
                            (username, row[1], row[2], row[3], source_name, m_hash)
                        )
            except Exception as e: logging.error(f"Error parsing FTS: {e}")
            conn.close()

        # 4. Media
        conn = sqlite3.connect(manifest_db)
        cursor = conn.cursor()
        media_patterns = [(f"Documents/{user_hash}/Audio/%", "audio"), (f"Documents/{user_hash}/Video/%", "video"), (f"Documents/{user_hash}/OpenData/%", "image"), (f"Documents/{user_hash}/Img/%", "image")]
        for pattern, mtype in media_patterns:
            cursor.execute("SELECT fileID, relativePath FROM Files WHERE domain LIKE '%com.tencent.xin%' AND relativePath LIKE ?", (pattern,))
            for fid, rel in cursor.fetchall():
                src_path = os.path.join(backup_dir, fid[:2], fid)
                if not os.path.exists(src_path): src_path = os.path.join(backup_dir, fid)
                if not os.path.exists(src_path): continue
                
                parts = rel.split("/")
                contact_folder_hash = parts[3] if len(parts) >= 5 else user_hash
                file_ext = os.path.splitext(parts[-1])[1]
                if not file_ext:
                    if mtype == "image": file_ext = ".jpg"
                    elif mtype == "video": file_ext = ".mp4"
                    elif mtype == "audio": file_ext = ".aud"
                
                dest_rel_path = os.path.join(contact_folder_hash, fid + file_ext)
                dest_path = os.path.join(MEDIA_ROOT, dest_rel_path)
                os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                
                try:
                    if not os.path.exists(dest_path): shutil.copy2(src_path, dest_path)
                    
                    final_path = dest_path
                    if mtype == "audio" and dest_path.lower().endswith((".aud", ".silk")):
                        final_path = convert_silk_to_mp3(dest_path)
                    elif dest_path.lower().endswith(".video_thum"):
                        final_path = convert_video_thum_to_jpg(dest_path)
                    elif dest_path.lower().endswith((".pic", ".pic_hd", ".pic_thum", ".pic_mid", ".pic_cmid", ".dftemp")):
                        final_path = convert_image(dest_path)
                    
                    rel_path = os.path.relpath(final_path, MEDIA_ROOT)
                    out_cursor.execute(
                        "INSERT OR IGNORE INTO group7_raw_media (id, username, type, relative_path, original_path, file_size, source) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (fid, user_hash, mtype, rel_path, rel, os.path.getsize(final_path), source_name)
                    )
                except Exception as e: logging.error(f"Error media {rel}: {e}")
        conn.close()

def main():
    if not os.path.exists(IOS_BACKUP_DIR): return
    conn = init_db()
    parse_ios_backup(IOS_BACKUP_DIR, conn)
    conn.commit()
    conn.close()
    logging.info("Group 7 parsing finished.")

if __name__ == "__main__":
    main()

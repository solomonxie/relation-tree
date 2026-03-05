"""
Group 4: WeChat iOS Backup Parser
-----------------------
Target: blobs/Wechat2/4c29b1307decf4b1224800b65ab52a877104e9d3/
Analysis: Standard iOS backup folders (generated via iTunes/Finder) containing
a Manifest.db. This is the most complete source for WeChat data, including
contacts, messages, and media.
Features:
1. Parses contacts, messages, moments, and media from iOS backup.
2. Uses local schema file for database initialization.
3. Deduplication based on message hash.
4. Converts WeChat Silk (.aud/.silk) to MP3 using pilk and ffmpeg.
5. Media path: data/media/wechat_media/<contact folder hash>/<fileID>.<ext>
6. Converts .video_thum to .jpg (they are JPEG files).
7. Converts various .pic* and .dftemp formats to .jpg or .png based on file content.
8. Extracts nicknames and remarks from WCDB_Contact.sqlite blobs.
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
OUTPUT_DB = "data/db/raw/group4_wechat_ios.sqlite"
SCHEMA_FILE = "data/schema/raw/group4_wechat_ios.sql"
PARTNERS_MAP_FILE = "data/partners_map.json"
OWNER_NAME = '几何体'
OWNER_ID = 610784125
MEDIA_ROOT = "data/media/wechat_media"
IOS_BACKUP_DIR = "blobs/Wechat2/4c29b1307decf4b1224800b65ab52a877104e9d3"


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


def load_global_mappings():
    """Build global Name <-> ID mappings (placeholder for consistency with group 2)."""
    # WeChat uses string usernames, so the integer OWNER_ID logic from QQ scripts
    # doesn't directly apply here, but we keep the structure for consistency.
    pass


def convert_silk_to_mp3(src_path):
    """Converts WeChat Silk (.aud/.silk) to MP3 using pilk and ffmpeg."""
    if not HAS_PILK:
        logging.warning("pilk not installed, skipping silk conversion.")
        return src_path

    # Check for silk v3 header (might have 0x02 prefix)
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
        # Need to strip the first byte for some decoders
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
        # 1. Decode Silk to PCM
        pilk.decode(actual_silk_path, pcm_path)
        
        # 2. Encode PCM to MP3
        cmd = [
            "ffmpeg", "-y", "-loglevel", "error",
            "-f", "s16le", "-ar", "24000", "-ac", "1",
            "-i", pcm_path,
            dest_path
        ]
        subprocess.run(cmd, check=True)
        # Cleanup original silk in destination if converted
        if os.path.exists(dest_path) and dest_path != src_path:
            os.remove(src_path)
        return dest_path
    except Exception as e:
        logging.error(f"Error converting silk {src_path}: {e}")
        return src_path
    finally:
        if os.path.exists(pcm_path):
            os.remove(pcm_path)
        if temp_silk and os.path.exists(temp_silk):
            os.remove(temp_silk)


def convert_video_thum_to_jpg(src_path):
    """Renames .video_thum to .jpg as they are usually JPEGs."""
    if not src_path.lower().endswith(".video_thum"):
        return src_path
    
    dest_path = os.path.splitext(src_path)[0] + ".jpg"
    try:
        if os.path.exists(dest_path):
            os.remove(src_path)
        else:
            os.rename(src_path, dest_path)
        return dest_path
    except Exception as e:
        logging.error(f"Error converting video_thum {src_path}: {e}")
        return src_path


def convert_image(src_path):
    """Converts .pic, .pic_hd, .pic_thum, .dftemp etc. to .jpg or .png based on content."""
    weird_exts = (".pic", ".pic_hd", ".pic_thum", ".pic_mid", ".pic_cmid", ".dftemp")
    if not src_path.lower().endswith(weird_exts):
        return src_path
    
    if not os.path.exists(src_path):
        return src_path
        
    with open(src_path, "rb") as f:
        header = f.read(4)
    
    ext = ".jpg"
    if header.startswith(b"\x89PNG"):
        ext = ".png"
    elif header.startswith(b"\xff\xd8\xff"):
        ext = ".jpg"
    elif header.startswith(b"GIF8"):
        ext = ".gif"
        
    dest_path = os.path.splitext(src_path)[0] + ext
    try:
        if os.path.exists(dest_path):
            os.remove(src_path)
        else:
            os.rename(src_path, dest_path)
        return dest_path
    except Exception as e:
        logging.error(f"Error converting image {src_path}: {e}")
        return src_path


def get_file_path(backup_dir, manifest_db, relative_path):
    """Map iOS relative path to the actual file ID in the backup folder."""
    if not os.path.exists(manifest_db):
        return None
    try:
        conn = sqlite3.connect(manifest_db)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT fileID FROM Files WHERE relativePath = ?", (relative_path,)
        )
        row = cursor.fetchone()
        conn.close()
        if row:
            file_id = row[0]
            # Newer backups use subfolders based on first 2 chars of fileID
            path = os.path.join(backup_dir, file_id[:2], file_id)
            if os.path.exists(path):
                return path
            # Older backups might have everything in the root
            root_path = os.path.join(backup_dir, file_id)
            if os.path.exists(root_path):
                return root_path
    except Exception as e:
        logging.error(f"Error querying Manifest.db: {e}")
    return None


def clean_blob(blob_data):
    """Extract human readable strings from WeChat BLOBs."""
    if not blob_data or not isinstance(blob_data, bytes):
        return None
    try:
        # Extract sequences of printable characters
        # WeChat often uses protobuf or similar, but names are usually plain strings
        # We look for sequences of at least 2 printable characters
        matches = re.findall(b'[\x20-\x7E\x80-\xFF]{2,}', blob_data)
        if not matches:
            return None
        # Join and clean up
        texts = []
        for m in matches:
            try:
                t = m.decode('utf-8', errors='ignore').strip()
                if len(t) > 1 and not all(c.isdigit() or c in '.-_ ' for c in t):
                    texts.append(t)
            except:
                continue
        return " / ".join(texts) if texts else None
    except:
        return None


def verify_insertion(out_conn, table, source, expected_min=1):
    """Verify that records were inserted for a specific source."""
    cursor = out_conn.cursor()
    query = f"SELECT COUNT(*) FROM {table} WHERE source = ?"
    cursor.execute(query, (source,))
    count = cursor.fetchone()[0]
    logging.info(
        f"Verification: {table} for {source} has {count} records "
        f"(expected ~{expected_min})."
    )
    return count


def compute_msg_hash(username, create_time, content):
    """Computes a unique hash for a message to prevent global duplicates."""
    base_str = f"{username}|{create_time}|{content}"
    return hashlib.md5(base_str.encode('utf-8', errors='replace')).hexdigest()


def parse_ios_backup(backup_dir, out_conn):
    """Main parsing logic for iOS backups."""
    logging.info(f"Parsing iOS backup: {backup_dir}")
    manifest_db = os.path.join(backup_dir, "Manifest.db")
    if not os.path.exists(manifest_db):
        logging.warning(f"Manifest.db not found in {backup_dir}")
        return

    out_cursor = out_conn.cursor()

    # Identify user hashes (32-char hex folders in Documents)
    user_hashes = []
    try:
        conn = sqlite3.connect(manifest_db)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT DISTINCT substr(relativePath, 11, 32) FROM Files "
            "WHERE domain LIKE '%com.tencent.xin%' "
            "AND relativePath LIKE 'Documents/________________________________/%'"
        )
        user_hashes = [
            row[0]
            for row in cursor.fetchall()
            if row[0] and len(row[0]) == 32 and row[0] != "0" * 32
        ]
        conn.close()
    except Exception as e:
        logging.error(f"Error finding user hashes: {e}")

    if not user_hashes:
        logging.warning(f"No user hashes found in {backup_dir}")
        return

    source_name = f"ios_backup_{os.path.basename(backup_dir)}"

    for user_hash in user_hashes:
        logging.info(f"Processing user hash: {user_hash}")

        # 1. Contacts (WCDB_Contact.sqlite)
        contact_db_rel = f"Documents/{user_hash}/DB/WCDB_Contact.sqlite"
        contact_db_path = get_file_path(backup_dir, manifest_db, contact_db_rel)
        if contact_db_path:
            conn = sqlite3.connect(contact_db_path)
            cursor = conn.cursor()
            try:
                # We check for the table and its columns
                cursor.execute("SELECT userName, type, dbContactLocal, dbContactRemark FROM Friend")
                rows = cursor.fetchall()
                for row in rows:
                    uname = row[0]
                    utype = row[1]
                    unick = clean_blob(row[2])
                    uremark = clean_blob(row[3])
                    out_cursor.execute(
                        "INSERT OR IGNORE INTO group4_raw_contacts "
                        "(username, type, nickname, remark) VALUES (?, ?, ?, ?)",
                        (uname, utype, unick, uremark),
                    )
                    # Update if already exists but has no names
                    if unick or uremark:
                        out_cursor.execute(
                            "UPDATE group4_raw_contacts SET nickname = ?, remark = ? "
                            "WHERE username = ?", (unick, uremark, uname),
                        )
                logging.info(f"Processed {len(rows)} contacts.")
            except Exception as e:
                logging.error(f"Error parsing contacts: {e}")
            conn.close()

        # 2. Moments & Nicknames (wc005_008.db)
        wc_db_rel = f"Documents/{user_hash}/wc/wc005_008.db"
        wc_db_path = get_file_path(backup_dir, manifest_db, wc_db_rel)
        if wc_db_path:
            conn = sqlite3.connect(wc_db_path)
            cursor = conn.cursor()
            try:
                # Update nicknames
                cursor.execute(
                    "SELECT DISTINCT FromUser, from_nickname FROM MyWC_Message01 "
                    "WHERE from_nickname IS NOT NULL AND from_nickname != ''"
                )
                for row in cursor.fetchall():
                    out_cursor.execute(
                        "UPDATE group4_raw_contacts SET nickname = ? "
                        "WHERE username = ? AND nickname IS NULL", (row[1], row[0]),
                    )
                    if out_cursor.rowcount == 0:
                        out_cursor.execute(
                            "INSERT OR IGNORE INTO group4_raw_contacts "
                            "(username, nickname) VALUES (?, ?)",
                            (row[0], row[1]),
                        )

                # Insert moments
                cursor.execute(
                    "SELECT Id, FromUser, from_nickname, CreateTime, content "
                    "FROM MyWC_Message01"
                )
                rows = cursor.fetchall()
                for row in rows:
                    m_hash = compute_msg_hash(row[1], row[3], row[4])
                    out_cursor.execute(
                        "INSERT OR IGNORE INTO group4_raw_moments "
                        "(id, username, nickname, create_time, content, msg_hash) "
                        "VALUES (?, ?, ?, ?, ?, ?)", (*row, m_hash),
                    )
                logging.info(f"Processed {len(rows)} moments.")
            except Exception as e:
                logging.error(f"Error parsing moments: {e}")
            conn.close()

        # 3. Messages from FTS (fts_message.db)
        fts_db_rel = f"Documents/{user_hash}/fts/fts_message.db"
        fts_db_path = get_file_path(backup_dir, manifest_db, fts_db_rel)
        total_msgs = 0
        if fts_db_path:
            conn = sqlite3.connect(fts_db_path)
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT usernameid, UsrName FROM fts_username_id")
                user_map = {row[0]: row[1] for row in cursor.fetchall()}
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' "
                    "AND name LIKE 'fts_message_table_%_content'"
                )
                for table in [r[0] for r in cursor.fetchall()]:
                    cursor.execute(
                        f"SELECT c0usernameid, c2CreateTime, c3Message, "
                        f"c1MesLocalID FROM {table}"
                    )
                    rows = cursor.fetchall()
                    for row in rows:
                        username = user_map.get(row[0], f"unknown_{row[0]}")
                        m_hash = compute_msg_hash(username, row[1], row[2])
                        out_cursor.execute(
                            "INSERT OR IGNORE INTO group4_raw_messages "
                            "(username, create_time, content, local_id, "
                            "source, msg_hash) VALUES (?, ?, ?, ?, ?, ?)",
                            (username, row[1], row[2], row[3], source_name, m_hash),
                        )
                    total_msgs += len(rows)
                logging.info(f"Processed {total_msgs} messages.")
            except Exception as e:
                logging.error(f"Error parsing FTS messages: {e}")
            conn.close()
            verify_insertion(
                out_conn, "group4_raw_messages", source_name,
                expected_min=total_msgs
            )

        # 4. Media Mapping
        conn = sqlite3.connect(manifest_db)
        cursor = conn.cursor()
        media_patterns = [
            (f"Documents/{user_hash}/Audio/%", "audio"),
            (f"Documents/{user_hash}/Video/%", "video"),
            (f"Documents/{user_hash}/OpenData/%", "image"),
            (f"Documents/{user_hash}/Img/%", "image"),
        ]
        total_media = 0
        for pattern, mtype in media_patterns:
            cursor.execute(
                "SELECT fileID, relativePath FROM Files "
                "WHERE domain LIKE '%com.tencent.xin%' "
                "AND relativePath LIKE ?", (pattern,),
            )
            for fid, rel in cursor.fetchall():
                src_path = os.path.join(backup_dir, fid[:2], fid)
                if not os.path.exists(src_path):
                    src_path = os.path.join(backup_dir, fid)
                if not os.path.exists(src_path):
                    continue

                parts = rel.split("/")
                # Identify contact folder hash from path
                # Pattern: Documents/{user_hash}/Audio/{contact_hash}/...
                contact_folder_hash = user_hash
                if len(parts) >= 5:
                    contact_folder_hash = parts[3]

                # Get extension from the original backup path
                file_ext = os.path.splitext(parts[-1])[1]
                if not file_ext:
                    if mtype == "image": file_ext = ".jpg"
                    elif mtype == "video": file_ext = ".mp4"
                    elif mtype == "audio": file_ext = ".aud"
                
                dest_filename = fid + file_ext
                dest_rel_path = os.path.join(contact_folder_hash, dest_filename)
                dest_path = os.path.join(MEDIA_ROOT, dest_rel_path)
                os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                
                try:
                    if not os.path.exists(dest_path):
                        shutil.copy2(src_path, dest_path)
                    
                    # Convert silk/aud to mp3 if needed
                    final_dest_path = dest_path
                    final_rel_path = dest_rel_path
                    if mtype == "audio" and dest_path.lower().endswith((".aud", ".silk")):
                        converted_path = convert_silk_to_mp3(dest_path)
                        if converted_path != dest_path:
                            final_dest_path = converted_path
                            final_rel_path = os.path.relpath(final_dest_path, MEDIA_ROOT)
                    elif dest_path.lower().endswith(".video_thum"):
                        converted_path = convert_video_thum_to_jpg(dest_path)
                        if converted_path != dest_path:
                            final_dest_path = converted_path
                            final_rel_path = os.path.relpath(final_dest_path, MEDIA_ROOT)
                    elif dest_path.lower().endswith((".pic", ".pic_hd", ".pic_thum", ".pic_mid", ".pic_cmid", ".dftemp")):
                        converted_path = convert_image(dest_path)
                        if converted_path != dest_path:
                            final_dest_path = converted_path
                            final_rel_path = os.path.relpath(final_dest_path, MEDIA_ROOT)

                    out_cursor.execute(
                        "INSERT OR IGNORE INTO group4_raw_media "
                        "(id, username, type, relative_path, original_path, "
                        "file_size, source) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (fid, user_hash, mtype, final_rel_path, rel,
                         os.path.getsize(final_dest_path), source_name),
                    )
                    total_media += 1
                except Exception as e:
                    logging.error(f"Error copying/converting media {rel}: {e}")
        conn.close()
        verify_insertion(
            out_conn, "group4_raw_media", source_name, expected_min=total_media
        )


def cleanup_old_structures():
    """Removes non-hash folders from MEDIA_ROOT."""
    if not os.path.exists(MEDIA_ROOT):
        return
    logging.info(f"Cleaning up old structures in {MEDIA_ROOT}")
    for item in os.listdir(MEDIA_ROOT):
        item_path = os.path.join(MEDIA_ROOT, item)
        if os.path.isdir(item_path):
            # If not a 32-char hex string, remove it
            if not re.match(r'^[0-9a-f]{32}$', item):
                logging.info(f"Removing old folder: {item}")
                shutil.rmtree(item_path)


def main():
    load_global_mappings()
    backup_dir = sys.argv[1] if len(sys.argv) > 1 else IOS_BACKUP_DIR
    
    if not os.path.exists(backup_dir):
        logging.info(f"Directory not found: {backup_dir}")
        return

    conn = init_db()
    parse_ios_backup(backup_dir, conn)
    conn.commit()
    conn.close()
    
    cleanup_old_structures()
    
    logging.info("iOS Backup parsing and cleanup finished.")


if __name__ == "__main__":
    main()

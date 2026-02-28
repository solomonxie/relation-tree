"""
WeChat iOS Backup Parser
-----------------------
Target: blobs/Wechat2/4c29b1307decf4b1224800b65ab52a877104e9d3/
Analysis: Standard iOS backup folders (generated via iTunes/Finder) containing
a Manifest.db. This is the most complete source for WeChat data, including
contacts, messages, and media.
Destination: wechat_raw_contacts, wechat_raw_messages, wechat_moments, wechat_raw_media
"""

import hashlib
import logging
import os
import shutil
import sqlite3
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Paths
OUTPUT_DB = "data/db/database.sqlite"
MEDIA_OUTPUT_DIR = "data/media/wechat_media"
IOS_BACKUP_DIR = "blobs/Wechat2/4c29b1307decf4b1224800b65ab52a877104e9d3"


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
                cursor.execute("SELECT userName, type FROM Friend")
                rows = cursor.fetchall()
                for row in rows:
                    out_cursor.execute(
                        "INSERT OR IGNORE INTO wechat_raw_contacts "
                        "(username, type) VALUES (?, ?)",
                        row,
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
                        "UPDATE wechat_raw_contacts SET nickname = ? "
                        "WHERE username = ?", (row[1], row[0]),
                    )
                    if out_cursor.rowcount == 0:
                        out_cursor.execute(
                            "INSERT OR IGNORE INTO wechat_raw_contacts "
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
                    out_cursor.execute(
                        "INSERT OR IGNORE INTO wechat_moments "
                        "(id, username, nickname, create_time, content) "
                        "VALUES (?, ?, ?, ?, ?)", row,
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
                        out_cursor.execute(
                            "INSERT OR IGNORE INTO wechat_raw_messages "
                            "(username, create_time, content, local_id, "
                            "source) VALUES (?, ?, ?, ?, ?)",
                            (username, row[1], row[2], row[3], source_name),
                        )
                    total_msgs += len(rows)
                logging.info(f"Processed {total_msgs} messages.")
            except Exception as e:
                logging.error(f"Error parsing FTS messages: {e}")
            conn.close()
            verify_insertion(
                out_conn, "wechat_raw_messages", source_name,
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
                dest_rel = (
                    os.path.join(source_name, mtype, *parts[3:])
                    if len(parts) > 3
                    else os.path.join(source_name, mtype, parts[-1])
                )
                dest_path = os.path.join(MEDIA_OUTPUT_DIR, dest_rel)
                os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                try:
                    if not os.path.exists(dest_path):
                        shutil.copy2(src_path, dest_path)
                    out_cursor.execute(
                        "INSERT OR IGNORE INTO wechat_raw_media "
                        "(id, username, type, relative_path, original_path, "
                        "file_size, source) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (fid, user_hash, mtype, dest_rel, rel,
                         os.path.getsize(dest_path), source_name),
                    )
                    total_media += 1
                except Exception as e:
                    logging.error(f"Error copying media {rel}: {e}")
        conn.close()
        verify_insertion(
            out_conn, "wechat_raw_media", source_name, expected_min=total_media
        )


def main():
    if not os.path.exists(IOS_BACKUP_DIR):
        logging.info(f"Directory not found: {IOS_BACKUP_DIR}")
        return

    os.makedirs(os.path.dirname(OUTPUT_DB), exist_ok=True)
    conn = sqlite3.connect(OUTPUT_DB)
    parse_ios_backup(IOS_BACKUP_DIR, conn)
    conn.commit()
    conn.close()
    logging.info("iOS Backup parsing finished.")


if __name__ == "__main__":
    main()

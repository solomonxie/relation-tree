import hashlib
import logging
import os
import re
import shutil
import sqlite3
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Paths
OUTPUT_DB = "data/db/wechat.sqlite"
SCHEMA_FILE = "data/schema/wechat/wechat.sql"
MEDIA_OUTPUT_DIR = "data/media/wechat_media"


def get_file_path(backup_dir, manifest_db, relative_path):
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
            path = os.path.join(backup_dir, file_id[:2], file_id)
            if os.path.exists(path):
                return path
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
        f"Verification: {table} for {source} has {count} records (found in source: {expected_min})."
    )
    assert count > 0 or expected_min == 0, (
        f"Assertion failed: {table} for {source} is empty but source had {expected_min} records"
    )
    return count


def parse_ios_backup(backup_dir, out_conn):
    logging.info(f"Parsing iOS backup: {backup_dir}")
    manifest_db = os.path.join(backup_dir, "Manifest.db")
    if not os.path.exists(manifest_db):
        logging.warning(f"Manifest.db not found in {backup_dir}")
        return

    out_cursor = out_conn.cursor()

    # Identify user hashes
    user_hashes = []
    try:
        conn = sqlite3.connect(manifest_db)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT DISTINCT substr(relativePath, 11, 32) FROM Files WHERE domain LIKE '%com.tencent.xin%' AND relativePath LIKE 'Documents/________________________________/%'"
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
        return

    source_name = f"ios_backup_{os.path.basename(backup_dir)}"

    for user_hash in user_hashes:
        logging.info(f"Processing user hash: {user_hash}")
        # 1. Contacts
        contact_db_rel = f"Documents/{user_hash}/DB/WCDB_Contact.sqlite"
        contact_db_path = get_file_path(backup_dir, manifest_db, contact_db_rel)
        if contact_db_path:
            conn = sqlite3.connect(contact_db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT userName, type FROM Friend")
            rows = cursor.fetchall()
            for row in rows:
                out_cursor.execute(
                    "INSERT OR IGNORE INTO contacts (username, type) VALUES (?, ?)", row
                )
            conn.close()
            logging.info(f"Processed {len(rows)} contacts from iOS backup.")

        # 2. Nicknames and Moments
        wc_db_rel = f"Documents/{user_hash}/wc/wc005_008.db"
        wc_db_path = get_file_path(backup_dir, manifest_db, wc_db_rel)
        if wc_db_path:
            conn = sqlite3.connect(wc_db_path)
            cursor = conn.cursor()
            cursor.execute(
                "SELECT DISTINCT FromUser, from_nickname FROM MyWC_Message01 WHERE from_nickname IS NOT NULL AND from_nickname != ''"
            )
            for row in cursor.fetchall():
                out_cursor.execute(
                    "UPDATE contacts SET nickname = ? WHERE username = ?",
                    (row[1], row[0]),
                )
                if out_cursor.rowcount == 0:
                    out_cursor.execute(
                        "INSERT OR IGNORE INTO contacts (username, nickname) VALUES (?, ?)",
                        (row[0], row[1]),
                    )

            cursor.execute(
                "SELECT Id, FromUser, from_nickname, CreateTime, content FROM MyWC_Message01"
            )
            rows = cursor.fetchall()
            for row in rows:
                out_cursor.execute(
                    "INSERT OR IGNORE INTO moments (id, username, nickname, create_time, content) VALUES (?, ?, ?, ?, ?)",
                    row,
                )
            conn.close()
            logging.info(f"Processed {len(rows)} moments from iOS backup.")

        # 3. Messages from FTS
        fts_db_rel = f"Documents/{user_hash}/fts/fts_message.db"
        fts_db_path = get_file_path(backup_dir, manifest_db, fts_db_rel)
        total_msgs = 0
        if fts_db_path:
            conn = sqlite3.connect(fts_db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT usernameid, UsrName FROM fts_username_id")
            user_map = {row[0]: row[1] for row in cursor.fetchall()}
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'fts_message_table_%_content'"
            )
            for table in [r[0] for r in cursor.fetchall()]:
                cursor.execute(
                    f"SELECT c0usernameid, c2CreateTime, c3Message, c1MesLocalID FROM {table}"
                )
                rows = cursor.fetchall()
                for row in rows:
                    username = user_map.get(row[0], f"unknown_{row[0]}")
                    out_cursor.execute(
                        "INSERT OR IGNORE INTO messages (username, create_time, content, local_id, source) VALUES (?, ?, ?, ?, ?)",
                        (username, row[1], row[2], row[3], source_name),
                    )
                total_msgs += len(rows)
            conn.close()
            verify_insertion(out_conn, "messages", source_name, expected_min=total_msgs)

        # 4. Media
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
                "SELECT fileID, relativePath FROM Files WHERE domain LIKE '%com.tencent.xin%' AND relativePath LIKE ?",
                (pattern,),
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
                        "INSERT OR IGNORE INTO media (id, username, type, relative_path, original_path, file_size, source) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (
                            fid,
                            user_hash,
                            mtype,
                            dest_rel,
                            rel,
                            os.path.getsize(dest_path),
                            source_name,
                        ),
                    )
                    total_media += 1
                except Exception as e:
                    logging.error(f"Error copying {rel}: {e}")
        conn.close()
        verify_insertion(out_conn, "media", source_name, expected_min=total_media)


def parse_direct_sqlite(sqlite_path, out_conn):
    logging.info(f"Parsing direct SQLite: {sqlite_path}")
    if not os.path.exists(sqlite_path):
        return

    source_name = f"sqlite_{os.path.basename(sqlite_path)}"
    out_cursor = out_conn.cursor()

    conn = sqlite3.connect(sqlite_path)
    cursor = conn.cursor()

    # Parse Contacts
    try:
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='Friend'"
        )
        if cursor.fetchone():
            cursor.execute("PRAGMA table_info(Friend)")
            columns = [c[1] for c in cursor.fetchall()]
            u_col = (
                "userName"
                if "userName" in columns
                else "UsrName"
                if "UsrName" in columns
                else None
            )
            n_col = (
                "nickname"
                if "nickname" in columns
                else "NickName"
                if "NickName" in columns
                else None
            )

            if u_col:
                query = (
                    f"SELECT {u_col}, Type"
                    + (f", {n_col}" if n_col else "")
                    + " FROM Friend"
                )
                cursor.execute(query)
                for row in cursor.fetchall():
                    uname, utype = row[0], row[1]
                    out_cursor.execute(
                        "INSERT OR IGNORE INTO contacts (username, type) VALUES (?, ?)",
                        (uname, utype),
                    )
                    if n_col and row[2]:
                        out_cursor.execute(
                            "UPDATE contacts SET nickname = ? WHERE username = ?",
                            (row[2], uname),
                        )
    except Exception as e:
        logging.error(f"Error parsing contacts from {sqlite_path}: {e}")

    # Parse Messages from Chat_ tables
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'Chat_%' AND name NOT LIKE '%Ext%'"
    )
    chat_tables = [r[0] for r in cursor.fetchall()]
    total_msgs = 0
    for table in chat_tables:
        hash_id = table.replace("Chat_", "")
        try:
            cursor.execute(f"SELECT CreateTime, Message, MesLocalID FROM {table}")
            rows = cursor.fetchall()
            for row in rows:
                out_cursor.execute(
                    "INSERT OR IGNORE INTO messages (username, create_time, content, local_id, source) VALUES (?, ?, ?, ?, ?)",
                    (hash_id, row[0], row[1], row[2], source_name),
                )
            total_msgs += len(rows)
        except Exception as e:
            logging.error(f"Error parsing {table}: {e}")
    conn.close()
    if total_msgs > 0:
        verify_insertion(out_conn, "messages", source_name, expected_min=total_msgs)


def parse_exported_text(export_dir, out_conn):
    logging.info(f"Parsing exported text from: {export_dir}")
    if not os.path.exists(export_dir):
        return

    source_name = "exported_text"
    out_cursor = out_conn.cursor()
    total_msgs = 0

    for filename in os.listdir(export_dir):
        if not filename.endswith(".txt"):
            continue

        username = filename.split("的消息记录")[0]
        file_path = os.path.join(export_dir, filename)

        try:
            with open(file_path, "r", encoding="gbk", errors="replace") as f:
                content = f.read()

            lines = content.split("\n")
            for line in lines:
                match = re.match(
                    r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2})\s+(.*?)\s+(发送|接收)\s+(.*?)\s+(.*)",
                    line,
                )
                if match:
                    dt_str, contact, direction, mtype, msg_content = match.groups()
                    ts = int(datetime.strptime(dt_str, "%Y-%m-%d %H:%M").timestamp())
                    local_id = int(hashlib.md5(line.encode()).hexdigest()[:8], 16)

                    out_cursor.execute(
                        "INSERT OR IGNORE INTO messages (username, create_time, content, local_id, source) VALUES (?, ?, ?, ?, ?)",
                        (username, ts, msg_content.strip(), local_id, source_name),
                    )
                    total_msgs += 1

                    if direction == "接收" and contact != "我":
                        out_cursor.execute(
                            "UPDATE contacts SET nickname = ? WHERE username = ?",
                            (contact, username),
                        )
                        if out_cursor.rowcount == 0:
                            out_cursor.execute(
                                "INSERT OR IGNORE INTO contacts (username, nickname) VALUES (?, ?)",
                                (username, contact),
                            )
        except Exception as e:
            logging.error(f"Error parsing {filename}: {e}")

    if total_msgs > 0:
        verify_insertion(out_conn, "messages", source_name, expected_min=total_msgs)


def main():
    os.makedirs(os.path.dirname(OUTPUT_DB), exist_ok=True)
    out_conn = sqlite3.connect(OUTPUT_DB)
    if os.path.exists(SCHEMA_FILE):
        with open(SCHEMA_FILE, "r") as f:
            out_conn.executescript(f.read())

    # 1. iOS Backup
    parse_ios_backup("blobs/Wechat2/4c29b1307decf4b1224800b65ab52a877104e9d3", out_conn)

    # 2. Direct SQLite files
    wechat1_dir = "blobs/Wechat"
    if os.path.exists(wechat1_dir):
        for f in os.listdir(wechat1_dir):
            if f.endswith(".sqlite"):
                parse_direct_sqlite(os.path.join(wechat1_dir, f), out_conn)

    # 3. Exported Text files
    parse_exported_text("blobs/Wechat2/导出", out_conn)

    out_conn.commit()
    counts = {
        "contacts": out_conn.execute("SELECT COUNT(*) FROM contacts").fetchone()[0],
        "messages": out_conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0],
        "moments": out_conn.execute("SELECT COUNT(*) FROM moments").fetchone()[0],
        "media": out_conn.execute("SELECT COUNT(*) FROM media").fetchone()[0],
    }
    logging.info(f"Finished. Total status: {counts}")
    out_conn.close()


if __name__ == "__main__":
    main()

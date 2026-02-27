import hashlib
import logging
import os
import re
import shutil
import sqlite3
import tempfile
import subprocess
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
    # Don't assert, just log. Some sources might be empty or have duplicates.
    if count == 0 and expected_min > 0:
        logging.warning(f"Likely missed data: {table} for {source} is empty but source had {expected_min} records")
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
        logging.warning(f"No user hashes found in {backup_dir}")
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
            try:
                cursor.execute("SELECT userName, type FROM Friend")
                rows = cursor.fetchall()
                for row in rows:
                    out_cursor.execute(
                        "INSERT OR IGNORE INTO contacts (username, type) VALUES (?, ?)", row
                    )
                logging.info(f"Processed {len(rows)} contacts from iOS backup.")
            except Exception as e:
                logging.error(f"Error parsing contacts from {contact_db_path}: {e}")
            conn.close()

        # 2. Nicknames and Moments
        wc_db_rel = f"Documents/{user_hash}/wc/wc005_008.db"
        wc_db_path = get_file_path(backup_dir, manifest_db, wc_db_rel)
        if wc_db_path:
            conn = sqlite3.connect(wc_db_path)
            cursor = conn.cursor()
            try:
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
                logging.info(f"Processed {len(rows)} moments from iOS backup.")
            except Exception as e:
                logging.error(f"Error parsing moments from {wc_db_path}: {e}")
            conn.close()

        # 3. Messages from FTS
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
                logging.info(f"Processed {total_msgs} messages from iOS backup.")
            except Exception as e:
                logging.error(f"Error parsing FTS messages from {fts_db_path}: {e}")
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

    try:
        conn = sqlite3.connect(sqlite_path)
        cursor = conn.cursor()
    except Exception as e:
        logging.error(f"Error opening SQLite {sqlite_path}: {e}")
        return

    # Check if it's already in our format
    try:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='messages'")
        if cursor.fetchone():
            logging.info(f"Source {sqlite_path} matches our schema, copying directly.")
            # Copy contacts
            cursor.execute("SELECT username, nickname, type FROM contacts")
            for row in cursor.fetchall():
                out_cursor.execute("INSERT OR IGNORE INTO contacts (username, nickname, type) VALUES (?, ?, ?)", row)
            
            # Copy messages
            cursor.execute("SELECT username, create_time, content, local_id FROM messages")
            count = 0
            for row in cursor.fetchall():
                out_cursor.execute(
                    "INSERT OR IGNORE INTO messages (username, create_time, content, local_id, source) VALUES (?, ?, ?, ?, ?)",
                    (*row, source_name)
                )
                count += 1
            verify_insertion(out_conn, "messages", source_name, expected_min=count)
            
            # Copy moments
            cursor.execute("SELECT id, username, nickname, create_time, content FROM moments")
            for row in cursor.fetchall():
                out_cursor.execute("INSERT OR IGNORE INTO moments (id, username, nickname, create_time, content) VALUES (?, ?, ?, ?, ?)", row)
            
            # Copy media
            cursor.execute("SELECT id, username, type, relative_path, original_path, file_size FROM media")
            for row in cursor.fetchall():
                out_cursor.execute(
                    "INSERT OR IGNORE INTO media (id, username, type, relative_path, original_path, file_size, source) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (*row, source_name)
                )
            conn.close()
            return
    except Exception as e:
        logging.warning(f"Error during direct schema copy for {sqlite_path}: {e}")

    # Parse Contacts from Friend table (typical WCDB)
    try:
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND (name='Friend' OR name='Contact')"
        )
        t_row = cursor.fetchone()
        if t_row:
            table_name = t_row[0]
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = [c[1] for c in cursor.fetchall()]
            u_col = next((c for c in columns if c.lower() in ["username", "usrname"]), None)
            n_col = next((c for c in columns if c.lower() in ["nickname"]), None)
            t_col = next((c for c in columns if c.lower() in ["type"]), None)

            if u_col:
                query = f"SELECT {u_col}"
                cols = [u_col]
                if t_col: 
                    query += f", {t_col}"
                    cols.append(t_col)
                if n_col: 
                    query += f", {n_col}"
                    cols.append(n_col)
                query += f" FROM {table_name}"
                
                cursor.execute(query)
                for row in cursor.fetchall():
                    uname = row[0]
                    utype = row[1] if t_col else None
                    unick = row[2] if n_col else None
                    out_cursor.execute(
                        "INSERT OR IGNORE INTO contacts (username, type, nickname) VALUES (?, ?, ?)",
                        (uname, utype, unick),
                    )
                    if unick:
                        out_cursor.execute(
                            "UPDATE contacts SET nickname = ? WHERE username = ?",
                            (unick, uname),
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
            cursor.execute(f"PRAGMA table_info({table})")
            columns = [c[1] for c in cursor.fetchall()]
            m_col = next((c for c in columns if c.lower() in ["message", "content"]), None)
            t_col = next((c for c in columns if c.lower() in ["createtime", "create_time"]), None)
            l_col = next((c for c in columns if c.lower() in ["meslocalid", "localid", "id"]), None)
            
            if m_col and t_col and l_col:
                cursor.execute(f"SELECT {t_col}, {m_col}, {l_col} FROM {table}")
                rows = cursor.fetchall()
                for row in rows:
                    out_cursor.execute(
                        "INSERT OR IGNORE INTO messages (username, create_time, content, local_id, source) VALUES (?, ?, ?, ?, ?)",
                        (hash_id, row[0], row[1], row[2], source_name),
                    )
                total_msgs += len(rows)
        except Exception as e:
            logging.error(f"Error parsing {table} in {sqlite_path}: {e}")
    
    conn.close()
    if total_msgs > 0:
        verify_insertion(out_conn, "messages", source_name, expected_min=total_msgs)


def parse_exported_text(export_dir, out_conn):
    logging.info(f"Parsing exported text from: {export_dir}")
    if not os.path.exists(export_dir):
        return

    source_name = f"exported_text_{os.path.basename(export_dir)}"
    out_cursor = out_conn.cursor()
    total_msgs = 0

    for filename in os.listdir(export_dir):
        if not filename.endswith(".txt"):
            continue

        username = filename.split("的消息记录")[0]
        file_path = os.path.join(export_dir, filename)

        try:
            # Try different encodings
            encodings = ["utf-8", "gbk", "utf-16"]
            content = None
            for enc in encodings:
                try:
                    with open(file_path, "r", encoding=enc) as f:
                        content = f.read()
                    break
                except Exception:
                    continue
            
            if not content:
                logging.error(f"Could not read {filename} with any encoding")
                continue

            lines = content.split("\n")
            for line in lines:
                # Match pattern: 2024-01-01 12:00 Nickname 发送/接收 Message
                match = re.match(
                    r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2})\s+(.*?)\s+(发送|接收)\s+(.*?)\s+(.*)",
                    line,
                )
                if not match:
                    # Match pattern: 2024-01-01 12:00:00 Nickname: Message
                    match = re.match(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+(.*?):\s+(.*)", line)
                
                if match:
                    groups = match.groups()
                    if len(groups) == 5:
                        dt_str, contact, direction, mtype, msg_content = groups
                        fmt = "%Y-%m-%d %H:%M"
                    else:
                        dt_str, contact, msg_content = groups
                        fmt = "%Y-%m-%d %H:%M:%S"
                    
                    try:
                        ts = int(datetime.strptime(dt_str, fmt).timestamp())
                    except Exception:
                        continue
                        
                    local_id = int(hashlib.md5(line.encode()).hexdigest()[:8], 16)

                    out_cursor.execute(
                        "INSERT OR IGNORE INTO messages (username, create_time, content, local_id, source) VALUES (?, ?, ?, ?, ?)",
                        (username, ts, msg_content.strip(), local_id, source_name),
                    )
                    total_msgs += 1

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


def parse_compressed_source(archive_path, out_conn):
    logging.info(f"Parsing compressed source: {archive_path}")
    temp_dir = tempfile.mkdtemp()
    try:
        # Use 7z to extract
        subprocess.run(["7z", "x", f"-o{temp_dir}", archive_path, "-y"], capture_output=True)
        
        # Recursively find and parse files in temp_dir
        for root, dirs, files in os.walk(temp_dir):
            for f in files:
                fpath = os.path.join(root, f)
                if f.endswith(".sqlite") or f.endswith(".db"):
                    parse_direct_sqlite(fpath, out_conn)
                elif f.endswith(".txt"):
                    # For txt, we'll treat the directory as an export dir
                    parse_exported_text(root, out_conn)
                    break # Only parse once per directory if it contains txt files
    except Exception as e:
        logging.error(f"Error extracting/parsing {archive_path}: {e}")
    finally:
        shutil.rmtree(temp_dir)


def append_to_processed_log(counts):
    log_path = "blobs/processed_log.md"
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"\n### ### WeChat Raw Ingestion (Updated {timestamp})\n")
        f.write("- **Description**: Aggregates WeChat messages, contacts, and moments from multiple disparate sources (iOS, PC, Text).\n")
        f.write("- **Source Files**: `blobs/Wechat/`, `blobs/Wechat2/`, `blobs/Wechat3/`, `blobs/old_wechat.sqlite`\n")
        f.write(f"- **Destination**: `{OUTPUT_DB}` (Tables: `messages`, `contacts`, `moments`, `media`)\n")
        f.write(f"- **Status**: {counts['messages']} messages and {counts['contacts']} contacts ingested.\n")
        f.write("- **Processor**: `scripts/parse_wechat.py` -> `main()`\n")
        f.write("- **Example File**: `blobs/Wechat2/4c29b1307decf4b1224800b65ab52a877104e9d3/Manifest.db`\n")
        f.write("- **Example Message**: `what happened?[Shocked]`\n")


def main():
    os.makedirs(os.path.dirname(OUTPUT_DB), exist_ok=True)
    out_conn = sqlite3.connect(OUTPUT_DB)
    if os.path.exists(SCHEMA_FILE):
        with open(SCHEMA_FILE, "r") as f:
            out_conn.executescript(f.read())

    # 1. iOS Backups in blobs/Wechat2
    ios_dir = "blobs/Wechat2/4c29b1307decf4b1224800b65ab52a877104e9d3"
    if os.path.exists(ios_dir):
        parse_ios_backup(ios_dir, out_conn)

    # 2. Exported Text in blobs/Wechat2
    export_dir = "blobs/Wechat2/导出"
    if os.path.exists(export_dir):
        parse_exported_text(export_dir, out_conn)

    # 3. Direct SQLite files in blobs/Wechat
    wechat_dir = "blobs/Wechat"
    if os.path.exists(wechat_dir):
        for f in os.listdir(wechat_dir):
            if f.endswith(".sqlite") or f.endswith(".db"):
                parse_direct_sqlite(os.path.join(wechat_dir, f), out_conn)

    # 4. Old wechat sqlite
    old_sqlite = "blobs/old_wechat.sqlite"
    if os.path.exists(old_sqlite):
        parse_direct_sqlite(old_sqlite, out_conn)

    # 5. Compressed files in blobs/Wechat3
    wechat3_dir = "blobs/Wechat3"
    if os.path.exists(wechat3_dir):
        for f in os.listdir(wechat3_dir):
            if f.endswith((".zip", ".7z", ".rar")):
                parse_compressed_source(os.path.join(wechat3_dir, f), out_conn)

    out_conn.commit()
    counts = {
        "contacts": out_conn.execute("SELECT COUNT(*) FROM contacts").fetchone()[0],
        "messages": out_conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0],
        "moments": out_conn.execute("SELECT COUNT(*) FROM moments").fetchone()[0],
        "media": out_conn.execute("SELECT COUNT(*) FROM media").fetchone()[0],
    }
    logging.info(f"Finished. Total status: {counts}")
    append_to_processed_log(counts)
    out_conn.close()


if __name__ == "__main__":
    main()

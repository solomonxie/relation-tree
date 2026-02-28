"""
WeChat Internal Schema (Legacy) SQLite Parser
---------------------------------------------
Target: blobs/old_wechat.sqlite
Analysis: Databases that match our internal raw schema (e.g. from previous
extraction or migrations). These contain 'wechat_raw_messages', 
'wechat_raw_contacts', 'wechat_moments', and 'wechat_raw_media' tables.
Destination: wechat_raw_contacts, wechat_raw_messages, wechat_moments, wechat_raw_media
"""

import hashlib
import logging
import os
import sqlite3

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Paths
OUTPUT_DB = "data/db/database.sqlite"
OLD_SQLITE = "blobs/old_wechat.sqlite"


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


def parse_internal_sqlite(sqlite_path, out_conn):
    """Migrates data from an internal schema SQLite database."""
    logging.info(f"Migrating internal schema SQLite: {sqlite_path}")
    if not os.path.exists(sqlite_path):
        return

    source_name = f"sqlite_internal_{os.path.basename(sqlite_path)}"
    out_cursor = out_conn.cursor()

    try:
        conn = sqlite3.connect(sqlite_path)
        cursor = conn.cursor()
    except Exception as e:
        logging.error(f"Error opening SQLite {sqlite_path}: {e}")
        return

    # Check for the existence of our internal tables
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND (name='messages' OR name='wechat_raw_messages')"
    )
    if not cursor.fetchone():
        logging.warning(
            f"Source {sqlite_path} does not match internal schema."
        )
        conn.close()
        return

    logging.info(f"Source matches our schema, copying directly.")

    # 1. Copy Contacts
    try:
        cursor.execute(
            "SELECT username, nickname, type FROM wechat_raw_contacts"
        )
    except Exception:
        cursor.execute("SELECT username, nickname, type FROM contacts")

    for row in cursor.fetchall():
        out_cursor.execute(
            "INSERT OR IGNORE INTO wechat_raw_contacts "
            "(username, nickname, type) VALUES (?, ?, ?)", row,
        )

    # 2. Copy Messages
    try:
        cursor.execute(
            "SELECT username, create_time, content, local_id "
            "FROM wechat_raw_messages"
        )
    except Exception:
        cursor.execute(
            "SELECT username, create_time, content, local_id FROM messages"
        )
    count = 0
    for row in cursor.fetchall():
        m_hash = compute_msg_hash(row[0], row[1], row[2])
        out_cursor.execute(
            "INSERT OR IGNORE INTO wechat_raw_messages "
            "(username, create_time, content, local_id, source, msg_hash) "
            "VALUES (?, ?, ?, ?, ?, ?)", (*row, source_name, m_hash),
        )
        count += 1
    verify_insertion(
        out_conn, "wechat_raw_messages", source_name, expected_min=count
    )

    # 3. Copy Moments
    try:
        cursor.execute(
            "SELECT id, username, nickname, create_time, content "
            "FROM wechat_moments"
        )
    except Exception:
        cursor.execute(
            "SELECT id, username, nickname, create_time, content FROM moments"
        )
    for row in cursor.fetchall():
        m_hash = compute_msg_hash(row[1], row[3], row[4])
        out_cursor.execute(
            "INSERT OR IGNORE INTO wechat_moments "
            "(id, username, nickname, create_time, content, msg_hash) "
            "VALUES (?, ?, ?, ?, ?, ?)", (*row, m_hash),
        )

    # 4. Copy Media
    try:
        cursor.execute(
            "SELECT id, username, type, relative_path, original_path, "
            "file_size FROM wechat_raw_media"
        )
    except Exception:
        cursor.execute(
            "SELECT id, username, type, relative_path, original_path, "
            "file_size FROM media"
        )
    for row in cursor.fetchall():
        out_cursor.execute(
            "INSERT OR IGNORE INTO wechat_raw_media "
            "(id, username, type, relative_path, original_path, "
            "file_size, source) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (*row, source_name),
        )

    conn.close()


def main():
    if not os.path.exists(OLD_SQLITE):
        logging.info(f"File not found: {OLD_SQLITE}")
        return

    os.makedirs(os.path.dirname(OUTPUT_DB), exist_ok=True)
    conn = sqlite3.connect(OUTPUT_DB)
    parse_internal_sqlite(OLD_SQLITE, conn)
    conn.commit()
    conn.close()
    logging.info("WeChat Internal SQLite migration finished.")


if __name__ == "__main__":
    main()

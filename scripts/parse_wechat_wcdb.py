"""
WeChat WCDB (Standard) SQLite Parser
------------------------------------
Target: blobs/Wechat/ (MM*.sqlite)
Analysis: Standard WeChat (WCDB) SQLite databases typically extracted from
mobile devices. These contain standard 'Friend', 'Contact', and 'Chat_[hash]'
tables.
Destination: wechat_raw_contacts, wechat_raw_messages
"""

import hashlib
import logging
import os
import sqlite3
from setup_db import setup_db

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Paths
OUTPUT_DB = "data/db/raw/wechat_wcdb.sqlite"
WECHAT_DIR = "blobs/Wechat"


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


def parse_wcdb_sqlite(sqlite_path, out_conn):
    """Parses standard WeChat (WCDB) message and contact tables."""
    logging.info(f"Parsing WCDB SQLite: {sqlite_path}")
    if not os.path.exists(sqlite_path):
        return

    source_name = f"sqlite_wcdb_{os.path.basename(sqlite_path)}"
    out_cursor = out_conn.cursor()

    try:
        conn = sqlite3.connect(sqlite_path)
        cursor = conn.cursor()
    except Exception as e:
        logging.error(f"Error opening SQLite {sqlite_path}: {e}")
        return

    # 1. Parse Contacts from Friend/Contact table
    try:
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND (name='Friend' OR name='Contact')"
        )
        t_row = cursor.fetchone()
        if t_row:
            table_name = t_row[0]
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = [c[1] for c in cursor.fetchall()]
            u_col = next(
                (c for c in columns if c.lower() in ["username", "usrname"]),
                None
            )
            n_col = next((c for c in columns if c.lower() in ["nickname"]), None)
            t_col = next((c for c in columns if c.lower() in ["type"]), None)

            if u_col:
                query = f"SELECT {u_col}"
                if t_col:
                    query += f", {t_col}"
                if n_col:
                    query += f", {n_col}"
                query += f" FROM {table_name}"

                cursor.execute(query)
                for row in cursor.fetchall():
                    uname = row[0]
                    utype = row[1] if t_col else None
                    unick = row[2] if n_col else None
                    out_cursor.execute(
                        "INSERT OR IGNORE INTO wechat_raw_contacts "
                        "(username, type, nickname) VALUES (?, ?, ?)",
                        (uname, utype, unick),
                    )
                    if unick:
                        out_cursor.execute(
                            "UPDATE wechat_raw_contacts SET nickname = ? "
                            "WHERE username = ?", (unick, uname),
                        )
    except Exception as e:
        logging.error(f"Error parsing contacts: {e}")

    # 2. Parse Messages from Chat_ tables
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND name LIKE 'Chat_%' AND name NOT LIKE '%Ext%'"
    )
    chat_tables = [r[0] for r in cursor.fetchall()]
    total_msgs = 0
    for table in chat_tables:
        hash_id = table.replace("Chat_", "")
        try:
            cursor.execute(f"PRAGMA table_info({table})")
            columns = [c[1] for c in cursor.fetchall()]
            m_col = next(
                (c for c in columns if c.lower() in ["message", "content"]),
                None
            )
            t_col = next(
                (c for c in columns if c.lower() in ["createtime", "create_time"]),
                None
            )
            l_col = next(
                (c for c in columns if c.lower() in ["meslocalid", "localid", "id"]),
                None
            )

            if m_col and t_col and l_col:
                cursor.execute(f"SELECT {t_col}, {m_col}, {l_col} FROM {table}")
                rows = cursor.fetchall()
                for row in rows:
                    m_hash = compute_msg_hash(hash_id, row[0], row[1])
                    out_cursor.execute(
                        "INSERT OR IGNORE INTO wechat_raw_messages "
                        "(username, create_time, content, local_id, source, "
                        "msg_hash) VALUES (?, ?, ?, ?, ?, ?)",
                        (hash_id, row[0], row[1], row[2], source_name, m_hash),
                    )
                total_msgs += len(rows)
        except Exception as e:
            logging.error(f"Error parsing {table}: {e}")

    conn.close()
    if total_msgs > 0:
        verify_insertion(
            out_conn, "wechat_raw_messages", source_name,
            expected_min=total_msgs
        )


def main():
    if not os.path.exists(WECHAT_DIR):
        logging.info(f"Directory not found: {WECHAT_DIR}")
        return

    setup_db(OUTPUT_DB)
    conn = sqlite3.connect(OUTPUT_DB)

    for f in os.listdir(WECHAT_DIR):
        if f.endswith(".sqlite") or f.endswith(".db"):
            parse_wcdb_sqlite(os.path.join(WECHAT_DIR, f), conn)

    conn.commit()
    conn.close()
    logging.info("WeChat WCDB SQLite parsing finished.")


if __name__ == "__main__":
    main()

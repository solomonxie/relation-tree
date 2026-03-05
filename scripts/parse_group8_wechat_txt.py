"""
Group 8: WeChat Manual Text Export Parser
-------------------------------
Target: blobs/Wechat_txt/
Analysis: Text-based chat logs with Beijing time (UTC+8).
Features:
1. Parses date, nickname, status, type, content.
2. Uses local schema file for database initialization.
3. Deduplication based on message hash (normalized to UTC).
"""

import hashlib
import logging
import os
import re
import sqlite3
from datetime import datetime, timedelta, timezone

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Paths
OUTPUT_DB = "data/db/raw/group8_wechat_txt.sqlite"
SCHEMA_FILE = "data/schema/raw/group8_wechat_txt.sql"
EXPORT_DIR = "blobs/Wechat_txt"


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


def compute_msg_hash(username, create_time, content):
    """Computes a unique hash for a message."""
    base_str = f"{username}|{create_time}|{content}"
    return hashlib.md5(base_str.encode('utf-8', errors='replace')).hexdigest()


def parse_exported_text(export_dir, out_conn):
    """Parses text chat logs from the WeChat export directory."""
    logging.info(f"Parsing exported text from: {export_dir}")
    if not os.path.exists(export_dir):
        return

    out_cursor = out_conn.cursor()
    total_msgs = 0
    beijing_tz = timezone(timedelta(hours=8))

    for filename in os.listdir(export_dir):
        if not filename.endswith(".txt"):
            continue

        username = filename.split("的消息记录")[0]
        file_path = os.path.join(export_dir, filename)

        try:
            content = None
            for enc in ["utf-8", "gbk", "utf-16"]:
                try:
                    with open(file_path, "r", encoding=enc) as f:
                        content = f.read()
                    break
                except: continue

            if not content:
                logging.error(f"Could not read {filename}")
                continue

            lines = content.splitlines()
            for line in lines:
                # Format: 2018-06-15 11:34        Nickname                  Status                        Type                         Content
                match = re.match(
                    r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2})\s+(.*?)\s+(发送|接收|未知类型)\s+(.*?)\s+(.*)",
                    line,
                )
                if match:
                    dt_str, contact, direction, mtype, msg_content = match.groups()
                    try:
                        dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M").replace(tzinfo=beijing_tz)
                        ts = int(dt.timestamp())
                    except: continue

                    msg_content = msg_content.strip()
                    m_hash = compute_msg_hash(username, ts, msg_content)

                    out_cursor.execute(
                        "INSERT OR IGNORE INTO group8_raw_messages "
                        "(username, create_time, content, source, "
                        "msg_hash) VALUES (?, ?, ?, ?, ?)",
                        (username, ts, msg_content, filename, m_hash),
                    )
                    total_msgs += 1

                    # Log contact
                    out_cursor.execute(
                        "INSERT OR IGNORE INTO group8_raw_contacts "
                        "(username, nickname) VALUES (?, ?)",
                        (username, contact),
                    )
        except Exception as e:
            logging.error(f"Error parsing {filename}: {e}")

    logging.info(f"Inserted {total_msgs} messages into {OUTPUT_DB}")


def main():
    if not os.path.exists(EXPORT_DIR):
        return

    conn = init_db()
    parse_exported_text(EXPORT_DIR, conn)
    conn.commit()
    conn.close()
    logging.info("WeChat Manual Text Export parsing finished.")


if __name__ == "__main__":
    main()

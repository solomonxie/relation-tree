"""
WeChat Manual Text Export Parser
-------------------------------
Target: blobs/Wechat2/导出/
Analysis: Text-based chat logs exported directly from the WeChat app or
via 3rd party tools. These files follow standard formats but often have
different encodings (UTF-8, GBK, UTF-16).
Destination: wechat_raw_messages, wechat_raw_contacts
"""

import hashlib
import logging
import os
import re
import sqlite3
from datetime import datetime
from setup_db import setup_db

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Paths
OUTPUT_DB = "data/db/raw/group8_wechat_txt.sqlite"
EXPORT_DIR = "blobs/Wechat2/导出"


def compute_msg_hash(username, create_time, content):
    """Computes a unique hash for a message to prevent global duplicates."""
    base_str = f"{username}|{create_time}|{content}"
    return hashlib.md5(base_str.encode('utf-8', errors='replace')).hexdigest()


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


def parse_exported_text(export_dir, out_conn):
    """Parses text chat logs from the WeChat export directory."""
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
                logging.error(f"Could not read {filename} with any encoding.")
                continue

            lines = content.split("\n")
            for line in lines:
                # Regex for common WeChat export formats
                match = re.match(
                    r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2})\s+(.*?)\s+"
                    r"(发送|接收)\s+(.*?)\s+(.*)",
                    line,
                )
                if not match:
                    # Alternative simplified format
                    match = re.match(
                        r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+(.*?):\s+(.*)",
                        line
                    )

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
                    m_hash = compute_msg_hash(username, ts, msg_content.strip())

                    out_cursor.execute(
                        "INSERT OR IGNORE INTO wechat_raw_messages "
                        "(username, create_time, content, local_id, source, "
                        "msg_hash) VALUES (?, ?, ?, ?, ?, ?)",
                        (username, ts, msg_content.strip(), local_id,
                         source_name, m_hash),
                    )
                    total_msgs += 1

                    # Log contact/nickname
                    out_cursor.execute(
                        "UPDATE wechat_raw_contacts SET nickname = ? "
                        "WHERE username = ?", (contact, username),
                    )
                    if out_cursor.rowcount == 0:
                        out_cursor.execute(
                            "INSERT OR IGNORE INTO wechat_raw_contacts "
                            "(username, nickname) VALUES (?, ?)",
                            (username, contact),
                        )
        except Exception as e:
            logging.error(f"Error parsing {filename}: {e}")

    if total_msgs > 0:
        verify_insertion(
            out_conn, "wechat_raw_messages", source_name,
            expected_min=total_msgs
        )


def main():
    if not os.path.exists(EXPORT_DIR):
        logging.info(f"Export directory not found: {EXPORT_DIR}")
        return

    setup_db(OUTPUT_DB)
    conn = sqlite3.connect(OUTPUT_DB)
    parse_exported_text(EXPORT_DIR, conn)
    conn.commit()
    conn.close()
    logging.info("WeChat Manual Text Export parsing finished.")


if __name__ == "__main__":
    main()

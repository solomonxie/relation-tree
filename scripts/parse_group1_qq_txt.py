"""
Group 1: QQ TXT Parser
Source: blobs/qq_txt/*.txt
Features:
1. Parses QQ chat logs from individual text files.
2. Uses local schema file for database setup.
3. Correctly handles sender names, nicknames, and timestamps.
"""

import hashlib
import logging
import os
import re
import sqlite3
import sys
from datetime import datetime
from glob import glob

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Constants
OUTPUT_DB = "data/db/raw/group1_qq_txt.sqlite"
SCHEMA_FILE = "data/schema/raw/group1_qq_txt.sql"
OWNER_NAME = '几何体'


def compute_msg_hash(sender, create_time, content):
    """Computes a unique hash for a message to prevent global duplicates."""
    base_str = f"{sender}|{create_time}|{content}"
    return hashlib.md5(base_str.encode('utf-8', errors='replace')).hexdigest()


def clean_msg(msg):
    # Remove styling tags
    msg = re.sub(r"[^>\n]*'(?:MS Sans Serif|Tahoma|宋体|微软雅黑|Arial|Times New Roman)'[^>\n]*>", "", msg)
    # Generic cleanup for remaining font/color markers
    msg = re.sub(r"<font[^>]*>|</font>", "", msg)
    return msg.strip()


def init_db():
    """Initialize DB using the local schema file."""
    os.makedirs(os.path.dirname(OUTPUT_DB), exist_ok=True)
    with open(SCHEMA_FILE, 'r') as f:
        schema_sql = f.read()

    conn = sqlite3.connect(OUTPUT_DB)
    cursor = conn.cursor()
    cursor.executescript(schema_sql)
    conn.commit()
    logging.info(f"Database initialized at {OUTPUT_DB} using {SCHEMA_FILE}")
    return conn


def parse_file(filepath, cursor):
    logging.info(f"Processing {filepath}")
    filename = os.path.basename(filepath)
    # Filename format: Name_QQID.txt
    parts = filename.replace('.txt', '').split('_')
    contact_name = parts[0]
    qqid = parts[1] if len(parts) > 1 else contact_name

    date_ = '1900-01-01'
    time_ = '00:00:00'
    raw_sender = contact_name
    last_content_end_idx = -1

    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        lines = f.read().splitlines()

    i, msg_start = 0, 0
    total_msgs = 0
    while i <= len(lines):
        line = lines[i].strip() if i < len(lines) else "日期: END"
        time_match = re.match(r'^(\d{1,2}:\d{2}:\d{2})$', line) if i < len(lines) else None

        if line.startswith('日期:') or time_match:
            if msg_start > 0:
                end_idx = i - 2 if time_match else i - 1
                msg = clean_msg('\n'.join(lines[msg_start:end_idx+1]))
                last_content_end_idx = end_idx
                if msg:
                    if "窗口抖动" in msg:
                        if "您发送了一个" in msg:
                            raw_sender = OWNER_NAME
                        elif "给您发送了一个" in msg:
                            raw_sender = contact_name
                    username = OWNER_NAME if raw_sender == OWNER_NAME else contact_name
                    nickname = raw_sender
                    if date_ != '1900-01-01' and time_ != '00:00:00':
                        ts_str = f"{date_} {time_}"
                        ts = int(datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").timestamp())
                        m_hash = compute_msg_hash(username, ts, msg)
                        cursor.execute(
                            "INSERT OR IGNORE INTO group1_qq_txt_raw_chats "
                            "(source_file, username, nickname, create_time, content, platform, subfolder, msg_hash) "
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                            (filepath, username, nickname, ts, msg, "qq_txt", qqid, m_hash)
                        )
                        total_msgs += 1
            if i >= len(lines):
                break

            if line.startswith('日期:'):
                date_match = re.search(r'(\d{4}-\d{2}-\d{2})', line)
                if not date_match and i + 1 < len(lines):
                    date_match = re.search(r'(\d{4}-\d{2}-\d{2})', lines[i+1])
                    if date_match:
                        i += 1
                if date_match:
                    date_ = date_match.group(1)
                msg_start = 0
            else:
                new_time = time_match.group(1)
                if len(new_time) == 7:
                    new_time = "0" + new_time

                potential_sender_idx = i - 1
                if potential_sender_idx > last_content_end_idx:
                    potential_sender = lines[potential_sender_idx].strip()
                    if potential_sender and not re.match(r'^(\d{1,2}:\d{2}:\d{2})$', potential_sender) and "窗口抖动" not in potential_sender:
                        raw_sender = potential_sender

                time_ = new_time
                msg_start = i + 1
        i += 1
    return total_msgs


def main():
    if len(sys.argv) > 1:
        files = [sys.argv[1]]
    else:
        files = glob("blobs/qq_txt/*.txt")

    if not files:
        logging.warning("No files found to process.")
        return

    logging.info(f"Found {len(files)} files to process.")

    # Initialize DB using local schema file
    conn = init_db()
    cursor = conn.cursor()

    total_extracted = 0
    for filepath in sorted(files):
        try:
            total_extracted += parse_file(filepath, cursor)
            if total_extracted % 1000 == 0:
                conn.commit()
        except Exception as e:
            logging.error(f"Failed to process {filepath}: {e}")

    conn.commit()
    conn.close()
    logging.info(f"Processing complete. Total messages: {total_extracted}")


if __name__ == '__main__':
    main()

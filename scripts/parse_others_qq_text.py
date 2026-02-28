"""
Other Chat QQ Text Parser
-------------------------
Target: blobs/others/ (QQ_chat_history_archive*.txt)
Analysis: Massive consolidated text files or individual text exports
from QQ messenger. These are human-readable but require multi-line
regex parsing for date, time, and sender.
Destination: other_raw_chats
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
OUTPUT_DB = "data/db/raw/others_qq_text.sqlite"
OTHERS_DIR = "blobs/others"


def compute_msg_hash(username, create_time, content):
    """Computes a unique hash for a message to prevent global duplicates."""
    base_str = f"{username}|{create_time}|{content}"
    return hashlib.md5(base_str.encode('utf-8', errors='replace')).hexdigest()


def parse_qq_text_chat(file_path, cursor, subfolder):
    """Parses QQ multi-line text export format."""
    logging.info(f"Parsing QQ text export: {file_path}")
    try:
        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()

        current_user = None
        current_ts = None
        current_content = []
        total_msgs = 0
        # Format: 2026-02-27 16:30:00 Username
        header_re = re.compile(
            r"^(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})\s+(.*)$"
        )

        for line in lines:
            line = line.strip()
            if not line:
                continue
            match = header_re.match(line)
            if match:
                # Save previous message
                if current_user and current_ts and current_content:
                    msg_content = "\n".join(current_content)
                    m_hash = compute_msg_hash(current_user, current_ts, msg_content)
                    cursor.execute(
                        "INSERT OR IGNORE INTO other_raw_chats "
                        "(source_file, username, create_time, content, "
                        "platform, subfolder, msg_hash) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (file_path, current_user, current_ts,
                         msg_content, "qq_text", subfolder, m_hash),
                    )
                    total_msgs += 1

                # Start new message
                current_ts = int(
                    datetime.strptime(
                        f"{match.group(1)} {match.group(2)}",
                        "%Y-%m-%d %H:%M:%S"
                    ).timestamp()
                )
                current_user = match.group(3).strip()
                current_content = []
            else:
                if current_user:
                    current_content.append(line)

        # Save last message
        if current_user and current_ts and current_content:
            msg_content = "\n".join(current_content)
            m_hash = compute_msg_hash(current_user, current_ts, msg_content)
            cursor.execute(
                "INSERT OR IGNORE INTO other_raw_chats "
                "(source_file, username, create_time, content, platform, "
                "subfolder, msg_hash) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (file_path, current_user, current_ts,
                 msg_content, "qq_text", subfolder, m_hash),
            )
            total_msgs += 1
        logging.info(f"Extracted {total_msgs} messages.")
    except Exception as e:
        logging.error(f"Error parsing QQ text {file_path}: {e}")


def main():
    if not os.path.exists(OTHERS_DIR):
        logging.info(f"Others directory not found: {OTHERS_DIR}")
        return

    setup_db(OUTPUT_DB)
    conn = sqlite3.connect(OUTPUT_DB)
    cursor = conn.cursor()

    for item in os.listdir(OTHERS_DIR):
        item_path = os.path.join(OTHERS_DIR, item)
        if os.path.isfile(item_path) and "QQ" in item and item.endswith(".txt"):
            parse_qq_text_chat(item_path, cursor, "root")

    # Also check QQ subdirectory
    qq_dir = os.path.join(OTHERS_DIR, "QQ")
    if os.path.exists(qq_dir):
        for f in os.listdir(qq_dir):
            if f.endswith(".txt"):
                parse_qq_text_chat(os.path.join(qq_dir, f), cursor, "QQ")

    conn.commit()
    conn.close()
    logging.info("QQ Text parsing finished.")


if __name__ == "__main__":
    main()

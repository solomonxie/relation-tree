"""
Other Chat Binary .bak Parser
-----------------------------
Target: blobs/others/QQ/, blobs/others/Tencent TT/
Analysis: Legacy binary backup files from the QQ messenger and Tencent TT
browser. These are not directly readable but contain plaintext strings
mixed with binary metadata. Handled using the 'strings' utility.
Destination: other_raw_chats
"""

import hashlib
import logging
import os
import re
import sqlite3
from setup_db import setup_dbimport subprocess
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Paths
OUTPUT_DB = "data/db/raw/others_bak.sqlite"
OTHERS_DIR = "blobs/others"


def compute_msg_hash(username, create_time, content):
    """Computes a unique hash for a message to prevent global duplicates."""
    base_str = f"{username}|{create_time}|{content}"
    return hashlib.md5(base_str.encode('utf-8', errors='replace')).hexdigest()


def parse_metadata_only(file_path, cursor, subfolder):
    """Logs metadata for files where content cannot be parsed."""
    filename = os.path.basename(file_path)
    ext = os.path.splitext(filename)[1].lower()
    platform = f"{ext[1:]}_metadata"
    username = (
        filename.split("(")[0].strip()
        if "(" in filename
        else filename.replace(ext, "")
    )
    mtime = int(os.path.getmtime(file_path))
    content = f"[Metadata Only] Chat log: {filename}"
    m_hash = compute_msg_hash(username, mtime, content)

    cursor.execute(
        "INSERT OR IGNORE INTO other_raw_chats "
        "(source_file, username, create_time, content, platform, subfolder, msg_hash) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (file_path, username, mtime, content, platform, subfolder, m_hash),
    )


def parse_bak_chat(file_path, cursor, subfolder):
    """Extracts human-readable strings from binary .bak files."""
    logging.info(f"Attempting string extraction from .bak: {file_path}")
    try:
        # Run 'strings' utility to extract plain text
        result = subprocess.run(
            ["strings", file_path], capture_output=True, text=True
        )
        if result.returncode != 0:
            parse_metadata_only(file_path, cursor, subfolder)
            return

        text = result.stdout
        filename = os.path.basename(file_path)
        username = (
            filename.split("(")[0].strip()
            if "(" in filename
            else filename.replace(".bak", "")
        )

        lines = text.splitlines()
        total_msgs = 0
        for line in lines:
            # Look for timestamps embedded in lines
            match = re.search(r"(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})", line)
            if match:
                ts_str = f"{match.group(1)} {match.group(2)}"
                content = line.replace(ts_str, "").strip()
                if content:
                    try:
                        ts = int(
                            datetime.strptime(
                                ts_str, "%Y-%m-%d %H:%M:%S"
                            ).timestamp()
                        )
                        m_hash = compute_msg_hash(username, ts, content)
                        cursor.execute(
                            "INSERT OR IGNORE INTO other_raw_chats "
                            "(source_file, username, create_time, content, "
                            "platform, subfolder, msg_hash) VALUES (?, ?, ?, ?, ?, ?, ?)",
                            (file_path, username, ts, content, "bak_strings",
                             subfolder, m_hash),
                        )
                        total_msgs += 1
                    except Exception:
                        continue
        if total_msgs == 0:
            parse_metadata_only(file_path, cursor, subfolder)
        else:
            logging.info(f"Extracted {total_msgs} messages.")
    except Exception as e:
        logging.error(f"Error parsing .bak {file_path}: {e}")
        parse_metadata_only(file_path, cursor, subfolder)


def main():
    if not os.path.exists(OTHERS_DIR):
        logging.info(f"Others directory not found: {OTHERS_DIR}")
        return

    setup_db(OUTPUT_DB)
    conn = sqlite3.connect(OUTPUT_DB)
    cursor = conn.cursor()

    # Walk through OTHERS_DIR and identify subdirectories
    for item in os.listdir(OTHERS_DIR):
        item_path = os.path.join(OTHERS_DIR, item)
        if os.path.isdir(item_path):
            for root, _, files in os.walk(item_path):
                for f in files:
                    if f.lower().endswith(".bak"):
                        parse_bak_chat(os.path.join(root, f), cursor, item)
        elif item.lower().endswith(".bak"):
            parse_bak_chat(item_path, cursor, "root")

    conn.commit()
    conn.close()
    logging.info("QQ .bak parsing finished.")


if __name__ == "__main__":
    main()

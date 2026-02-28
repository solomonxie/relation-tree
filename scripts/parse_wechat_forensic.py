"""
WeChat Forensic/Android Export Parser
-------------------------------------
Target: blobs/wechat_20260627/0c50ac31fe2ee3c890e9219c3e7c3ac2/
Analysis: Unpacked forensic or Android backups organized under hash-named
folders. These typically include ChatPackage, Index, and Media subfolders,
often encrypted or in proprietary formats.
Destination: wechat_raw_contacts, other_raw_chats
"""

import hashlib
import logging
import os
import sqlite3
from datetime import datetime
from setup_db import setup_db

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Paths
OUTPUT_DB = "data/db/raw/wechat_forensic.sqlite"
FORENSIC_DIR = "blobs/wechat_20260627"


def compute_msg_hash(username, create_time, content):
    """Computes a unique hash for a message to prevent global duplicates."""
    base_str = f"{username}|{create_time}|{content}"
    return hashlib.md5(base_str.encode('utf-8', errors='replace')).hexdigest()


def parse_forensic_wechat(forensic_dir, out_conn):
    """Logs metadata for encrypted forensic WeChat sources."""
    logging.info(f"Parsing forensic WeChat backup: {forensic_dir}")
    if not os.path.exists(forensic_dir):
        return

    out_cursor = out_conn.cursor()
    total_found = 0

    # Path to the specific nested files
    files_dir = os.path.join(
        forensic_dir, "0c50ac31fe2ee3c890e9219c3e7c3ac2", "files", "1"
    )

    if os.path.exists(files_dir):
        for user_hash in os.listdir(files_dir):
            user_path = os.path.join(files_dir, user_hash)
            if os.path.isdir(user_path):
                # Log this user hash as a contact source
                out_cursor.execute(
                    "INSERT OR IGNORE INTO wechat_raw_contacts "
                    "(username, nickname, type) VALUES (?, ?, ?)",
                    (user_hash, f"Forensic User {user_hash[:8]}", 0),
                )

                # Track presence of encrypted data in other_raw_chats
                content = f"[Encrypted Forensic Data] Hash: {user_hash}"
                ts = int(datetime.now().timestamp())
                m_hash = compute_msg_hash(user_hash, ts, content)
                
                out_cursor.execute(
                    "INSERT OR IGNORE INTO other_raw_chats "
                    "(source_file, username, create_time, content, "
                    "platform, subfolder, msg_hash) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (user_path, user_hash, ts, content,
                     "forensic_encrypted", "wechat_20260627", m_hash),
                )
                total_found += 1

    logging.info(f"Logged {total_found} forensic user directories.")


def main():
    if not os.path.exists(FORENSIC_DIR):
        logging.info(f"Forensic directory not found: {FORENSIC_DIR}")
        return

    setup_db(OUTPUT_DB)
    conn = sqlite3.connect(OUTPUT_DB)
    parse_forensic_wechat(FORENSIC_DIR, conn)
    conn.commit()
    conn.close()
    logging.info("WeChat Forensic metadata logging finished.")


if __name__ == "__main__":
    main()

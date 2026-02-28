"""
Other Chat Generic/Web Parser
-----------------------------
Target: blobs/HTML_CHATS, blobs/MHTML_CHATS, blobs/TXT_CHATS
Analysis: Miscellaneous chat logs in various formats (web-archived
single-file MHTML, zipped HTML, plain text).
Each group outputs to a unique table in others_generic.sqlite.
"""

import hashlib
import logging
import os
import re
import sqlite3
from datetime import datetime
from html.parser import HTMLParser

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Paths
OUTPUT_DB = "data/db/raw/group6_txt.sqlite"
HTML_DIR = "blobs/HTML_CHATS"
MHTML_DIR = "blobs/MHTML_CHATS"
TXT_DIR = "blobs/TXT_CHATS"

def compute_msg_hash(sender_id, create_time, content):
    """Computes a unique hash for a message to prevent global duplicates."""
    base_str = f"{sender_id}|{create_time}|{content}"
    return hashlib.md5(base_str.encode('utf-8', errors='replace')).hexdigest()

def split_user_info(user_str):
    """Splits 'Name(ID)' into (Name, ID)."""
    if not user_str:
        return None, None
    match = re.search(r"^(.*?)\((\d+)\)$", user_str)
    if match:
        return match.group(1), match.group(2)
    return user_str, None

class QQHTMLParser(HTMLParser):
    """Basic parser for QQ MHTML/HTML chat logs."""
    def __init__(self):
        super().__init__()
        self.messages = []
        self.current_date = None
        self.current_user = None
        self.current_time = None
        self.recording_content = False
        self.content_parts = []

    def handle_data(self, data):
        data = data.strip()
        if not data:
            return

        # Detect Date header
        if "日期:" in data:
            match = re.search(r"(\d{4}-\d{2}-\d{2})", data)
            if match:
                self.current_date = match.group(1)
            return

        # Detect User and Time (Format: "Username 12:34:56")
        time_match = re.search(r"(\d{2}:\d{2}:\d{2})$", data)
        if time_match:
            self.current_time = time_match.group(1)
            self.current_user = data.replace(self.current_time, "").strip()
            self.recording_content = True
            self.content_parts = []
            return

        if self.recording_content:
            self.content_parts.append(data)

    def handle_endtag(self, tag):
        if tag in ["div", "td"] and self.recording_content:
            if self.current_date and self.current_user and self.current_time and self.content_parts:
                content = " ".join(self.content_parts).strip()
                try:
                    ts_str = f"{self.current_date} {self.current_time}"
                    ts = int(datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").timestamp())
                    self.messages.append((self.current_user, ts, content))
                except Exception:
                    pass
                self.recording_content = False
                self.content_parts = []

def init_raw_table(cursor, table_name):
    """Creates a standardized raw table for message groups."""
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    cursor.execute(f"""
    CREATE TABLE {table_name} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_file TEXT,
        sender_name TEXT,
        sender_id TEXT,
        create_time INTEGER,
        content TEXT,
        platform TEXT,
        subfolder TEXT,
        msg_hash TEXT
    )
    """)
    cursor.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_name}_hash ON {table_name} (msg_hash)")

def parse_html_group(directory, cursor, table_name, platform):
    """Parses a directory of HTML/MHTML files into a specific table."""
    if not os.path.exists(directory):
        logging.warning(f"Directory not found: {directory}")
        return

    init_raw_table(cursor, table_name)
    subfolder = os.path.basename(directory)

    for filename in os.listdir(directory):
        if not (filename.endswith(".htm") or filename.endswith(".html") or filename.endswith(".mht")):
            continue
        
        file_path = os.path.join(directory, filename)
        logging.info(f"Parsing {platform} chat: {file_path}")
        
        try:
            # Simple read for now, MHTML should ideally use email lib but applying HTML logic first
            with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                html_content = f.read()

            parser = QQHTMLParser()
            parser.feed(html_content)

            for user_str, ts, content in parser.messages:
                name, uid = split_user_info(user_str)
                m_hash = compute_msg_hash(uid or name, ts, content)
                cursor.execute(
                    f"INSERT OR IGNORE INTO {table_name} "
                    "(source_file, sender_name, sender_id, create_time, content, platform, "
                    "subfolder, msg_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (file_path, name, uid, ts, content, platform, subfolder, m_hash),
                )
            logging.info(f"Extracted {len(parser.messages)} messages from {filename}")
        except Exception as e:
            logging.error(f"Error parsing {filename}: {e}")

def main():
    # 1. Database Reset (Delete file)
    if os.path.exists(OUTPUT_DB):
        os.remove(OUTPUT_DB)
        logging.info(f"Deleted existing database: {OUTPUT_DB}")

    os.makedirs(os.path.dirname(OUTPUT_DB), exist_ok=True)
    conn = sqlite3.connect(OUTPUT_DB)
    cursor = conn.cursor()

    # Group 1: HTML_CHATS
    parse_html_group(HTML_DIR, cursor, "group1_raw_html", "html_parsed")

    # Group 2: MHTML_CHATS
    parse_html_group(MHTML_DIR, cursor, "group2_raw_mhtml", "html_parsed")

    conn.commit()
    conn.close()
    logging.info("Generic and Web Chat parsing finished.")

if __name__ == "__main__":
    main()

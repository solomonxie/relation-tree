"""
Other Chat Generic/Web Parser
-----------------------------
Target: blobs/others/ (Generic .txt, .mht, .html, .zip)
Analysis: Miscelaneous chat logs in various formats (web-archived
single-file MHTML, zipped HTML, plain text).
Destination: other_raw_chats
"""

import hashlib
import logging
import os
import re
import sqlite3
from datetime import datetime
from setup_db import setup_db
from html.parser import HTMLParser

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Paths
OUTPUT_DB = "data/db/raw/others_generic.sqlite"
OTHERS_DIR = "blobs/others"


def compute_msg_hash(username, create_time, content):
    """Computes a unique hash for a message to prevent global duplicates."""
    base_str = f"{username}|{create_time}|{content}"
    return hashlib.md5(base_str.encode('utf-8', errors='replace')).hexdigest()


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

    def handle_starttag(self, tag, attrs):
        # If we see a new block starting and we were recording content, save it
        if tag in ["div", "tr", "td"] and self.recording_content and self.content_parts:
            # This is a bit simplistic but works for the observed structure
            pass

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


def parse_metadata_only(file_path, cursor, subfolder):
    """Logs metadata for files where full content parsing is not implemented."""
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


def parse_html_chat(file_path, cursor, subfolder):
    """Parses MHTML/HTML chat logs using a custom HTML parser."""
    logging.info(f"Parsing HTML/MHTML chat: {file_path}")
    try:
        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            html_content = f.read()

        parser = QQHTMLParser()
        parser.feed(html_content)

        if not parser.messages:
            parse_metadata_only(file_path, cursor, subfolder)
            return

        for user, ts, content in parser.messages:
            m_hash = compute_msg_hash(user, ts, content)
            cursor.execute(
                "INSERT OR IGNORE INTO other_raw_chats "
                "(source_file, username, create_time, content, platform, "
                "subfolder, msg_hash) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (file_path, user, ts, content, "html_parsed", subfolder, m_hash),
            )
        logging.info(f"Extracted {len(parser.messages)} messages from {file_path}")
    except Exception as e:
        logging.error(f"Error parsing HTML {file_path}: {e}")
        parse_metadata_only(file_path, cursor, subfolder)


def parse_txt_chat(file_path, cursor, subfolder):
    """Parses simple line-by-line text chat logs."""
    platform = "generic_txt"
    try:
        filename = os.path.basename(file_path)
        username = (
            filename.split("(")[0].strip()
            if "(" in filename
            else filename.replace(".txt", "")
        )

        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                # Basic YYYY-MM-DD HH:MM:SS format detection
                if (
                    len(line) > 19
                    and line[4] == "-"
                    and line[7] == "-"
                    and line[13] == ":"
                ):
                    try:
                        ts_str = line[:19]
                        ts = int(
                            datetime.strptime(
                                ts_str, "%Y-%m-%d %H:%M:%S"
                            ).timestamp()
                        )
                        content = line[19:].strip()
                        m_hash = compute_msg_hash(username, ts, content)
                        cursor.execute(
                            "INSERT OR IGNORE INTO other_raw_chats "
                            "(source_file, username, create_time, content, "
                            "platform, subfolder, msg_hash) VALUES (?, ?, ?, ?, ?, ?, ?)",
                            (file_path, username, ts, content, platform,
                             subfolder, m_hash),
                        )
                    except Exception:
                        continue
    except Exception as e:
        logging.error(f"Error parsing {file_path}: {e}")


def main():
    if not os.path.exists(OTHERS_DIR):
        logging.info(f"Others directory not found: {OTHERS_DIR}")
        return

    setup_db(OUTPUT_DB)
    conn = sqlite3.connect(OUTPUT_DB)
    cursor = conn.cursor()

    for item in os.listdir(OTHERS_DIR):
        item_path = os.path.join(OTHERS_DIR, item)
        if os.path.isdir(item_path):
            for root, _, files in os.walk(item_path):
                for f in files:
                    if f.startswith("."):
                        continue
                    fpath = os.path.join(root, f)
                    ext = os.path.splitext(f)[1].lower()
                    if ext == ".txt":
                        parse_txt_chat(fpath, cursor, item)
                    elif ext in [".mht", ".mhtl", ".html"]:
                        parse_html_chat(fpath, cursor, item)
                    elif ext in [".zip", ".docx"]:
                        parse_metadata_only(fpath, cursor, item)
        else:
            if item.startswith("."):
                continue
            ext = os.path.splitext(item)[1].lower()
            if ext == ".txt":
                if "QQ" not in item:
                    parse_txt_chat(item_path, cursor, "root")
            elif ext in [".mht", ".mhtl", ".html"]:
                parse_html_chat(item_path, cursor, "root")
            elif ext in [".zip", ".docx"]:
                parse_metadata_only(item_path, cursor, "root")

    conn.commit()
    conn.close()
    logging.info("Generic and Web Chat parsing finished.")


if __name__ == "__main__":
    main()

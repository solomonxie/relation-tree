"""
Group 2: QQ MHTML Chat Parser
Target: blobs/MHTML_CHATS/*.mht
Output: data/db/raw/group2_mhtml.sqlite (group2_raw_mhtml)
"""

import hashlib
import logging
import os
import re
import sqlite3
import email
from datetime import datetime
from html.parser import HTMLParser

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

OUTPUT_DB = "data/db/raw/group2_mhtml.sqlite"
MHTML_DIR = "blobs/MHTML_CHATS"

def compute_msg_hash(sender_id, create_time, content):
    base_str = f"{sender_id}|{create_time}|{content}"
    return hashlib.md5(base_str.encode('utf-8', errors='replace')).hexdigest()

def split_user_info(user_str):
    if not user_str: return None, None
    match = re.search(r"^(.*?)\((\d+)\)$", user_str)
    if match: return match.group(1), match.group(2)
    return user_str, None

class QQHTMLParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.messages, self.current_date, self.current_user, self.current_time = [], None, None, None
        self.recording_content, self.content_parts = False, []

    def handle_data(self, data):
        data = data.strip()
        if not data: return
        if "日期:" in data:
            match = re.search(r"(\d{4}-\d{2}-\d{2})", data)
            if match: self.current_date = match.group(1)
        time_match = re.search(r"(\d{2}:\d{2}:\d{2})$", data)
        if time_match:
            self.current_time = time_match.group(1)
            self.current_user = data.replace(self.current_time, "").strip()
            self.recording_content, self.content_parts = True, []
        elif self.recording_content:
            self.content_parts.append(data)

    def handle_endtag(self, tag):
        if tag in ["div", "td"] and self.recording_content:
            if self.current_date and self.current_user and self.current_time and self.content_parts:
                content = " ".join(self.content_parts).strip()
                try:
                    ts = int(datetime.strptime(f"{self.current_date} {self.current_time}", "%Y-%m-%d %H:%M:%S").timestamp())
                    self.messages.append((self.current_user, ts, content))
                except: pass
                self.recording_content, self.content_parts = False, []

def main():
    os.makedirs(os.path.dirname(OUTPUT_DB), exist_ok=True)
    conn = sqlite3.connect(OUTPUT_DB)
    cursor = conn.cursor()
    
    table_name = "group2_raw_mhtml"
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (id INTEGER PRIMARY KEY AUTOINCREMENT, source_file TEXT, sender_name TEXT, sender_id TEXT, create_time INTEGER, content TEXT, platform TEXT, subfolder TEXT, msg_hash TEXT)")
    
    if os.path.exists(MHTML_DIR):
        for filename in os.listdir(MHTML_DIR):
            if not filename.endswith(".mht"): continue
            file_path = os.path.join(MHTML_DIR, filename)
            
            with open(file_path, "rb") as f:
                msg = email.message_from_binary_file(f)
                html_content = ""
                for part in msg.walk():
                    if part.get_content_type() == "text/html":
                        html_content = part.get_payload(decode=True).decode('utf-8', 'ignore')
                        break
                
                if html_content:
                    parser = QQHTMLParser()
                    parser.feed(html_content)
                    for user_str, ts, content in parser.messages:
                        name, uid = split_user_info(user_str)
                        cursor.execute(f"INSERT OR IGNORE INTO {table_name} (source_file, sender_name, sender_id, create_time, content, platform, subfolder, msg_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                                       (file_path, name, uid, ts, content, "html_parsed", "MHTML_CHATS", compute_msg_hash(uid or name, ts, content)))
            logging.info(f"Processed {filename}")
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()

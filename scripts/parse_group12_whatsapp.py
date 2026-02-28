"""
Group 12: WhatsApp Chat Parser
Target: blobs/WhatsApp Chat - Jenny/_chat.txt
Output: data/db/raw/group12_whatsapp.sqlite (group12_raw_whatsapp)
"""

import hashlib
import logging
import os
import re
import sqlite3
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

OUTPUT_DB = "data/db/raw/group12_whatsapp.sqlite"
WHATSAPP_FILE = "blobs/WhatsApp Chat - Jenny/_chat.txt"

def compute_msg_hash(sender_id, create_time, content):
    base_str = f"{sender_id}|{create_time}|{content}"
    return hashlib.md5(base_str.encode('utf-8', errors='replace')).hexdigest()

def main():
    if os.path.exists(OUTPUT_DB):
        os.remove(OUTPUT_DB)
        logging.info(f"Deleted existing database: {OUTPUT_DB}")

    os.makedirs(os.path.dirname(OUTPUT_DB), exist_ok=True)
    conn = sqlite3.connect(OUTPUT_DB)
    cursor = conn.cursor()
    
    table_name = "group12_raw_whatsapp"
    cursor.execute(f"CREATE TABLE {table_name} (id INTEGER PRIMARY KEY AUTOINCREMENT, source_file TEXT, sender_name TEXT, sender_id TEXT, create_time INTEGER, content TEXT, platform TEXT, subfolder TEXT, msg_hash TEXT)")
    
    if os.path.exists(WHATSAPP_FILE):
        logging.info(f"Parsing WhatsApp chat: {WHATSAPP_FILE}")
        with open(WHATSAPP_FILE, "r", encoding="utf-8", errors="replace") as f:
            messages = []
            # Pattern: [DD/MM/YYYY, HH:MM:SS] User: Content
            pattern = re.compile(r"^\[(\d{2}/\d{2}/\d{4}, \d{2}:\d{2}:\d{2})\] (.*?): (.*)")
            
            current_msg = None
            for line in f:
                match = pattern.match(line)
                if match:
                    if current_msg:
                        messages.append(current_msg)
                    
                    ts_str, sender, content = match.groups()
                    try:
                        ts = int(datetime.strptime(ts_str, "%d/%m/%Y, %H:%M:%S").timestamp())
                        current_msg = {
                            "sender_name": sender,
                            "sender_id": None,
                            "create_time": ts,
                            "content": content.strip()
                        }
                    except Exception:
                        current_msg = None
                elif current_msg:
                    # Append multiline content
                    current_msg["content"] += "\n" + line.strip()
            
            if current_msg:
                messages.append(current_msg)

            for m in messages:
                m_hash = compute_msg_hash(m["sender_name"], m["create_time"], m["content"])
                cursor.execute(f"INSERT OR IGNORE INTO {table_name} (source_file, sender_name, sender_id, create_time, content, platform, subfolder, msg_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                               (WHATSAPP_FILE, m["sender_name"], m["sender_id"], m["create_time"], m["content"], "whatsapp_txt", "WhatsApp Chat - Jenny", m_hash))
            
            logging.info(f"Extracted {len(messages)} messages.")
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()

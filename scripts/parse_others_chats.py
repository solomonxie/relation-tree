import os
import sqlite3
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

CHATS_DB = "data/db/chats.sqlite"
MAIN_DB = "data/db/database.sqlite"
OTHERS_DIR = "blobs/others"

def setup_chats_db(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS raw_chats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_file TEXT,
        username TEXT,
        create_time INTEGER,
        content TEXT,
        platform TEXT
    )
    """)

def parse_txt_chat(file_path, cursor):
    """Example parser for a simple txt format."""
    platform = "generic_txt"
    with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
        for line in f:
            # Assume [YYYY-MM-DD HH:MM:SS] User: Message
            if line.startswith('['):
                try:
                    parts = line.split(']', 1)
                    ts_str = parts[0][1:]
                    ts = int(datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S').timestamp())
                    rest = parts[1].split(':', 1)
                    user = rest[0].strip()
                    content = rest[1].strip()
                    cursor.execute("INSERT INTO raw_chats (source_file, username, create_time, content, platform) VALUES (?, ?, ?, ?, ?)",
                                   (file_path, user, ts, content, platform))
                except:
                    continue

def merge_to_main():
    main_conn = sqlite3.connect(MAIN_DB)
    main_cursor = main_conn.cursor()
    main_cursor.execute("""
    CREATE TABLE IF NOT EXISTS other_messages (
        person_id INTEGER PRIMARY KEY,
        history TEXT,
        FOREIGN KEY (person_id) REFERENCES persons(id)
    )
    """)

    chats_conn = sqlite3.connect(CHATS_DB)
    chats_cursor = chats_conn.cursor()
    
    chats_cursor.execute("SELECT DISTINCT username, platform FROM raw_chats")
    users = chats_cursor.fetchall()
    
    for username, platform in users:
        # Try to map to person
        main_cursor.execute("SELECT id FROM persons WHERE name = ? OR nick_name = ?", (username, username))
        row = main_cursor.fetchone()
        if not row:
            logging.warning(f"No person found for user: {username}. Creating new person.")
            main_cursor.execute("INSERT INTO persons (name) VALUES (?)", (username,))
            person_id = main_cursor.lastrowid
        else:
            person_id = row[0]

        chats_cursor.execute("SELECT create_time, content FROM raw_chats WHERE username = ? AND platform = ? ORDER BY create_time", (username, platform))
        msgs = chats_cursor.fetchall()
        
        history_lines = []
        for ts, content in msgs:
            dt = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
            history_lines.append(f"[{dt}] [{platform}] {username}: {content}")
        
        new_history = "\n".join(history_lines)

        # Merge with existing history if any
        main_cursor.execute(
            "SELECT history FROM other_messages WHERE person_id = ?", (person_id,)
        )
        old_row = main_cursor.fetchone()
        if old_row:
            combined = old_row[0] + "\n" + new_history
        else:
            combined = new_history

        main_cursor.execute(
            "INSERT OR REPLACE INTO other_messages (person_id, history) VALUES (?, ?)",
            (person_id, combined),
        )

    main_conn.commit()
    main_conn.close()
    chats_conn.close()

def main():
    if not os.path.exists(OTHERS_DIR):
        os.makedirs(OTHERS_DIR, exist_ok=True)
    
    conn = sqlite3.connect(CHATS_DB)
    cursor = conn.cursor()
    setup_chats_db(cursor)

    for root, dirs, files in os.walk(OTHERS_DIR):
        for f in files:
            fpath = os.path.join(root, f)
            if f.endswith('.txt'):
                parse_txt_chat(fpath, cursor)
            # Add more parsers for other formats (json, csv, etc)
    
    conn.commit()
    conn.close()
    
    merge_to_main()
    logging.info("Others chats parsed and merged.")

if __name__ == "__main__":
    main()

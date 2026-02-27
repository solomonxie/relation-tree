import sqlite3
import os
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

MAIN_DB = "data/db/database.sqlite"
WECHAT_DB = "data/db/wechat.sqlite"

def setup_table(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS wechat_messages (
        person_id INTEGER PRIMARY KEY,
        history TEXT,
        FOREIGN KEY (person_id) REFERENCES persons(id)
    )
    """)

def get_wechat_mapping(main_cursor):
    main_cursor.execute("SELECT person_id, value FROM contacts WHERE type = 'wechat'")
    return {row[1]: row[0] for row in main_cursor.fetchall()}

def merge():
    if not os.path.exists(WECHAT_DB):
        logging.error(f"WeChat DB not found: {WECHAT_DB}")
        return

    main_conn = sqlite3.connect(MAIN_DB)
    main_cursor = main_conn.cursor()
    setup_table(main_cursor)
    
    wechat_map = get_wechat_mapping(main_cursor)
    
    wechat_conn = sqlite3.connect(WECHAT_DB)
    wechat_cursor = wechat_conn.row_factory = sqlite3.Row
    wechat_cursor = wechat_conn.cursor()
    
    # Get all unique usernames from messages
    wechat_cursor.execute("SELECT DISTINCT username FROM messages")
    usernames = [row[0] for row in wechat_cursor.fetchall()]
    
    logging.info(f"Merging messages for {len(usernames)} WeChat contacts...")
    
    for username in usernames:
        person_id = wechat_map.get(username)
        if not person_id:
            # Try to find by nickname in contacts table
            wechat_cursor.execute("SELECT nickname FROM contacts WHERE username = ?", (username,))
            nick_row = wechat_cursor.fetchone()
            nickname = nick_row[0] if nick_row else username
            
            main_cursor.execute("SELECT id FROM persons WHERE name = ? OR display_name = ? OR nick_name = ?", (nickname, nickname, nickname))
            p_row = main_cursor.fetchone()
            if p_row:
                person_id = p_row[0]
                # Add the wechat contact mapping for future
                main_cursor.execute("INSERT INTO contacts (person_id, type, value) VALUES (?, 'wechat', ?)", (person_id, username))
            else:
                logging.warning(f"No person found for WeChat user: {username} ({nickname}). Skipping.")
                continue

        # Fetch and format messages
        wechat_cursor.execute("SELECT create_time, content, source FROM messages WHERE username = ? ORDER BY create_time", (username,))
        msgs = wechat_cursor.fetchall()
        
        history_lines = []
        for m_time, m_content, m_source in msgs:
            dt = datetime.fromtimestamp(m_time).strftime('%Y-%m-%d %H:%M:%S')
            # Assuming 'source' might contain direction info or we just use it as is
            # In our current parse_wechat.py, source is like 'ios_backup_...' or 'sqlite_...'
            # For simplicity, we format it as requested
            line = f"[{dt}] [Contact: {username}] {m_content}"
            history_lines.append(line)

        full_history = "\n".join(history_lines)

        main_cursor.execute(
            "INSERT OR REPLACE INTO wechat_messages (person_id, history) VALUES (?, ?)",
            (person_id, full_history),
        )
        logging.info(f"Merged {len(history_lines)} messages for person_id: {person_id}")

    main_conn.commit()
    main_conn.close()
    wechat_conn.close()
    logging.info("Merge completed.")

if __name__ == "__main__":
    merge()

import sqlite3
import os

DB_PATH = "data/db/database.sqlite"

def setup_db():
    if not os.path.exists(os.path.dirname(DB_PATH)):
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 1. wechat_raw_messages
    # We add msg_hash for global deduplication
    cursor.execute("DROP TABLE IF EXISTS wechat_raw_messages")
    cursor.execute("""
    CREATE TABLE wechat_raw_messages (
        username TEXT,
        create_time INTEGER,
        content TEXT,
        local_id INTEGER,
        source TEXT,
        message_type TEXT,
        media_path TEXT,
        media_id TEXT,
        msg_hash TEXT,
        PRIMARY KEY (username, local_id, source)
    )
    """)
    cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_wechat_msg_hash ON wechat_raw_messages (msg_hash)")

    # 2. other_raw_chats
    cursor.execute("DROP TABLE IF EXISTS other_raw_chats")
    cursor.execute("""
    CREATE TABLE other_raw_chats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_file TEXT,
        username TEXT,
        create_time INTEGER,
        content TEXT,
        platform TEXT,
        subfolder TEXT,
        msg_hash TEXT
    )
    """)
    cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_other_raw_chats_hash ON other_raw_chats (msg_hash)")

    # 3. wechat_raw_contacts (No changes needed for global dedup usually, username is unique)
    cursor.execute("DROP TABLE IF EXISTS wechat_raw_contacts")
    cursor.execute("""
    CREATE TABLE wechat_raw_contacts (
        username TEXT PRIMARY KEY,
        nickname TEXT,
        type INTEGER
    )
    """)

    # 4. wechat_moments
    cursor.execute("DROP TABLE IF EXISTS wechat_moments")
    cursor.execute("""
    CREATE TABLE wechat_moments (
        id TEXT PRIMARY KEY,
        username TEXT,
        nickname TEXT,
        create_time INTEGER,
        content TEXT,
        msg_hash TEXT
    )
    """)
    cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_wechat_moments_hash ON wechat_moments (msg_hash)")

    # 5. wechat_raw_media
    cursor.execute("DROP TABLE IF EXISTS wechat_raw_media")
    cursor.execute("""
    CREATE TABLE wechat_raw_media (
        id TEXT PRIMARY KEY,
        username TEXT,
        type TEXT,
        relative_path TEXT,
        original_path TEXT,
        file_size INTEGER,
        source TEXT
    )
    """)

    conn.commit()
    conn.close()
    print("Database raw tables reset with msg_hash support.")

if __name__ == "__main__":
    setup_db()

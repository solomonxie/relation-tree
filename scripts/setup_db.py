import sqlite3
import os

DB_PATH = "data/db/database.sqlite"

def cleanup_duplicates():
    if not os.path.exists(DB_PATH):
        return
        
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 1. Deduplicate other_raw_chats
    cursor.execute("DROP TABLE IF EXISTS other_raw_chats_temp")
    cursor.execute("""
    CREATE TABLE other_raw_chats_temp AS
    SELECT MIN(id) as id, source_file, username, create_time, content, platform, subfolder
    FROM other_raw_chats
    GROUP BY source_file, username, create_time, content
    """)
    cursor.execute("DROP TABLE other_raw_chats")
    cursor.execute("""
    CREATE TABLE other_raw_chats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_file TEXT,
        username TEXT,
        create_time INTEGER,
        content TEXT,
        platform TEXT,
        subfolder TEXT
    )
    """)
    cursor.execute("""
    INSERT INTO other_raw_chats (id, source_file, username, create_time, content, platform, subfolder)
    SELECT id, source_file, username, create_time, content, platform, subfolder
    FROM other_raw_chats_temp
    """)
    cursor.execute("DROP TABLE other_raw_chats_temp")
    
    # Create the unique index now that it's clean
    cursor.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS idx_other_raw_chats_unique 
    ON other_raw_chats (source_file, username, create_time, content)
    """)
    
    conn.commit()
    conn.close()
    print("Database deduplicated and unique indexes created.")

if __name__ == "__main__":
    cleanup_duplicates()

import sqlite3
import os
import logging
from setup_db import setup_db, DB_PATH

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

RAW_DB_DIR = "data/db/raw"

def merge_dbs():
    if not os.path.exists(RAW_DB_DIR):
        logging.info(f"Raw DB directory not found: {RAW_DB_DIR}")
        return

    # 1. Setup the main database
    logging.info(f"Setting up main database: {DB_PATH}")
    setup_db(DB_PATH)
    
    main_conn = sqlite3.connect(DB_PATH)
    main_cursor = main_conn.cursor()

    # 2. Get all raw databases
    raw_dbs = [f for f in os.listdir(RAW_DB_DIR) if f.endswith(".sqlite")]
    logging.info(f"Found {len(raw_dbs)} raw databases to merge.")

    tables = [
        "wechat_raw_messages",
        "other_raw_chats",
        "wechat_raw_contacts",
        "wechat_moments",
        "wechat_raw_media"
    ]

    for db_file in raw_dbs:
        db_path = os.path.join(RAW_DB_DIR, db_file)
        logging.info(f"Merging {db_file}...")
        
        try:
            # Attach the raw DB
            main_cursor.execute(f"ATTACH DATABASE '{db_path}' AS raw_db")
            
            for table in tables:
                # Check if table exists in raw_db
                main_cursor.execute(
                    f"SELECT name FROM raw_db.sqlite_master WHERE type='table' AND name='{table}'"
                )
                if not main_cursor.fetchone():
                    continue

                logging.info(f"  - Merging table: {table}")
                
                # Special handling for tables with AUTOINCREMENT or specific PKs
                if table == "other_raw_chats":
                    # Skip the 'id' column to let the main DB autoincrement it
                    main_cursor.execute(f"""
                        INSERT OR IGNORE INTO {table} 
                        (source_file, username, create_time, content, platform, subfolder, msg_hash)
                        SELECT source_file, username, create_time, content, platform, subfolder, msg_hash
                        FROM raw_db.{table}
                    """)
                elif table == "wechat_raw_contacts":
                    # Update nickname if it's NULL in main but present in raw
                    main_cursor.execute(f"""
                        INSERT OR IGNORE INTO {table} SELECT * FROM raw_db.{table}
                    """)
                    main_cursor.execute(f"""
                        UPDATE {table} SET 
                        nickname = (SELECT nickname FROM raw_db.{table} WHERE raw_db.{table}.username = {table}.username),
                        type = (SELECT type FROM raw_db.{table} WHERE raw_db.{table}.username = {table}.username)
                        WHERE username IN (SELECT username FROM raw_db.{table} WHERE nickname IS NOT NULL OR type IS NOT NULL)
                    """)
                else:
                    main_cursor.execute(f"INSERT OR IGNORE INTO {table} SELECT * FROM raw_db.{table}")
            
            main_conn.commit()
            main_cursor.execute("DETACH DATABASE raw_db")
            
        except Exception as e:
            logging.error(f"Error merging {db_file}: {e}")
            main_conn.rollback()

    main_conn.close()
    logging.info("Merge completed.")

if __name__ == "__main__":
    merge_dbs()

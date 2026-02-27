import os
import sqlite3
import logging
import hashlib
from datetime import datetime
import zipfile

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

CHATS_DB = "data/db/chats.sqlite"
MAIN_DB = "data/db/database.sqlite"
OTHERS_DIR = "blobs/others"
LOG_PATH = "blobs/processed_log.md"

def append_to_processed_log(stats, merged_count):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    log_info = ", ".join([f"{k}: {v} files" for k, v in stats.items()])
    
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(f"\n### ### Miscellaneous Chat Histories (Updated {timestamp})\n")
        f.write("- **Description**: Parses or logs metadata for arbitrary chat backups stored in various document formats.\n")
        f.write(f"- **Source Folders**: blobs/others/ ({log_info})\n")
        f.write("- **Destination**: data/db/chats.sqlite (raw_chats) -> data/db/database.sqlite (other_messages)\n")
        f.write(f"- **Status**: Merged into {merged_count} person histories.\n")
        f.write("- **Processor**: `scripts/parse_others_chats.py` -> `main()`\n")
        f.write("- **Example File**: `blobs/others/PDF CHATS/someone(25063922).pdf`\n")
        f.write("- **Example Message**: `[2026-02-04 11:06:04] [pdf_metadata] [Metadata Only] Chat log: someone(25063922).pdf`\n")

def setup_chats_db(cursor):
    cursor.execute("DROP TABLE IF EXISTS raw_chats")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS raw_chats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_file TEXT,
        username TEXT,
        create_time INTEGER,
        content TEXT,
        platform TEXT,
        subfolder TEXT
    )
    """)

def parse_txt_chat(file_path, cursor, subfolder):
    """Parses text chat logs."""
    platform = "generic_txt"
    try:
        # Try finding username from filename: Name(ID).txt or Name.txt
        filename = os.path.basename(file_path)
        username = filename.split('(')[0].strip() if '(' in filename else filename.replace('.txt', '')
        
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            for line in f:
                # Basic pattern: 2024-01-01 12:00:00 User Message
                if len(line) > 19 and line[4] == '-' and line[7] == '-' and line[13] == ':':
                    try:
                        ts_str = line[:19]
                        ts = int(datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S').timestamp())
                        content = line[19:].strip()
                        cursor.execute("INSERT INTO raw_chats (source_file, username, create_time, content, platform, subfolder) VALUES (?, ?, ?, ?, ?, ?)",
                                       (file_path, username, ts, content, platform, subfolder))
                    except:
                        continue
    except Exception as e:
        logging.error(f"Error parsing {file_path}: {e}")

def parse_metadata_only(file_path, cursor, subfolder):
    """For PDFs, Images, MHTs where content extraction is complex, log metadata."""
    filename = os.path.basename(file_path)
    ext = os.path.splitext(filename)[1].lower()
    platform = f"{ext[1:]}_metadata"
    username = filename.split('(')[0].strip() if '(' in filename else filename.replace(ext, '')
    mtime = int(os.path.getmtime(file_path))
    
    cursor.execute("INSERT INTO raw_chats (source_file, username, create_time, content, platform, subfolder) VALUES (?, ?, ?, ?, ?, ?)",
                   (file_path, username, mtime, f"[Metadata Only] Chat log: {filename}", platform, subfolder))

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
    
    chats_cursor.execute("SELECT DISTINCT username, platform, subfolder FROM raw_chats")
    sources = chats_cursor.fetchall()
    
    total_merged = 0
    for username, platform, subfolder in sources:
        # Try to map to person
        main_cursor.execute("SELECT id FROM persons WHERE name = ? OR nick_name = ?", (username, username))
        row = main_cursor.fetchone()
        if not row:
            main_cursor.execute("INSERT INTO persons (name) VALUES (?)", (username,))
            person_id = main_cursor.lastrowid
        else:
            person_id = row[0]

        chats_cursor.execute("SELECT create_time, content FROM raw_chats WHERE username = ? AND platform = ? AND subfolder = ? ORDER BY create_time", (username, platform, subfolder))
        msgs = chats_cursor.fetchall()
        
        history_lines = []
        for ts, content in msgs:
            dt = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
            history_lines.append(f"[{dt}] [{platform}] {content}")
        
        new_history = f"\n--- Source: {subfolder} ({platform}) ---\n" + "\n".join(history_lines)
        
        main_cursor.execute("SELECT history FROM other_messages WHERE person_id = ?", (person_id,))
        old_row = main_cursor.fetchone()
        if old_row:
            combined = old_row[0] + "\n" + new_history
        else:
            combined = new_history
            
        main_cursor.execute("INSERT OR REPLACE INTO other_messages (person_id, history) VALUES (?, ?)", (person_id, combined))
        total_merged += 1

    main_conn.commit()
    main_conn.close()
    chats_conn.close()
    return total_merged

def main():
    if not os.path.exists(OTHERS_DIR):
        return
    
    os.makedirs(os.path.dirname(CHATS_DB), exist_ok=True)
    conn = sqlite3.connect(CHATS_DB)
    cursor = conn.cursor()
    setup_chats_db(cursor)

    stats = {}
    
    for item in os.listdir(OTHERS_DIR):
        item_path = os.path.join(OTHERS_DIR, item)
        if os.path.isdir(item_path):
            stats[item] = 0
            for root, dirs, files in os.walk(item_path):
                for f in files:
                    if f.startswith('.'): continue
                    fpath = os.path.join(root, f)
                    ext = os.path.splitext(f)[1].lower()
                    if ext == '.txt':
                        parse_txt_chat(fpath, cursor, item)
                    else:
                        parse_metadata_only(fpath, cursor, item)
                    stats[item] += 1
        else:
            if item.startswith('.'): continue
            ext = os.path.splitext(item)[1].lower()
            if ext == '.txt':
                parse_txt_chat(item_path, cursor, "root")
                stats["root_txt"] = stats.get("root_txt", 0) + 1
            elif ext in ['.mht', '.mhtl', '.zip', '.bak']:
                parse_metadata_only(item_path, cursor, "root")
                stats["root_metadata"] = stats.get("root_metadata", 0) + 1
    
    conn.commit()
    conn.close()
    
    merged_count = merge_to_main()
    append_to_processed_log(stats, merged_count)
    logging.info(f"Others chats parsed and merged. Details logged to {LOG_PATH}")

if __name__ == "__main__":
    main()

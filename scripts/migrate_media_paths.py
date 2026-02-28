import os
import sqlite3
import shutil
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

DB_PATH = "data/db/database.sqlite"
MEDIA_ROOT = "data/media/wechat_media"

def migrate_media_paths():
    if not os.path.exists(DB_PATH):
        return
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 1. Fetch all media records
    cursor.execute("SELECT id, relative_path FROM wechat_raw_media")
    records = cursor.fetchall()
    
    total_moved = 0
    for mid, rel_path in records:
        # Check if path follows the nested structure
        if rel_path.startswith("ios_backup_"):
            parts = rel_path.split("/")
            if len(parts) > 1:
                # Remove the source folder prefix
                new_rel_path = "/".join(parts[1:])
                
                src_abs = os.path.join(MEDIA_ROOT, rel_path)
                dest_abs = os.path.join(MEDIA_ROOT, new_rel_path)
                
                if os.path.exists(src_abs):
                    os.makedirs(os.path.dirname(dest_abs), exist_ok=True)
                    try:
                        # Move file
                        if not os.path.exists(dest_abs):
                            shutil.move(src_abs, dest_abs)
                        else:
                            # If dest exists (e.g. from multiple backups), just delete src
                            os.remove(src_abs)
                        
                        # Update database
                        cursor.execute(
                            "UPDATE wechat_raw_media SET relative_path = ? WHERE id = ?",
                            (new_rel_path, mid)
                        )
                        # Also update messages mapping
                        cursor.execute(
                            "UPDATE wechat_raw_messages SET media_path = ? WHERE media_id = ?",
                            (new_rel_path, mid)
                        )
                        total_moved += 1
                    except Exception as e:
                        logging.error(f"Error moving {rel_path}: {e}")
                else:
                    logging.warning(f"File not found: {src_abs}")

    conn.commit()
    conn.close()
    logging.info(f"Finished! Migrated {total_moved} media paths.")

    # Cleanup empty directories
    for root, dirs, files in os.walk(MEDIA_ROOT, topdown=False):
        for name in dirs:
            dir_path = os.path.join(root, name)
            if not os.listdir(dir_path):
                os.rmdir(dir_path)

if __name__ == "__main__":
    migrate_media_paths()

import sqlite3
import hashlib
import os

DB_PATH = "data/db/database.sqlite"

def generate_hash(name):
    return hashlib.md5(name.encode()).hexdigest()[:16]

def fix_hashes():
    if not os.path.exists(DB_PATH):
        return
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("SELECT id, name, folder_hash FROM persons")
    rows = cursor.fetchall()
    
    updates = 0
    for pid, name, folder_hash in rows:
        if not folder_hash:
            new_hash = generate_hash(f"{name}_{pid}")
            cursor.execute("UPDATE persons SET folder_hash = ? WHERE id = ?", (new_hash, pid))
            updates += 1
            
    conn.commit()
    conn.close()
    print(f"Updated {updates} person records with folder hashes.")

if __name__ == "__main__":
    fix_hashes()

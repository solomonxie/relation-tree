import os
import re
import sqlite3
import hashlib
from datetime import datetime

# Paths
WECHAT_TXT_DIR = "blobs/Wechat_txt"
GROUP4_DB = "data/db/raw/group4_wechat_ios.sqlite"

def get_group4_messages():
    if not os.path.exists(GROUP4_DB):
        return set()
    
    conn = sqlite3.connect(GROUP4_DB)
    cursor = conn.cursor()
    msg_hashes = set()
    try:
        cursor.execute("SELECT username, create_time, content FROM group4_raw_messages LIMIT 5")
        print("Sample G4 messages:")
        for row in cursor.fetchall():
            username, ts, content = row
            ts_min = (ts // 60) * 60
            print(f"G4: {username} | {ts} ({datetime.fromtimestamp(ts)}) | {content[:30] if content else ''}")
            h = hashlib.md5(f"{username}|{ts_min}|{content.strip() if content else ''}".encode('utf-8', errors='ignore')).hexdigest()
            msg_hashes.add(h)
            
        cursor.execute("SELECT username, create_time, content FROM group4_raw_messages")
        for row in cursor.fetchall():
            username, ts, content = row
            ts_min = (ts // 60) * 60
            h = hashlib.md5(f"{username}|{ts_min}|{content.strip() if content else ''}".encode('utf-8', errors='ignore')).hexdigest()
            msg_hashes.add(h)
    except Exception as e:
        print(f"Error: {e}")
    conn.close()
    return msg_hashes

def check_dupes():
    g4_hashes = get_group4_messages()
    
    filename = "cuimingzhe的消息记录_20180619152328.txt"
    file_path = os.path.join(WECHAT_TXT_DIR, filename)
    username = "cuimingzhe"
    
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        content = f.read()
    
    lines = content.splitlines()
    print("\nSample TXT messages from cuimingzhe:")
    count = 0
    for line in lines:
        match = re.match(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2})\s+(.*?)\s+(发送|接收|未知类型)\s+(.*?)\s+(.*)", line)
        if match:
            dt_str, contact, direction, mtype, msg_content = match.groups()
            ts = int(datetime.strptime(dt_str, "%Y-%m-%d %H:%M").timestamp())
            print(f"TXT: {username} | {ts} ({dt_str}) | {msg_content[:30]}")
            h = hashlib.md5(f"{username}|{ts}|{msg_content.strip()}".encode('utf-8', errors='ignore')).hexdigest()
            if h in g4_hashes:
                print(" -> MATCH FOUND")
            count += 1
            if count > 5: break

if __name__ == "__main__":
    check_dupes()

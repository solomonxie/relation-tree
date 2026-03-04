import os
import re
import sqlite3
import hashlib
from datetime import datetime, timedelta, timezone

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
        cursor.execute("SELECT username, create_time, content FROM group4_raw_messages")
        for row in cursor.fetchall():
            username = row[0]
            # Normalize to minute precision (G4 stores UTC)
            ts_min = (row[1] // 60) * 60
            content = row[2].strip() if row[2] else ""
            h = hashlib.md5(f"{username}|{ts_min}|{content}".encode('utf-8', errors='ignore')).hexdigest()
            msg_hashes.add(h)
    except Exception as e:
        print(f"Error: {e}")
    conn.close()
    return msg_hashes

def check_dupes():
    print("Loading Group 4 messages...")
    g4_hashes = get_group4_messages()
    print(f"Loaded {len(g4_hashes)} unique message hashes from Group 4.")

    total_txt_msgs = 0
    dupe_count = 0
    new_contacts = set()
    
    txt_files = [f for f in os.listdir(WECHAT_TXT_DIR) if f.endswith(".txt")]
    
    # Timezone Beijing UTC+8
    beijing_tz = timezone(timedelta(hours=8))

    for filename in txt_files:
        username = filename.split("的消息记录")[0]
        file_path = os.path.join(WECHAT_TXT_DIR, filename)
        
        content = None
        for enc in ["utf-8", "gbk", "utf-16"]:
            try:
                with open(file_path, "r", encoding=enc, errors="strict") as f:
                    content = f.read()
                break
            except: continue
        
        if content is None: continue
            
        lines = content.splitlines()
        file_has_new = False
        for line in lines:
            match = re.match(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2})\s+(.*?)\s+(发送|接收|未知类型)\s+(.*?)\s+(.*)", line)
            if match:
                dt_str, contact, direction, mtype, msg_content = match.groups()
                try:
                    # Interpret as Beijing time, convert to UTC timestamp
                    dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M").replace(tzinfo=beijing_tz)
                    ts = int(dt.timestamp())
                except: continue
                
                total_txt_msgs += 1
                msg_content = msg_content.strip()
                h = hashlib.md5(f"{username}|{ts}|{msg_content}".encode('utf-8', errors='ignore')).hexdigest()
                if h in g4_hashes:
                    dupe_count += 1
                else:
                    file_has_new = True
        
        if file_has_new:
            new_contacts.add(username)

    print(f"\nResults:")
    print(f"Total messages in Wechat_txt: {total_txt_msgs}")
    if total_txt_msgs > 0:
        print(f"Duplicate messages found in Group 4: {dupe_count} ({dupe_count/total_txt_msgs*100:.1f}%)")
        print(f"New messages count: {total_txt_msgs - dupe_count}")
    print(f"Total unique contacts in Wechat_txt files: {len(txt_files)}")
    print(f"Contacts with at least one new message: {len(new_contacts)}")

if __name__ == "__main__":
    check_dupes()

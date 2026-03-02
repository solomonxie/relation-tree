"""
Group 2: MHTML (QQ Chat Archive) Parser
Source: blobs/qq_mht/*.mht
Features:
1. Parses MHTML/HTML QQ chat logs.
2. Improved sender and receiver recognition for 1-on-1 AND Group chats.
3. Detects group context to correctly assign sender/receiver.
4. Uses global name-to-ID mapping and handles nicknames.
5. Deduplication based on (sender_id or sender_name), create_time, and content.
"""

import json
import logging
import os
import re
import sqlite3
import sys
from datetime import datetime
from glob import glob

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Constants
OUTPUT_DB = "data/db/raw/group2_mhtml.sqlite"
SCHEMA_FILE = "data/schema/raw/group2_mhtml.sql"
PARTNERS_MAP_FILE = "data/partners_map.json"
OWNER_NAME = '几何体'
OWNER_ID = 610784125

# Global mappings
NAME_TO_ID = {}
ID_TO_NAME = {}
GROUPS = set()


def clean_html(html):
    """Remove HTML tags and clean up whitespace."""
    if not html:
        return ""
    html = re.sub(r"<IMG[^>]+>", "[图片]", html, flags=re.IGNORECASE)
    html = re.sub(r"<style.*?>.*?</style>", "", html, flags=re.DOTALL)
    html = html.replace("&nbsp;", " ")
    text = re.sub(r"<[^'\">]*?(?:'[^']*?'|\"[^\"]*?\"|[^'\">])*?>", "", html, flags=re.DOTALL)
    text = text.replace("&quot;", "\"").replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")
    text = re.sub(r"[^>\n]*'(?:MS Sans Serif|Tahoma|宋体|微软雅黑|Arial|Times New Roman|楷体_GB2312|新宋体|SimSun|Comic Sans MS|黑体|fontname)'[^>\n]*>", "", text)
    return text.strip()


def init_db():
    """Initialize DB using the local schema file."""
    if os.path.exists(OUTPUT_DB):
        os.remove(OUTPUT_DB)
    os.makedirs(os.path.dirname(OUTPUT_DB), exist_ok=True)
    with open(SCHEMA_FILE, 'r') as f:
        schema_sql = f.read()
    conn = sqlite3.connect(OUTPUT_DB)
    conn.executescript(schema_sql)
    conn.commit()
    return conn


def extract_qq_id(text):
    """Extract integer QQ ID from text, or return None."""
    if not text: return None
    match = re.search(r'(\d{5,12})', str(text))
    return int(match.group(1)) if match else None


def load_global_mappings():
    """Build global Name <-> ID mappings and Group list."""
    global NAME_TO_ID, ID_TO_NAME, GROUPS
    
    if os.path.exists(PARTNERS_MAP_FILE):
        try:
            with open(PARTNERS_MAP_FILE, 'r') as f:
                data = json.load(f)
                for p in data.get('partners', {}).values():
                    pid = extract_qq_id(p.get('id'))
                    if pid:
                        pname = p.get('name')
                        if pname:
                            NAME_TO_ID[pname] = pid
                            ID_TO_NAME[pid] = pname
                        for nick in p.get('nicknames', []):
                            if nick and nick not in NAME_TO_ID:
                                NAME_TO_ID[nick] = pid
                        if p.get('type') == 'group' or 'group' in str(p.get('id')):
                            GROUPS.add(pid)
                            if pname: GROUPS.add(pname)
        except Exception as e:
            logging.error(f"Failed to load partners map: {e}")

    for filepath in glob("blobs/qq_mht/*.mht"):
        filename = os.path.basename(filepath).replace('.mht', '')
        match = re.search(r'^(.*?)\((.*?)\)', filename)
        if match:
            name, raw_id = match.group(1).strip(), match.group(2).strip()
            qq_id = extract_qq_id(raw_id)
            if qq_id:
                NAME_TO_ID[name] = qq_id
                ID_TO_NAME[qq_id] = name
                if 'group' in filename.lower():
                    GROUPS.add(qq_id)
                    GROUPS.add(name)

    logging.info(f"Loaded {len(NAME_TO_ID)} name-to-ID mappings. Identified {len(GROUPS)} groups.")


def parse_file(filepath, cursor, seen_msgs):
    logging.info(f"Processing {filepath}")
    filename = os.path.basename(filepath)
    
    f_name, f_id = (filename.replace('.mht', ''), None)
    match = re.search(r'^(.*?)\((.*?)\)', f_name)
    if match:
        f_name = match.group(1).strip()
        f_id = extract_qq_id(match.group(2).strip())
    
    current_context_id = f_id or NAME_TO_ID.get(f_name)
    current_context_name = ID_TO_NAME.get(current_context_id) if current_context_id else f_name
    is_group_context = current_context_id in GROUPS or current_context_name in GROUPS or 'group' in filename.lower()

    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        content = f.read()

    html_match = re.search(r'(<html.*</html>)', content, re.DOTALL | re.IGNORECASE)
    if not html_match: return 0
    
    html_content = html_match.group(1)
    rows = re.findall(r'<tr.*?>(.*?)</tr>', html_content, re.DOTALL)
    current_date = '1900-01-01'
    total_msgs = 0
    
    for row in rows:
        # Detect Category/Group context change
        cat_match = re.search(r'消息分组:\s*(.*?)($|<)', row.replace('&nbsp;', ' '))
        if cat_match:
            is_group_context = '群' in cat_match.group(1)
            continue

        obj_match = re.search(r'消息对象:\s*(.*?)($|<)', row.replace('&nbsp;', ' '))
        if obj_match:
            raw_obj = clean_html(obj_match.group(1)).strip()
            current_context_id = extract_qq_id(raw_obj) or NAME_TO_ID.get(raw_obj)
            current_context_name = ID_TO_NAME.get(current_context_id) or raw_obj
            if not is_group_context:
                is_group_context = current_context_id in GROUPS or current_context_name in GROUPS
            continue

        date_match = re.search(r'日期:\s*(\d{4}-\d{2}-\d{2})', row)
        if date_match:
            current_date = date_match.group(1)
            continue
        
        msg_match = re.search(
            r'<div[^>]*float:left[^>]*>(.*?)</div>\s*(\d{1,2}:\d{2}:\d{2})</div>.*?<div[^>]*padding-left:20px[^>]*>(.*?)</div>',
            row, re.DOTALL
        )
        
        if msg_match:
            nickname = clean_html(msg_match.group(1))
            time_str = msg_match.group(2).strip()
            clean_content = clean_html(msg_match.group(3))
            if not clean_content: continue
            
            is_owner = nickname == OWNER_NAME or 'color:#42B475' in row or NAME_TO_ID.get(nickname) == OWNER_ID
            
            if is_owner:
                s_name, s_id = OWNER_NAME, OWNER_ID
                r_name, r_id = current_context_name, current_context_id
            elif is_group_context:
                # In group, the nickname is the actual sender, group is the receiver
                s_name = nickname
                s_id = extract_qq_id(nickname) or NAME_TO_ID.get(nickname)
                r_name, r_id = current_context_name, current_context_id
            else:
                # 1-on-1, contact speaking, owner receiving
                s_name, s_id = current_context_name, current_context_id
                if not s_id: s_id = extract_qq_id(nickname)
                r_name, r_id = OWNER_NAME, OWNER_ID
            
            try:
                ts = int(datetime.strptime(f"{current_date} {time_str}", "%Y-%m-%d %H:%M:%S").timestamp())
            except: continue
            
            msg_key = (s_id or s_name, ts, clean_content)
            if msg_key in seen_msgs: continue
            seen_msgs.add(msg_key)
                
            cursor.execute(
                "INSERT INTO group2_raw_mhtml "
                "(source_file, sender_name, sender_id, receiver_name, receiver_id, nicknames, create_time, content) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (filepath, s_name, s_id, r_name, r_id, nickname, ts, clean_content)
            )
            total_msgs += 1
            
    return total_msgs


def main():
    load_global_mappings()
    files = [sys.argv[1]] if len(sys.argv) > 1 else glob("blobs/qq_mht/*.mht")
    if not files: return
    conn = init_db()
    cursor = conn.cursor()
    seen_msgs = set()
    total_extracted = 0
    for filepath in sorted(files):
        try:
            count = parse_file(filepath, cursor, seen_msgs)
            total_extracted += count
            logging.info(f"Extracted {count} unique messages from {os.path.basename(filepath)}")
            if total_extracted % 10000 == 0: conn.commit()
        except Exception as e:
            logging.error(f"Failed to process {filepath}: {e}")
    conn.commit()
    conn.close()
    logging.info(f"Processing complete. Total unique messages: {total_extracted}")

if __name__ == '__main__':
    main()

"""
Group 1: QQ TXT Parser (Contact Table Refactor)
Source: blobs/qq_txt/*.txt
Features:
1. Populates 'contacts' table (id, type, name, nicknames, grouping).
2. Attributes messages using the contact mapping.
3. Correctly handles Group vs Person chats.
"""

import hashlib
import logging
import os
import re
import sqlite3
import json
import requests
from datetime import datetime
from glob import glob
from dotenv import load_dotenv

load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Configuration for Local LLM
LLM_API_BASE = os.getenv("LLM_API_BASE", "http://localhost:11434/v1")
LLM_API_KEY = os.getenv("LLM_API_KEY", "ollama")
MODEL_NAME = os.getenv("LLM_MODEL", "llama3:latest")

OUTPUT_DB = "data/db/raw/group1_html.sqlite"

METADATA_PROMPT = """
You are an expert at analyzing QQ chat log headers.
Identify the main chat partner or group from the following filename and content snippet.
The user's own name is "几何体".

Filename: {filename}
Content Snippet:
---
{snippet}
---

Return a JSON object with:
- "is_group": boolean,
- "partner_id": string (the QQ ID from filename or header, for groups use "group_<slug>"),
- "partner_name": string (the real name of the person or the name of the group),
- "nicknames": list of strings (include nicknames, and for groups, include names of members mentioned),
- "grouping": string (the value after "消息分组:"),
- "chat_type": "person" or "group"

Rules:
1. If "消息分组" is "我的QQ群", it's a group.
2. If it's a group, "partner_id" should be "group_" followed by an English slug of the group name.
3. "partner_name" MUST be populated. For a group, it's the "消息对象".
4. "nicknames" should include all names seen in the header or filename that refer to the partner or group members.
5. "partner_id" for a person should be the integer QQ ID if found in filename (e.g., Name_ID.txt) or header.

Only return the JSON object.
"""

def query_llm(prompt):
    payload = {
        "model": MODEL_NAME,
        "messages": [
            {"role": "system", "content": "You are a helpful assistant that extracts structured data from text into JSON format."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.1,
        "response_format": {"type": "json_object"},
    }
    try:
        response = requests.post(
            f"{LLM_API_BASE}/chat/completions",
            headers={"Authorization": f"Bearer {LLM_API_KEY}"},
            json=payload,
            timeout=30
        )
        response.raise_for_status()
        content = response.json()["choices"][0]["message"]["content"]
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0].strip()
        elif "```" in content:
            content = content.split("```")[1].split("```")[0].strip()
        return json.loads(content)
    except Exception as e:
        logging.error(f"Error querying LLM: {e}")
        return {}

def extract_metadata_llm(filepath):
    filename = os.path.basename(filepath)
    snippet = ""
    try:
        with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
            lines = []
            for _ in range(100):
                line = next(f, None)
                if line is None: break
                lines.append(line)
            snippet = "".join(lines)
    except Exception as e:
        logging.error(f"Error reading snippet from {filename}: {e}")
    
    prompt = METADATA_PROMPT.format(filename=filename, snippet=snippet)
    metadata = query_llm(prompt)
    
    # Defaults/Sanity check
    if not metadata.get("partner_id"):
        match = re.search(r"(\d+)", filename)
        if match:
            metadata["partner_id"] = match.group(1)
            metadata["chat_type"] = "person"
        else:
            metadata["partner_id"] = "unknown"
            
    if not metadata.get("partner_name"):
        match = re.search(r"消息对象:(.*)", snippet)
        if match:
            metadata["partner_name"] = match.group(1).strip()
        else:
            metadata["partner_name"] = filename.split('_')[0]
            
    if not metadata.get("chat_type"):
        metadata["chat_type"] = "group" if "我的QQ群" in snippet else "person"
            
    return metadata

def compute_msg_hash(sender_id, create_time, content):
    base_str = f"{sender_id}|{create_time}|{content}"
    return hashlib.md5(base_str.encode('utf-8', errors='replace')).hexdigest()

def init_db():
    if os.path.exists(OUTPUT_DB):
        os.remove(OUTPUT_DB)
        logging.info(f"Deleted existing database: {OUTPUT_DB}")
    
    os.makedirs(os.path.dirname(OUTPUT_DB), exist_ok=True)
    conn = sqlite3.connect(OUTPUT_DB)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS contacts (
            id TEXT PRIMARY KEY,
            type TEXT,
            name TEXT,
            nicknames TEXT,
            grouping TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS group1_raw_html (
            id INTEGER PRIMARY KEY AUTOINCREMENT, 
            source_file TEXT, 
            sender_id TEXT, 
            receiver_id TEXT, 
            chat_type TEXT,
            create_time INTEGER, 
            content TEXT, 
            platform TEXT, 
            subfolder TEXT, 
            msg_hash TEXT
        )
    """)
    cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_group1_msg_hash ON group1_raw_html (msg_hash)")
    conn.commit()
    return conn

def parse_file(filepath, conn):
    logging.info(f"Processing: {filepath}")
    metadata = extract_metadata_llm(filepath)
    if not metadata:
        logging.error(f"Could not extract metadata for {filepath}")
        return

    cursor = conn.cursor()
    nicknames_json = json.dumps(metadata.get("nicknames", []))
    cursor.execute(
        "INSERT OR REPLACE INTO contacts (id, type, name, nicknames, grouping) VALUES (?, ?, ?, ?, ?)",
        (metadata["partner_id"], metadata["chat_type"], metadata["partner_name"], nicknames_json, metadata.get("grouping"))
    )
    conn.commit()

    partner_id = metadata["partner_id"]
    chat_type = metadata["chat_type"]
    
    with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
        lines = f.readlines()

    current_date = None
    i = 0
    messages_count = 0
    
    while i < len(lines):
        line = lines[i].strip()
        
        if line == "日期:":
            if i + 1 < len(lines):
                date_match = re.search(r"(\d{4}-\d{2}-\d{2})", lines[i+1])
                if date_match:
                    current_date = date_match.group(1)
                    i += 2
                    continue
        
        time_match = re.match(r"^(\d{2}:\d{2}:\d{2})$", line)
        if time_match and i > 0:
            sender_name = lines[i-1].strip()
            if sender_name == "日期:":
                i += 1
                continue
                
            time_str = time_match.group(1)
            
            content_parts = []
            j = i + 1
            while j < len(lines):
                next_line = lines[j].strip()
                if next_line == "日期:":
                    break
                if re.match(r"^\d{2}:\d{2}:\d{2}$", next_line):
                    if j > i + 1:
                        content_parts.pop() 
                    break
                content_parts.append(lines[j].strip())
                j += 1
            
            content = " ".join(content_parts).strip()
            
            if current_date and sender_name:
                try:
                    ts_str = f"{current_date} {time_str}"
                    ts = int(datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").timestamp())
                    
                    if sender_name == "几何体":
                        s_id = "Me"
                        r_id = partner_id
                    else:
                        s_id = partner_id if chat_type == "person" else sender_name
                        r_id = "Me" if chat_type == "person" else partner_id
                    
                    m_hash = compute_msg_hash(s_id, ts, content)
                    
                    cursor.execute(
                        "INSERT OR IGNORE INTO group1_raw_html (source_file, sender_id, receiver_id, chat_type, create_time, content, platform, subfolder, msg_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (filepath, s_id, r_id, chat_type, ts, content, "qq", "qq_txt", m_hash)
                    )
                    messages_count += 1
                except Exception:
                    pass
            
            i = j
            continue
        
        i += 1
    
    conn.commit()
    logging.info(f"Extracted {messages_count} messages from {os.path.basename(filepath)}")

def main():
    conn = init_db()
    files = glob("blobs/qq_txt/*.txt")
    logging.info(f"Found {len(files)} files to process.")
    
    for filepath in files:
        try:
            parse_file(filepath, conn)
        except Exception as e:
            logging.error(f"Failed to process {filepath}: {e}")
            
    conn.close()
    logging.info("Processing complete.")

if __name__ == '__main__':
    main()


import os
import re
import hashlib
import sqlite3
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

HTML_DIR = "blobs/HTML_CHATS"
TXT_DIR = "blobs/qq_txt"

MY_DEFAULT_NAME = "几何体"
MY_DEFAULT_ID = "610784125"

def clean_val(val):
    if not val: return "unknown"
    return re.sub(r'[^\w\u4e00-\u9fff]', '', str(val)).strip() or "unknown"

def strip_tags(html):
    # Remove styles and scripts
    text = re.sub(r'<style.*?>.*?</style>', '', html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<script.*?>.*?</script>', '', text, flags=re.DOTALL | re.IGNORECASE)
    
    # Remove specifically the common noise mentioned by user
    # like ','MS Sans Serif',sans-serif; FONT-SIZE: 10pt" color=#000000>
    # These often appear because of how some parsers handle font/span tags
    text = re.sub(r"['\"],['\"]MS Sans Serif['\"],sans-serif; FONT-SIZE: \d+pt['\"]", "", text, flags=re.IGNORECASE)
    text = re.sub(r"color=#[0-9a-fA-F]+", "", text, flags=re.IGNORECASE)
    text = re.sub(r"style=['\"].*?['\"]", "", text, flags=re.IGNORECASE)

    # Replace tags with newlines
    text = re.sub(r'<[^>]+>', '\n', text)
    
    # Entities
    text = text.replace('&nbsp;', ' ').replace('&lt;', '<').replace('&gt;', '>').replace('&amp;', '&')
    
    # Final cleanup of common residual fragments
    text = re.sub(r"FONT-SIZE: \d+pt", "", text, flags=re.IGNORECASE)
    
    # Collapse whitespace
    return re.sub(r'\n\s*\n', '\n', text).strip()

def extract_metadata(html_content, filename):
    snippet = html_content[:5000]
    chat_group = "unknown"
    chat_object_raw = "unknown"
    
    group_match = re.search(r"消息分组:(.*?)(?:\n|<)", snippet)
    if group_match: chat_group = group_match.group(1).strip()
    
    obj_match = re.search(r"消息对象:(.*?)(?:\n|<)", snippet)
    if obj_match: chat_object_raw = obj_match.group(1).strip()
    
    p_name, p_id = None, None
    m = re.search(r"^(.*?)\((\d{5,})\)$", chat_object_raw)
    if m:
        p_name, p_id = m.group(1), m.group(2)
    elif chat_object_raw.isdigit() and len(chat_object_raw) >= 5:
        p_id = chat_object_raw
    else:
        p_name = chat_object_raw
        
    if not p_id:
        m_fn = re.search(r"\((\d{5,})\)", filename)
        if m_fn: p_id = m_fn.group(1)
        
    return {
        "partner_name": clean_val(p_name),
        "partner_id": p_id or "unknown",
        "my_name": clean_val(MY_DEFAULT_NAME),
        "my_id": MY_DEFAULT_ID,
        "chat_type": chat_group
    }

def main():
    if not os.path.exists(HTML_DIR):
        logging.error(f"HTML directory {HTML_DIR} not found.")
        return
    os.makedirs(TXT_DIR, exist_ok=True)
    
    files = [f for f in os.listdir(HTML_DIR) if f.endswith(".htm")]
    logging.info(f"Analyzing {len(files)} files...")
    
    # Use a set to track used filenames and avoid collisions
    used_names = {}

    for filename in files:
        path = os.path.join(HTML_DIR, filename)
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                content = f.read()
            
            meta = extract_metadata(content, filename)
            
            base_name = f"sender_id_{meta['partner_id']}__sender_name_{meta['partner_name']}__receiver_name_{meta['my_name']}"
            
            # Ensure unique filename
            if base_name not in used_names:
                used_names[base_name] = 0
                new_filename = f"{base_name}.txt"
            else:
                used_names[base_name] += 1
                new_filename = f"{base_name}_{used_names[base_name]}.txt"

            if len(new_filename) > 240:
                new_filename = new_filename[:230] + ".txt"
                
            out_path = os.path.join(TXT_DIR, new_filename)
            
            plain_text = strip_tags(content)
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(plain_text)
        except Exception as e:
            logging.error(f"Error {filename}: {e}")

if __name__ == "__main__":
    main()

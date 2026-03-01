import os
import re
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

MHTML_DIR = "blobs/MHTML_CHATS"
TXT_DIR = "blobs/qq_txt"

MY_DEFAULT_NAME = "几何体"
MY_DEFAULT_ID = "610784125"

def clean_val(val):
    if not val: return "unknown"
    return re.sub(r'[^\w\u4e00-\u9fff-]', '', str(val)).strip() or "unknown"

def strip_tags(html):
    # Remove styles and scripts
    text = re.sub(r'<style.*?>.*?</style>', '', html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<script.*?>.*?</script>', '', text, flags=re.DOTALL | re.IGNORECASE)
    
    # Aggressive removal of residual style fragments
    # e.g. ','MS Sans Serif',sans-serif;" color='000000'>
    text = re.sub(r"['\"],?['\"]MS Sans Serif['\"],sans-serif;?['\"]?", "", text, flags=re.IGNORECASE)
    text = re.sub(r"color=['\"].*?['\"]", "", text, flags=re.IGNORECASE)
    text = re.sub(r"style=['\"].*?['\"]", "", text, flags=re.IGNORECASE)
    text = re.sub(r"FONT-SIZE: \d+pt", "", text, flags=re.IGNORECASE)
    text = re.sub(r"font-family:.*?;", "", text, flags=re.IGNORECASE)

    # Replace tags with newlines
    text = re.sub(r'<[^>]+>', '\n', text)
    
    # Entities
    text = text.replace('&nbsp;', ' ').replace('&lt;', '<').replace('&gt;', '>').replace('&amp;', '&')
    
    # Collapse whitespace
    return re.sub(r'\n\s*\n', '\n', text).strip()

def extract_metadata(html_content, filename):
    snippet = html_content[:5000]
    chat_group = "unknown"
    chat_object_raw = "unknown"
    
    group_match = re.search(r"消息分组:(.*?)(?:\n|<)", snippet)
    if group_match: chat_group = group_match.group(1).replace('&nbsp;', ' ').strip()
    
    obj_match = re.search(r"消息对象:(.*?)(?:\n|<)", snippet)
    if obj_match: chat_object_raw = obj_match.group(1).replace('&nbsp;', ' ').strip()
    
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
        
    is_group = "我的QQ群" in chat_group or "group" in filename.lower()
    
    return {
        "partner_name": clean_val(p_name),
        "partner_id": p_id or ("unknown" if not is_group else f"group_{clean_val(p_name)}"),
        "my_name": clean_val(MY_DEFAULT_NAME),
        "my_id": MY_DEFAULT_ID,
        "is_group": is_group
    }

def get_html_content(path):
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()
    except Exception:
        with open(path, "r", encoding="gbk", errors="replace") as f:
            content = f.read()

    start = content.find("<html")
    if start == -1: return None
    end = content.find("</html>", start)
    if end == -1: return content[start:]
    return content[start:end+7]

def main():
    if not os.path.exists(MHTML_DIR):
        logging.error(f"MHTML directory {MHTML_DIR} not found.")
        return
    os.makedirs(TXT_DIR, exist_ok=True)
    
    files = [f for f in os.listdir(MHTML_DIR) if f.endswith(".mht")]
    logging.info(f"Analyzing {len(files)} files...")
    
    for filename in files:
        path = os.path.join(MHTML_DIR, filename)
        try:
            html_content = get_html_content(path)
            if not html_content:
                continue
            
            meta = extract_metadata(html_content, filename)
            p_name = meta['partner_name']
            p_id = meta['partner_id']
            
            new_filename = f"{p_name}_{p_id}.txt"
            out_path = os.path.join(TXT_DIR, new_filename)
            
            # For testing and fix: overwrite IF it contains 'unknown' and we are trying to fix it
            # Actually, user said don't conflict. 
            # I'll only run for the one I want to fix if I want to be safe.
            # But let's just make it overwrite if the file has those residual fragments.
            
            should_write = True
            if os.path.exists(out_path):
                should_write = False
                # Special case: if it's 'unknown_372252573.txt', let's overwrite to fix it
                if new_filename == "unknown_372252573.txt":
                    should_write = True
            
            if should_write:
                plain_text = strip_tags(html_content)
                with open(out_path, "w", encoding="utf-8") as f:
                    f.write(plain_text)
                logging.info(f"Converted/Updated: {filename} -> {new_filename}")
        except Exception as e:
            logging.error(f"Error {filename}: {e}")

if __name__ == "__main__":
    main()

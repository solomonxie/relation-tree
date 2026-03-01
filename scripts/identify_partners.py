import os
import re
import json
from collections import Counter

TXT_DIR = "blobs/qq_txt"
MY_NAME = "几何体"
MY_ID = "610784125"

def clean_name(name):
    if not name: return ""
    name = re.sub(r"\((\d+)\)$", "", name).strip()
    name = re.sub(r"<.*?>$", "", name).strip()
    name = re.sub(r"<.*$", "", name).strip()
    # Keep some characters for candidate processing, but final name will be cleaner
    return name.strip()

def score_name(name):
    """Higher score means more likely a real name."""
    if not name: return -1
    if name.isdigit(): return 0
    
    # Strictly alphanumeric/CJK
    c = re.sub(r"[^\w\u4e00-\u9fff]", "", name)
    if not c: return -1
    
    # Check for "rare" or "stylistic" Chinese characters often in nicknames
    # but not standard names.
    stylistic_chars = r"[\u3400-\u4dbf\uf900-\ufaff\u2e80-\u2eff\u31c0-\u31ef\u3000-\u303f]"
    # Stylistic/weird characters like 尛, 釹, 秂
    is_stylistic = bool(re.search(r"[尛釹秂ゞ★ゞ迷ゞ迷迷迷ゞ迷ゞ迷迷]", name))
    
    score = 10
    if c.isalpha(): score = 30
    if re.search(r"[\u4e00-\u9fff]", c): score = 50
    if re.fullmatch(r"[\u4e00-\u9fff]{2,4}", c): score = 100
    
    if is_stylistic: score -= 60 # Significant penalty
    
    return score

def main():
    if not os.path.exists(TXT_DIR):
        print("TXT directory not found.")
        return

    files = [f for f in os.listdir(TXT_DIR) if f.endswith(".txt")]
    partners_info = {}

    for filename in files:
        path = os.path.join(TXT_DIR, filename)
        match = re.match(r"^(.*?)_(.*?)\.txt$", filename)
        if not match: continue
        fname_name = match.group(1)
        fname_id = match.group(2)
        
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            lines = [line.strip() for line in f.readlines()[:500]]
        
        chat_type = "person"
        candidates = set()
        if fname_name != "unknown": candidates.add(fname_name)
        
        for line in lines:
            if "消息分组:" in line:
                if "群" in line or "讨论组" in line:
                    chat_type = "group"
            if "消息对象:" in line:
                candidates.add(line.replace("消息对象:", "").strip())
            
            # Message headers
            time_match = re.search(r"(\d{1,2}:\d{2}:\d{2})", line)
            if time_match:
                # If timestamp is end of line, part before is name
                if re.fullmatch(r".*?\d{1,2}:\d{2}:\d{2}", line):
                    n = line.replace(time_match.group(1), "").strip()
                    if n and n != MY_NAME: candidates.add(n)
                # If timestamp is full line, line above is name
                elif re.fullmatch(r"\d{1,2}:\d{2}:\d{2}", line):
                    # We'd need index here, handle below
                    pass

        # Second pass for line-above-timestamp names
        for i, line in enumerate(lines):
            if re.fullmatch(r"\d{1,2}:\d{2}:\d{2}", line):
                if i > 0 and lines[i-1].strip():
                    n = lines[i-1].strip()
                    if n and n != MY_NAME: candidates.add(n)

        # Process and rank candidates
        cleaned_candidates = {} # cleaned -> original
        for c in candidates:
            # We want to filter out tech junk but keep some nicknames
            if "成功发送" in c or len(c) > 50: continue
            
            # Simple cleaning for ranking
            simple = re.sub(r"\((\d+)\)$", "", c).strip() # remove (ID)
            simple = re.sub(r"[^\w\u4e00-\u9fff]", "", simple) # remove noise
            
            if simple:
                if simple not in cleaned_candidates or score_name(simple) > score_name(cleaned_candidates[simple]):
                    cleaned_candidates[simple] = simple

        # Sort by score descending
        sorted_names = sorted(cleaned_candidates.keys(), key=lambda x: score_name(x), reverse=True)
        
        best_name = sorted_names[0] if sorted_names else (fname_name if fname_name != "unknown" else "unknown")
        
        partners_info[filename] = {
            "id": fname_id,
            "type": chat_type,
            "name": best_name,
            "nicknames": list(cleaned_candidates.keys())
        }

    output = {
        "me": {"id": MY_ID, "name": MY_NAME, "type": "person", "nicknames": [MY_NAME]},
        "partners": partners_info
    }

    with open("data/partners_map.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    
    print(f"Contact mapping built for {len(partners_info)} files with improved ranking.")

if __name__ == "__main__":
    main()


import os
import re

TXT_DIR = "blobs/qq_txt"

def main():
    files = os.listdir(TXT_DIR)
    for filename in files:
        if "__" not in filename: continue
        
        path = os.path.join(TXT_DIR, filename)
        parts = filename.replace(".txt", "").split("__")
        
        pid = "unknown"
        pname = "unknown"
        
        for p in parts:
            if p.startswith("sender_id_"): pid = p.replace("sender_id_", "")
            elif p.startswith("sender_name_"): pname = p.replace("sender_name_", "")
            elif p.isdigit() and pid == "unknown": pid = p
            elif not p.startswith("receiver_name_") and pname == "unknown": pname = p

        new_name = f"{pname}_{pid}.txt"
        new_path = os.path.join(TXT_DIR, new_name)
        
        if os.path.exists(new_path) and new_name != filename:
            base = f"{pname}_{pid}"
            counter = 1
            while os.path.exists(os.path.join(TXT_DIR, f"{base}_{counter}.txt")):
                counter += 1
            new_name = f"{base}_{counter}.txt"
            new_path = os.path.join(TXT_DIR, new_name)
            
        os.rename(path, new_path)
        print(f"Renamed: {filename} -> {new_name}")

if __name__ == "__main__":
    main()

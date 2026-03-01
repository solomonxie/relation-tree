import os
import re

TXT_DIR = "blobs/qq_txt"

def main():
    files = os.listdir(TXT_DIR)
    groups = {}
    for f in files:
        if not f.endswith(".txt"): continue
        match = re.match(r"^(.*?)(?:_\d+)?\.txt$", f)
        if match:
            base = match.group(1)
            if base not in groups:
                groups[base] = []
            groups[base].append(f)
    
    deleted_count = 0
    for base, variants in groups.items():
        if len(variants) > 1:
            variants_with_size = []
            for v in variants:
                path = os.path.join(TXT_DIR, v)
                variants_with_size.append((v, os.path.getsize(path)))
            
            variants_with_size.sort(key=lambda x: x[1], reverse=True)
            
            best_file, best_size = variants_with_size[0]
            to_delete = variants_with_size[1:]
            
            print(f"Group: {base}")
            print(f"  KEEPING: {best_file} ({best_size} bytes)")
            for d_file, d_size in to_delete:
                d_path = os.path.join(TXT_DIR, d_file)
                os.remove(d_path)
                print(f"  DELETED: {d_file} ({d_size} bytes)")
                deleted_count += 1
                
    print(f"\nTotal duplicates deleted: {deleted_count}")

if __name__ == "__main__":
    main()

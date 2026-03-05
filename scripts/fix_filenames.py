
import os
import re

TXT_DIR = "blobs/qq_txt"

# Mapping: { filename_fragment: (id, name) }
RENAME_MAP = {
    "1035416647": ("1035416647", "路君慧"),
    "1286164011": ("1286164011", "李鑫"),
    "2271102210": ("2271102210", "刘非非"),
    "3250936494": ("3250936494", "Skyhale"),
    "328440954": ("328440954", "堔藍朵朵"),
    "328664942": ("328664942", "刘正"),
    "332006871": ("332006871", "空白"),
    "343663582": ("343663582", "小任"),
    "372252573": ("372252573", "罗涛"),
    "378216274": ("378216274", "卢静"),
    "380212160": ("380212160", "王雨婷"),
    "415216261": ("415216261", "刘颖"),
    "422631246": ("422631246", "纪婷婷"),
    "435977310": ("435977310", "王吉羊"),
    "43902067": ("43902067", "窦静"),
    "470860308": ("470860308", "董珩"),
    "492519377": ("492519377", "宋斯佳"),
    "540730627": ("540730627", "颜昆鹏"),
    "541624710": ("541624710", "何威"),
    "595814170": ("595814170", "陈卉"),
    "635974425": ("635974425", "董珩"),
    "645267906": ("645267906", "Martin Wise"),
    "659641533": ("659641533", "李辰男"),
    "906610774": ("906610774", "徐丽丽"),
    "卢婧": ("494518423", "卢婧"),
    "成禹": ("414584470", "成禹"),
    "李辰": ("277127549", "李辰"),
    "王然": ("305550996", "王然"),
    "刘露": ("812605717", "刘露"),
    "齐雅欣": ("齐雅欣", "齐雅欣"), # ID unknown yet
    "郑可君": ("郑可君", "郑可君"), 
    "郑天兵": ("郑天兵", "郑天兵"),
    "talice": ("苗苗", "苗苗"),
    "TeachernbspLeona": ("3243141566", "Teacher Leona"),
}

def main():
    files = os.listdir(TXT_DIR)
    for filename in files:
        if "unknown" not in filename: continue
        
        path = os.path.join(TXT_DIR, filename)
        new_filename = filename
        
        # Try to find a match in RENAME_MAP
        for frag, (new_id, new_name) in RENAME_MAP.items():
            if frag in filename:
                # Replace ID unknown or Name unknown
                if f"sender_id_{frag}__sender_name_unknown" in filename:
                    new_filename = filename.replace("sender_name_unknown", new_name)
                elif f"sender_id_unknown__sender_name_{frag}" in filename:
                    new_filename = filename.replace("sender_id_unknown", new_id)
                break
        
        if new_filename != filename:
            new_path = os.path.join(TXT_DIR, new_filename)
            os.rename(path, new_path)
            print(f"Renamed: {filename} -> {new_filename}")

if __name__ == "__main__":
    main()

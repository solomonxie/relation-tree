"""
Group 6: WeChat Historical Archive Parser
-----------------------------------------
Target: blobs/Wechat3/ (MicroMsg folders)
Analysis: Extracted MicroMsg folders from legacy backups (2013-2014).
Contains image2, voice2, video folders.
Features:
1. Recursively finds and extracts media from legacy structures.
2. Converts AMR to MP3 (legacy voice format).
3. Deduplication based on file path/hash.
4. Media path: data/media/wechat_media/<hash>/<fileID>.<ext>
"""

import hashlib
import logging
import os
import shutil
import sqlite3
import sys
import subprocess
import re
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Constants
OUTPUT_DB = "data/db/raw/group6_wechat_archive.sqlite"
SCHEMA_FILE = "data/schema/raw/group6_wechat_archive.sql"
MEDIA_ROOT = "data/media/wechat_media"
ARCHIVES_DIR = "blobs/Wechat3"


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


def convert_amr_to_mp3(src_path):
    """Converts legacy AMR to MP3."""
    if not os.path.exists(src_path):
        return src_path
    dest_path = os.path.splitext(src_path)[0] + ".mp3"
    try:
        cmd = ["ffmpeg", "-y", "-loglevel", "error", "-i", src_path, dest_path]
        subprocess.run(cmd, check=True)
        if os.path.exists(dest_path):
            os.remove(src_path)
        return dest_path
    except Exception as e:
        logging.error(f"Error converting AMR {src_path}: {e}")
        return src_path


def identify_format_and_fix_ext(file_path):
    """Identifies format and fixes extension."""
    if not os.path.exists(file_path): return file_path
    with open(file_path, "rb") as f:
        header = f.read(16)
    ext = ""
    if header.startswith(b"\xff\xd8\xff"): ext = ".jpg"
    elif header.startswith(b"\x89PNG"): ext = ".png"
    elif header.startswith(b"GIF8"): ext = ".gif"
    elif b"ftyp" in header[4:12]: ext = ".mp4"
    elif header.startswith(b"#!AMR"): ext = ".amr"
    
    if ext and not file_path.lower().endswith(ext):
        new_path = file_path + ext
        os.rename(file_path, new_path)
        return new_path
    return file_path


def parse_legacy_micromsg(base_dir, out_conn):
    """Recursively finds media in MicroMsg legacy folders."""
    logging.info(f"Scanning for media in: {base_dir}")
    out_cursor = out_conn.cursor()
    source_name = "legacy_archive"

    media_count = 0
    # Search for media folders
    for root, dirs, files in os.walk(base_dir):
        # We look for image2, voice2, video, sns
        folder_name = os.path.basename(root).lower()
        mtype = "unknown"
        if folder_name == "image2": mtype = "image"
        elif folder_name == "voice2": mtype = "audio"
        elif folder_name == "video": mtype = "video"
        elif folder_name == "sns": mtype = "moment_media"
        
        if mtype == "unknown":
            continue
            
        logging.info(f"Processing media folder: {root} ({mtype})")
        
        for f in files:
            if f.startswith(".") or f == "index.dat": continue
            src_path = os.path.join(root, f)
            if not os.path.isfile(src_path): continue
            
            # Use parent folder's parent name as 'contact' if possible
            # Standard MicroMsg path: .../df128d59.../image2/ab/cd/filename
            parts = root.split(os.sep)
            contact_hash = "legacy_unknown"
            for i, p in enumerate(parts):
                if p in ["image2", "voice2", "video", "sns"] and i > 0:
                    contact_hash = parts[i-1]
                    break
            
            dest_dir = os.path.join(MEDIA_ROOT, contact_hash)
            os.makedirs(dest_dir, exist_ok=True)
            dest_path = os.path.join(dest_dir, f)
            
            try:
                if not os.path.exists(dest_path):
                    shutil.copy2(src_path, dest_path)
                
                final_path = identify_format_and_fix_ext(dest_path)
                if final_path.lower().endswith(".amr"):
                    final_path = convert_amr_to_mp3(final_path)
                
                rel_path = os.path.relpath(final_path, MEDIA_ROOT)
                file_id = hashlib.md5(src_path.encode()).hexdigest()
                
                out_cursor.execute(
                    "INSERT OR IGNORE INTO group6_raw_media (id, username, type, relative_path, original_path, file_size, source) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (file_id, contact_hash, mtype, rel_path, src_path, os.path.getsize(final_path), source_name)
                )
                media_count += 1
            except Exception as e:
                logging.error(f"Error media {src_path}: {e}")

    logging.info(f"Extracted {media_count} media files from legacy archive.")


def main():
    if not os.path.exists(ARCHIVES_DIR): return
    conn = init_db()
    # Process all subfolders in Wechat3 that are not WechatBackup (handled by group 7)
    for d in os.listdir(ARCHIVES_DIR):
        dir_path = os.path.join(ARCHIVES_DIR, d)
        if os.path.isdir(dir_path) and "WechatBackup" not in d:
            parse_legacy_micromsg(dir_path, conn)
    
    conn.commit()
    conn.close()
    logging.info("Group 6 legacy archive parsing finished.")


if __name__ == "__main__":
    main()

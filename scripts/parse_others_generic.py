"""
Other Chat Generic/Web Parser
-----------------------------
Target: blobs/others/ (Generic .txt, .mht, .html, .zip)
Analysis: Miscelaneous chat logs in various formats (web-archived
single-file MHTML, zipped HTML, plain text). These are often unstructured
and handled by metadata-only logging unless a clear text pattern is found.
Destination: other_raw_chats
"""

import logging
import os
import sqlite3
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Paths
OUTPUT_DB = "data/db/database.sqlite"
OTHERS_DIR = "blobs/others"


def parse_metadata_only(file_path, cursor, subfolder):
    """Logs metadata for files where full content parsing is not implemented."""
    filename = os.path.basename(file_path)
    ext = os.path.splitext(filename)[1].lower()
    platform = f"{ext[1:]}_metadata"
    username = (
        filename.split("(")[0].strip()
        if "(" in filename
        else filename.replace(ext, "")
    )
    mtime = int(os.path.getmtime(file_path))

    cursor.execute(
        "INSERT OR IGNORE INTO other_raw_chats "
        "(source_file, username, create_time, content, platform, subfolder) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (file_path, username, mtime, f"[Metadata Only] Chat log: {filename}",
         platform, subfolder),
    )


def parse_txt_chat(file_path, cursor, subfolder):
    """Parses simple line-by-line text chat logs."""
    platform = "generic_txt"
    try:
        filename = os.path.basename(file_path)
        username = (
            filename.split("(")[0].strip()
            if "(" in filename
            else filename.replace(".txt", "")
        )

        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                # Basic YYYY-MM-DD HH:MM:SS format detection
                if (
                    len(line) > 19
                    and line[4] == "-"
                    and line[7] == "-"
                    and line[13] == ":"
                ):
                    try:
                        ts_str = line[:19]
                        ts = int(
                            datetime.strptime(
                                ts_str, "%Y-%m-%d %H:%M:%S"
                            ).timestamp()
                        )
                        content = line[19:].strip()
                        cursor.execute(
                            "INSERT OR IGNORE INTO other_raw_chats "
                            "(source_file, username, create_time, content, "
                            "platform, subfolder) VALUES (?, ?, ?, ?, ?, ?)",
                            (file_path, username, ts, content, platform,
                             subfolder),
                        )
                    except Exception:
                        continue
    except Exception as e:
        logging.error(f"Error parsing {file_path}: {e}")


def main():
    if not os.path.exists(OTHERS_DIR):
        logging.info(f"Others directory not found: {OTHERS_DIR}")
        return

    conn = sqlite3.connect(OUTPUT_DB)
    cursor = conn.cursor()

    for item in os.listdir(OTHERS_DIR):
        item_path = os.path.join(OTHERS_DIR, item)
        if os.path.isdir(item_path):
            # Special subfolders handled by other scripts or by metadata
            for root, _, files in os.walk(item_path):
                for f in files:
                    if f.startswith("."):
                        continue
                    fpath = os.path.join(root, f)
                    ext = os.path.splitext(f)[1].lower()
                    if ext == ".txt":
                        parse_txt_chat(fpath, cursor, item)
                    elif ext in [".mht", ".mhtl", ".html", ".zip", ".docx"]:
                        parse_metadata_only(fpath, cursor, item)
        else:
            if item.startswith("."):
                continue
            ext = os.path.splitext(item)[1].lower()
            if ext == ".txt":
                # QQ archives are handled by parse_others_qq_text.py
                if "QQ" not in item:
                    parse_txt_chat(item_path, cursor, "root")
            elif ext in [".mht", ".mhtl", ".html", ".zip", ".docx"]:
                parse_metadata_only(item_path, cursor, "root")

    conn.commit()
    conn.close()
    logging.info("Generic and Web Chat parsing finished.")


if __name__ == "__main__":
    main()

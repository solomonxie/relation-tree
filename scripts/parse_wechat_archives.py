"""
WeChat Historical Archive Parser
--------------------------------
Target: blobs/Wechat3/
Analysis: Compressed archives from 2013-2016 (ZIP, 7z, RAR). These contain
extracted MicroMsg folders, SQLite databases, and text exports.
Destination: wechat_raw_messages, wechat_raw_contacts
"""

import logging
import os
import shutil
import sqlite3
import subprocess
import tempfile

# Use logic from related WeChat parsers for consistent extraction
from parse_wechat_wcdb import parse_wcdb_sqlite
from parse_wechat_text import parse_exported_text

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Paths
OUTPUT_DB = "data/db/database.sqlite"
ARCHIVES_DIR = "blobs/Wechat3"


def parse_compressed_source(archive_path, out_conn):
    """Extracts and recursively parses files from an archive."""
    logging.info(f"Parsing compressed source: {archive_path}")
    temp_dir = tempfile.mkdtemp()
    try:
        # Use 7-zip for broad format support
        subprocess.run(
            ["7z", "x", f"-o{temp_dir}", archive_path, "-y"],
            capture_output=True, check=False
        )

        for root, _, files in os.walk(temp_dir):
            for f in files:
                fpath = os.path.join(root, f)
                # Check for direct database files
                if f.endswith((".sqlite", ".db")):
                    parse_wcdb_sqlite(fpath, out_conn)
                # Check for exported text files
                elif f.endswith(".txt"):
                    # Many archives bundle text exports in a subfolder
                    parse_exported_text(root, out_conn)
                    # Break loop for this subfolder once parse_exported_text
                    # handles all txt files in it.
                    break
    except Exception as e:
        logging.error(f"Error extracting/parsing {archive_path}: {e}")
    finally:
        shutil.rmtree(temp_dir)


def main():
    if not os.path.exists(ARCHIVES_DIR):
        logging.info(f"Archives directory not found: {ARCHIVES_DIR}")
        return

    os.makedirs(os.path.dirname(OUTPUT_DB), exist_ok=True)
    conn = sqlite3.connect(OUTPUT_DB)

    for f in os.listdir(ARCHIVES_DIR):
        if f.endswith((".zip", ".7z", ".rar")):
            parse_compressed_source(os.path.join(ARCHIVES_DIR, f), conn)

    conn.commit()
    conn.close()
    logging.info("WeChat Historical Archive parsing finished.")


if __name__ == "__main__":
    main()

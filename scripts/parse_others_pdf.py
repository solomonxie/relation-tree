"""
Other Chat PDF Parser
---------------------
Target: blobs/others/PDF CHATS/
Analysis: Chat histories exported as PDF files, often containing a "日期:"
(Date) header and structured message blocks. Parsed using pdftotext
with layout preservation.
Destination: other_raw_chats
"""

import logging
import os
import re
import sqlite3
import subprocess
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Paths
OUTPUT_DB = "data/db/database.sqlite"
PDF_DIR = "blobs/others/PDF CHATS"


def parse_metadata_only(file_path, cursor, subfolder):
    """Logs metadata for files where content cannot be parsed."""
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


def parse_pdf_chat(file_path, cursor, subfolder):
    """Extracts and parses text from PDF chat logs."""
    logging.info(f"Parsing PDF chat: {file_path}")
    try:
        # Extract text using pdftotext
        result = subprocess.run(
            ["pdftotext", "-layout", file_path, "-"],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            parse_metadata_only(file_path, cursor, subfolder)
            return

        text = result.stdout
        if not text.strip():
            parse_metadata_only(file_path, cursor, subfolder)
            return

        lines = text.splitlines()
        current_date = None
        total_msgs = 0

        i = 0
        while i < len(lines):
            line = lines[i].strip()
            # Look for "日期: YYYY-MM-DD" header
            date_match = re.match(r"日期:\s*(\d{4}-\d{2}-\d{2})", line)
            if date_match:
                current_date = date_match.group(1)
                i += 1
                continue

            # Look for "User HH:MM:SS" header
            header_match = re.match(r"^(.*?)\s+(\d{2}:\d{2}:\d{2})$", line)
            if header_match and current_date:
                username = header_match.group(1).strip()
                time_str = header_match.group(2)
                timestamp_str = f"{current_date} {time_str}"

                # Gather content lines until next header
                content_lines = []
                i += 1
                while i < len(lines):
                    next_line = lines[i].strip()
                    if re.match(r"日期:\s*\d{4}-\d{2}-\d{2}", next_line) or 
                       re.match(r"^.*?\s+\d{2}:\d{2}:\d{2}$", next_line):
                        break
                    if next_line:
                        content_lines.append(next_line)
                    i += 1

                content = "
".join(content_lines).strip()
                if content:
                    try:
                        ts = int(
                            datetime.strptime(
                                timestamp_str, "%Y-%m-%d %H:%M:%S"
                            ).timestamp()
                        )
                        cursor.execute(
                            "INSERT OR IGNORE INTO other_raw_chats "
                            "(source_file, username, create_time, content, "
                            "platform, subfolder) VALUES (?, ?, ?, ?, ?, ?)",
                            (file_path, username, ts, content, "pdf_regex",
                             subfolder),
                        )
                        total_msgs += 1
                    except Exception:
                        pass
                continue
            i += 1

        if total_msgs == 0:
            parse_metadata_only(file_path, cursor, subfolder)
        else:
            logging.info(f"Extracted {total_msgs} messages.")

    except Exception as e:
        logging.error(f"Error parsing PDF {file_path}: {e}")
        parse_metadata_only(file_path, cursor, subfolder)


def main():
    if not os.path.exists(PDF_DIR):
        logging.info(f"PDF directory not found: {PDF_DIR}")
        return

    conn = sqlite3.connect(OUTPUT_DB)
    cursor = conn.cursor()

    for f in os.listdir(PDF_DIR):
        if f.endswith(".pdf"):
            parse_pdf_chat(os.path.join(PDF_DIR, f), cursor, "PDF CHATS")

    conn.commit()
    conn.close()
    logging.info("PDF Chat parsing finished.")


if __name__ == "__main__":
    main()

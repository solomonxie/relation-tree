import os
import sqlite3
import logging
from datetime import datetime
import subprocess
import re

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

MAIN_DB = "data/db/database.sqlite"
OTHERS_DIR = "blobs/others"


def setup_chats_db(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS other_raw_chats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_file TEXT,
        username TEXT,
        create_time INTEGER,
        content TEXT,
        platform TEXT,
        subfolder TEXT
    )
    """)


def parse_txt_chat(file_path, cursor, subfolder):
    """Parses text chat logs."""
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
                if (
                    len(line) > 19
                    and line[4] == "-"
                    and line[7] == "-"
                    and line[13] == ":"
                ):
                    try:
                        ts_str = line[:19]
                        ts = int(
                            datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").timestamp()
                        )
                        content = line[19:].strip()
                        cursor.execute(
                            "INSERT INTO other_raw_chats (source_file, username, create_time, content, platform, subfolder) VALUES (?, ?, ?, ?, ?, ?)",
                            (file_path, username, ts, content, platform, subfolder),
                        )
                    except Exception:
                        continue
    except Exception as e:
        logging.error(f"Error parsing {file_path}: {e}")


def parse_pdf_chat(file_path, cursor, subfolder):
    """Extracts text from PDF and uses regex to parse it into messages."""
    logging.info(f"Parsing PDF chat: {file_path}")
    try:
        result = subprocess.run(
            ["pdftotext", "-layout", file_path, "-"], capture_output=True, text=True
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
            date_match = re.match(r"日期:\s*(\d{4}-\d{2}-\d{2})", line)
            if date_match:
                current_date = date_match.group(1)
                i += 1
                continue

            header_match = re.match(r"^(.*?)\s+(\d{2}:\d{2}:\d{2})$", line)
            if header_match and current_date:
                username = header_match.group(1).strip()
                time_str = header_match.group(2)
                timestamp_str = f"{current_date} {time_str}"

                content_lines = []
                i += 1
                while i < len(lines):
                    next_line = lines[i].strip()
                    if re.match(r"日期:\s*\d{4}-\d{2}-\d{2}", next_line) or re.match(
                        r"^.*?\s+\d{2}:\d{2}:\d{2}$", next_line
                    ):
                        break
                    if next_line:
                        content_lines.append(next_line)
                    i += 1

                content = "\n".join(content_lines).strip()
                if content:
                    try:
                        ts = int(
                            datetime.strptime(
                                timestamp_str, "%Y-%m-%d %H:%M:%S"
                            ).timestamp()
                        )
                        cursor.execute(
                            "INSERT INTO other_raw_chats (source_file, username, create_time, content, platform, subfolder) VALUES (?, ?, ?, ?, ?, ?)",
                            (file_path, username, ts, content, "pdf_regex", subfolder),
                        )
                        total_msgs += 1
                    except Exception:
                        pass
                continue
            i += 1

        if total_msgs == 0:
            parse_metadata_only(file_path, cursor, subfolder)
        else:
            logging.info(f"Extracted {total_msgs} messages from {file_path}")

    except Exception as e:
        logging.error(f"Error parsing PDF {file_path}: {e}")
        parse_metadata_only(file_path, cursor, subfolder)


def parse_qq_text_chat(file_path, cursor, subfolder):
    """Parses QQ multi-line text export format."""
    logging.info(f"Parsing QQ text export: {file_path}")
    try:
        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()

        current_user = None
        current_ts = None
        current_content = []
        total_msgs = 0
        header_re = re.compile(r"^(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})\s+(.*)$")

        for line in lines:
            line = line.strip()
            if not line:
                continue
            match = header_re.match(line)
            if match:
                if current_user and current_ts and current_content:
                    cursor.execute(
                        "INSERT INTO other_raw_chats (source_file, username, create_time, content, platform, subfolder) VALUES (?, ?, ?, ?, ?, ?)",
                        (
                            file_path,
                            current_user,
                            current_ts,
                            "\n".join(current_content),
                            "qq_text",
                            subfolder,
                        ),
                    )
                    total_msgs += 1
                current_ts = int(
                    datetime.strptime(
                        f"{match.group(1)} {match.group(2)}", "%Y-%m-%d %H:%M:%S"
                    ).timestamp()
                )
                current_user = match.group(3).strip()
                current_content = []
            else:
                if current_user:
                    current_content.append(line)

        if current_user and current_ts and current_content:
            cursor.execute(
                "INSERT INTO other_raw_chats (source_file, username, create_time, content, platform, subfolder) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    file_path,
                    current_user,
                    current_ts,
                    "\n".join(current_content),
                    "qq_text",
                    subfolder,
                ),
            )
            total_msgs += 1
        logging.info(f"Extracted {total_msgs} messages from QQ text export.")
    except Exception as e:
        logging.error(f"Error parsing QQ text {file_path}: {e}")


def parse_bak_chat(file_path, cursor, subfolder):
    """Extracts strings from binary .bak files."""
    logging.info(f"Attempting to extract strings from .bak: {file_path}")
    try:
        result = subprocess.run(["strings", file_path], capture_output=True, text=True)
        if result.returncode != 0:
            parse_metadata_only(file_path, cursor, subfolder)
            return

        text = result.stdout
        filename = os.path.basename(file_path)
        username = (
            filename.split("(")[0].strip()
            if "(" in filename
            else filename.replace(".bak", "")
        )

        lines = text.splitlines()
        total_msgs = 0
        for line in lines:
            match = re.search(r"(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})", line)
            if match:
                ts_str = f"{match.group(1)} {match.group(2)}"
                content = line.replace(ts_str, "").strip()
                if content:
                    try:
                        ts = int(
                            datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").timestamp()
                        )
                        cursor.execute(
                            "INSERT INTO other_raw_chats (source_file, username, create_time, content, platform, subfolder) VALUES (?, ?, ?, ?, ?, ?)",
                            (file_path, username, ts, content, "bak_strings", subfolder),
                        )
                        total_msgs += 1
                    except Exception:
                        continue
        if total_msgs == 0:
            parse_metadata_only(file_path, cursor, subfolder)
    except Exception as e:
        logging.error(f"Error parsing .bak {file_path}: {e}")
        parse_metadata_only(file_path, cursor, subfolder)


def parse_metadata_only(file_path, cursor, subfolder):
    """Logs metadata for complex formats."""
    filename = os.path.basename(file_path)
    ext = os.path.splitext(filename)[1].lower()
    platform = f"{ext[1:]}_metadata"
    username = (
        filename.split("(")[0].strip() if "(" in filename else filename.replace(ext, "")
    )
    mtime = int(os.path.getmtime(file_path))

    cursor.execute(
        "INSERT INTO other_raw_chats (source_file, username, create_time, content, platform, subfolder) VALUES (?, ?, ?, ?, ?, ?)",
        (
            file_path,
            username,
            mtime,
            f"[Metadata Only] Chat log: {filename}",
            platform,
            subfolder,
        ),
    )


def main():
    if not os.path.exists(OTHERS_DIR):
        return

    conn = sqlite3.connect(MAIN_DB)
    cursor = conn.cursor()
    setup_chats_db(cursor)

    for item in os.listdir(OTHERS_DIR):
        item_path = os.path.join(OTHERS_DIR, item)
        if os.path.isdir(item_path):
            for root, _, files in os.walk(item_path):
                for f in files:
                    if f.startswith("."):
                        continue
                    fpath = os.path.join(root, f)
                    ext = os.path.splitext(f)[1].lower()
                    if ext == ".txt":
                        parse_txt_chat(fpath, cursor, item)
                    elif ext == ".pdf":
                        parse_pdf_chat(fpath, cursor, item)
                    elif ext == ".bak":
                        parse_bak_chat(fpath, cursor, item)
                    else:
                        parse_metadata_only(fpath, cursor, item)
        else:
            if item.startswith("."):
                continue
            ext = os.path.splitext(item)[1].lower()
            if ext == ".txt":
                if "QQ" in item and "chat_history" in item:
                    parse_qq_text_chat(item_path, cursor, "root")
                else:
                    parse_txt_chat(item_path, cursor, "root")
            elif ext == ".pdf":
                parse_pdf_chat(item_path, cursor, "root")
            elif ext == ".bak":
                parse_bak_chat(item_path, cursor, "root")
            elif ext in [".mht", ".mhtl", ".zip"]:
                parse_metadata_only(item_path, cursor, "root")

    conn.commit()
    conn.close()
    logging.info("Others chats ingested.")


if __name__ == "__main__":
    main()

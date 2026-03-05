"""
Group 5: WeChat Standard SQLite and Media Parser
-----------------------------------------------
Target: blobs/Wechat/ (MM*.sqlite + images/ + voice/)
Analysis: Standard WeChat (WCDB) SQLite databases and media folders.
Features:
1. Extract contact info only from the target blobs folder (blobs/Wechat).
2. Maps hashed chat table names and media filenames back to real contact IDs using internal maps.
3. Parses messages from 'Chat_[hash]' tables in MM*.sqlite.
4. Converts XML-formatted messages to descriptive plain text (Title + Description).
5. Links media files to contacts using filename hash fragments.
6. Converts AMR to MP3 and identifies image formats.
7. Independent: No references to other groups or external SQLite databases.
"""

import hashlib
import logging
import os
import re
import shutil
import sqlite3
import subprocess
import xml.etree.ElementTree as ET

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Constants
OUTPUT_DB = "data/db/raw/group5_wechat_forensic.sqlite"
SCHEMA_FILE = "data/schema/raw/group5_wechat_forensic.sql"
WECHAT_DIR = "blobs/Wechat"
MEDIA_ROOT = "data/media/wechat_media"


def init_db():
    """Initialize DB using the local schema file."""
    if os.path.exists(OUTPUT_DB):
        os.remove(OUTPUT_DB)
    os.makedirs(os.path.dirname(OUTPUT_DB), exist_ok=True)
    with open(SCHEMA_FILE, "r") as f:
        schema_sql = f.read()
    conn = sqlite3.connect(OUTPUT_DB)
    conn.executescript(schema_sql)
    conn.commit()
    return conn


def get_internal_contact_mapping(wechat_dir, out_conn):
    """
    Extracts contact mapping from internal SQLite files in the target folder.
    Returns:
        id_to_nick: {id: nickname}
        hash_to_id: {md5(id): id}
        prefix_to_id: {md5(id)[:8]: id}
    """
    id_to_nick = {}
    hash_to_id = {}
    prefix_to_id = {}

    out_cursor = out_conn.cursor()

    for filename in os.listdir(wechat_dir):
        if not (filename.endswith(".sqlite") or filename.endswith(".db")):
            continue
        
        sqlite_path = os.path.join(wechat_dir, filename)
        try:
            conn = sqlite3.connect(sqlite_path)
            cursor = conn.cursor()
            
            # Check if 'Friend' table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Friend'")
            if cursor.fetchone():
                cursor.execute("SELECT UsrName, NickName FROM Friend")
                for row in cursor.fetchall():
                    uname, unick = row
                    if not uname or len(uname) < 2:
                        continue
                    
                    id_to_nick[uname] = unick
                    uhash = hashlib.md5(uname.encode("utf-8")).hexdigest()
                    hash_to_id[uhash] = uname
                    prefix_to_id[uhash[:8]] = uname

                    # Pre-populate output DB contacts
                    out_cursor.execute(
                        "INSERT OR IGNORE INTO group5_raw_contacts (username, nickname) VALUES (?, ?)",
                        (uname, unick),
                    )
            
            conn.close()
        except Exception as e:
            logging.debug(f"Info: Could not load contacts from {sqlite_path}: {e}")

    return id_to_nick, hash_to_id, prefix_to_id


def clean_xml_content(content):
    """Converts WeChat XML messages to descriptive plain text."""
    if not content or not content.strip().startswith("<"):
        return content

    if "appmsg" not in content and "msg" not in content and "voicemsg" not in content:
        return content

    try:
        xml_data = content.strip()
        if not xml_data.startswith("<?xml"):
            xml_data = f"<?xml version='1.0' encoding='UTF-8'?><root>{xml_data}</root>"
        else:
            if xml_data.count("<msg") > 1 or xml_data.count("<appmsg") > 1:
                xml_data = xml_data.replace('<?xml version="1.0"?>', "")
                xml_data = f"<root>{xml_data}</root>"

        root = ET.fromstring(xml_data)

        voice = root.find(".//voicemsg")
        if voice is not None:
            vlen = voice.get("voicelength", "0")
            return f"[Voice Message: {int(vlen) / 1000:.1f}s]"

        title = root.find(".//title")
        des = root.find(".//des")

        parts = []
        if title is not None and title.text:
            parts.append(title.text.strip())
        if des is not None and des.text:
            dtext = des.text.strip()
            if not parts or dtext not in parts[0]:
                parts.append(dtext)

        if parts:
            return " | ".join(parts)

        text_parts = [t.strip() for t in root.itertext() if t and t.strip()]
        if text_parts:
            res = " ".join(text_parts)
            return re.sub(r"\s+", " ", res).strip()

        return content
    except Exception:
        title_m = re.search(r"<title>(.*?)</title>", content, re.DOTALL)
        des_m = re.search(r"<des>(.*?)</des>", content, re.DOTALL)

        parts = []
        if title_m:
            parts.append(
                re.sub(
                    r"<!\[CDATA\[(.*?)\]\]>", r"\1", title_m.group(1), flags=re.DOTALL
                ).strip()
            )
        if des_m:
            parts.append(
                re.sub(
                    r"<!\[CDATA\[(.*?)\]\]>", r"\1", des_m.group(1), flags=re.DOTALL
                ).strip()
            )

        return " | ".join(parts) if parts else content


try:
    import pilk
    HAS_PILK = True
except ImportError:
    HAS_PILK = False

# Setup logging
...
def convert_audio_to_mp3(src_path, dest_path):
    """Converts Silk or AMR to MP3."""
    if not os.path.exists(src_path): return False
    
    with open(src_path, "rb") as f:
        header = f.read(10)
    
    # 1. Handle Silk
    if HAS_PILK and (header.startswith(b"#!SILK_V3") or header.startswith(b"\x02#!SILK_V3")):
        actual_silk_path = src_path
        temp_silk = None
        if header.startswith(b"\x02#!SILK_V3"):
            temp_silk = src_path + ".tmp.silk"
            with open(src_path, "rb") as f:
                f.seek(1)
                with open(temp_silk, "wb") as tf: tf.write(f.read())
            actual_silk_path = temp_silk
        
        pcm_path = src_path + ".pcm"
        try:
            pilk.decode(actual_silk_path, pcm_path)
            cmd = ["ffmpeg", "-y", "-loglevel", "error", "-f", "s16le", "-ar", "24000", "-ac", "1", "-i", pcm_path, dest_path]
            subprocess.run(cmd, check=True)
            return True
        except Exception as e:
            logging.error(f"Error converting silk {src_path}: {e}")
        finally:
            if os.path.exists(pcm_path): os.remove(pcm_path)
            if temp_silk and os.path.exists(temp_silk): os.remove(temp_silk)
    
    # 2. Handle AMR or others using ffmpeg directly
    try:
        cmd = ["ffmpeg", "-y", "-loglevel", "error", "-i", src_path, dest_path]
        subprocess.run(cmd, check=True)
        return True
    except Exception:
        return False


def identify_format_and_fix_ext(file_path):
    """Identifies format and fixes extension."""
    if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
        return file_path
    try:
        with open(file_path, "rb") as f:
            header = f.read(16)
        ext = ""
        if header.startswith(b"\xff\xd8\xff"): ext = ".jpg"
        elif header.startswith(b"\x89PNG"): ext = ".png"
        elif header.startswith(b"GIF8"): ext = ".gif"
        elif b"ftyp" in header[4:12]: ext = ".mp4"
        elif header.startswith(b"#!AMR"): ext = ".amr"
        elif header.startswith(b"#!SILK_V3") or header.startswith(b"\x02#!SILK_V3"): ext = ".silk"

        if ext and not file_path.lower().endswith(ext):
            new_path = file_path + ext
            if os.path.exists(new_path):
                if os.path.getsize(new_path) == os.path.getsize(file_path):
                    os.remove(file_path)
                    return new_path
                else:
                    new_path = file_path + "_fixed" + ext
            os.rename(file_path, new_path)
            return new_path
    except Exception:
        pass
    return file_path


def compute_msg_hash(username, create_time, content):
    """Computes a unique hash."""
    base_str = f"{username}|{create_time}|{content}"
    return hashlib.md5(base_str.encode("utf-8", errors="replace")).hexdigest()


def parse_wcdb_sqlite(sqlite_path, out_conn, id_to_nick, hash_to_id):
    """Parses standard WeChat message tables."""
    logging.info(f"Parsing WCDB messages: {sqlite_path}")
    source_name = f"sqlite_{os.path.basename(sqlite_path)}"
    out_cursor = out_conn.cursor()

    try:
        conn = sqlite3.connect(sqlite_path)
        cursor = conn.cursor()

        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'Chat_%' AND name NOT LIKE 'ChatExt%'"
        )
        chat_tables = [r[0] for r in cursor.fetchall()]

        total_msgs = 0
        for table in chat_tables:
            uhash = table.replace("Chat_", "")
            username = hash_to_id.get(uhash, uhash)
            nickname = id_to_nick.get(username, f"Unknown_{username[:8]}")

            out_cursor.execute(
                "INSERT OR IGNORE INTO group5_raw_contacts (username, nickname) VALUES (?, ?)",
                (username, nickname),
            )

            cursor.execute(f"PRAGMA table_info({table})")
            columns = [c[1].lower() for c in cursor.fetchall()]
            m_col = "message" if "message" in columns else "content"
            t_col = "createtime" if "createtime" in columns else "create_time"
            l_col = "meslocalid" if "meslocalid" in columns else "id"

            cursor.execute(f"SELECT {t_col}, {m_col}, {l_col} FROM {table}")
            for row in cursor.fetchall():
                if not row[1]:
                    continue
                clean_content = clean_xml_content(row[1])
                m_hash = compute_msg_hash(username, row[0], clean_content)
                out_cursor.execute(
                    "INSERT OR IGNORE INTO group5_raw_messages "
                    "(username, create_time, content, local_id, source, msg_hash) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (username, row[0], clean_content, row[2], source_name, m_hash),
                )
                total_msgs += 1

        conn.close()
        logging.info(f"Inserted {total_msgs} messages from {sqlite_path}")
    except Exception as e:
        logging.error(f"Error parsing messages from {sqlite_path}: {e}")


def parse_media(wechat_dir, out_conn, hash_to_id, prefix_to_id):
    """Scans media folders and logs files using fuzzy matching."""
    logging.info("Scanning media folders...")
    out_cursor = out_conn.cursor()
    source_name = "wechat_legacy_media"
    media_count = 0

    media_dirs = [os.path.join(wechat_dir, "images"), os.path.join(wechat_dir, "voice")]
    sorted_prefixes = sorted(prefix_to_id.keys(), key=len, reverse=True)

    for mdir in media_dirs:
        if not os.path.exists(mdir):
            continue
        for root, _, files in os.walk(mdir):
            for f in files:
                if f.startswith(".") or f == "index.dat":
                    continue
                src_path = os.path.join(root, f)

                real_id = "legacy_unknown"
                for h, uid in hash_to_id.items():
                    if h in f:
                        real_id = uid
                        break

                if real_id == "legacy_unknown":
                    for p in sorted_prefixes:
                        if p in f:
                            real_id = prefix_to_id[p]
                            break

                dest_dir = os.path.join(MEDIA_ROOT, real_id)
                os.makedirs(dest_dir, exist_ok=True)
                dest_path = os.path.join(dest_dir, f)

                try:
                    if not os.path.exists(dest_path):
                        shutil.copy2(src_path, dest_path)

                    final_path = identify_format_and_fix_ext(dest_path)
                    if final_path.lower().endswith(".amr"):
                        mp3_path = os.path.splitext(final_path)[0] + ".mp3"
                        if convert_audio_to_mp3(final_path, mp3_path):
                            os.remove(final_path)
                            final_path = mp3_path

                    mtype = "unknown"
                    if final_path.lower().endswith((".jpg", ".png", ".gif", ".jpeg")):
                        mtype = "image"
                    elif final_path.lower().endswith(".mp3"):
                        mtype = "audio"

                    rel_path = os.path.relpath(final_path, MEDIA_ROOT)
                    file_id = hashlib.md5(src_path.encode()).hexdigest()

                    out_cursor.execute(
                        "INSERT OR IGNORE INTO group5_raw_media "
                        "(id, username, type, relative_path, original_path, file_size, source) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (
                            file_id,
                            real_id,
                            mtype,
                            rel_path,
                            src_path,
                            os.path.getsize(final_path),
                            source_name,
                        ),
                    )
                    media_count += 1
                except Exception as e:
                    logging.error(f"Error processing media {src_path}: {e}")

    logging.info(f"Extracted {media_count} media files.")


def main():
    if not os.path.exists(WECHAT_DIR):
        return

    conn = init_db()
    id_to_nick, hash_to_id, prefix_to_id = get_internal_contact_mapping(WECHAT_DIR, conn)
    logging.info(f"Loaded {len(id_to_nick)} internal contacts.")

    for f in os.listdir(WECHAT_DIR):
        if f.endswith(".sqlite") or f.endswith(".db"):
            if "MM" in f:
                parse_wcdb_sqlite(os.path.join(WECHAT_DIR, f), conn, id_to_nick, hash_to_id)

    parse_media(WECHAT_DIR, conn, hash_to_id, prefix_to_id)

    conn.commit()
    conn.close()
    logging.info("Group 5 parsing finished.")


if __name__ == "__main__":
    main()

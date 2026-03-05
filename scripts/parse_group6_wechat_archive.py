"""
Group 6: WeChat Historical Archive Parser
-----------------------------------------
Target: blobs/Wechat3/ (MicroMsg folders)
Analysis: Extracted MicroMsg folders from legacy backups (2013-2014).
Contains image2, voice2, video folders.
Features:
1. Extract contacts from binary logs (locallog/*.bin).
2. Recursively finds and extracts media from legacy structures.
3. Converts Silk/AMR to MP3 and fixes image/video extensions.
4. Media path: data/media/wechat_media/<hash>/<fileID>.<ext>
5. Independent: Each group script must be independent.
"""

import hashlib
import logging
import os
import shutil
import sqlite3
import subprocess
import re

# Try to import pilk for Silk decoding
try:
    import pilk
    HAS_PILK = True
except ImportError:
    HAS_PILK = False

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
    with open(SCHEMA_FILE, "r") as f:
        schema_sql = f.read()
    conn = sqlite3.connect(OUTPUT_DB)
    conn.executescript(schema_sql)
    conn.commit()
    return conn


def extract_contacts_from_bin(bin_path, out_conn):
    """Simple regex based contact extraction from binary logs."""
    if not os.path.exists(bin_path):
        return
    try:
        with open(bin_path, "rb") as f:
            data = f.read()
        # Find strings that look like wechat IDs or chatroom IDs
        # Patterns: wxid_..., @chatroom, @qqim, or simple alphanumeric
        matches = re.findall(rb"[\w\d_\-@]{6,32}", data)
        out_cursor = out_conn.cursor()
        count = 0
        for m in matches:
            try:
                uname = m.decode("utf-8", errors="ignore")
                if "@" in uname or uname.startswith("wxid_") or len(uname) > 10:
                    out_cursor.execute(
                        "INSERT OR IGNORE INTO group6_raw_contacts (username, nickname) VALUES (?, ?)",
                        (uname, f"Extracted_{uname[:8]}"),
                    )
                    count += 1
            except Exception:
                continue
        logging.info(f"Extracted {count} potential contacts from {bin_path}")
    except Exception as e:
        logging.error(f"Error reading bin file {bin_path}: {e}")


def convert_audio_to_mp3(src_path, dest_path):
    """Converts Silk or AMR to MP3."""
    if not os.path.exists(src_path):
        return False

    with open(src_path, "rb") as f:
        header = f.read(10)

    # 1. Handle Silk
    if HAS_PILK and (
        header.startswith(b"#!SILK_V3") or header.startswith(b"\x02#!SILK_V3")
    ):
        actual_silk_path = src_path
        temp_silk = None
        if header.startswith(b"\x02#!SILK_V3"):
            temp_silk = src_path + ".tmp.silk"
            with open(src_path, "rb") as f:
                f.seek(1)
                with open(temp_silk, "wb") as tf:
                    tf.write(f.read())
            actual_silk_path = temp_silk

        pcm_path = src_path + ".pcm"
        try:
            pilk.decode(actual_silk_path, pcm_path)
            cmd = [
                "ffmpeg",
                "-y",
                "-loglevel",
                "error",
                "-f",
                "s16le",
                "-ar",
                "24000",
                "-ac",
                "1",
                "-i",
                "pcm_path",
                dest_path,
            ]
            # Wait, pcm_path is a variable not string
            cmd = [
                "ffmpeg",
                "-y",
                "-loglevel",
                "error",
                "-f",
                "s16le",
                "-ar",
                "24000",
                "-ac",
                "1",
                "-i",
                pcm_path,
                dest_path,
            ]
            subprocess.run(cmd, check=True)
            return True
        except Exception as e:
            logging.error(f"Error converting silk {src_path}: {e}")
        finally:
            if os.path.exists(pcm_path):
                os.remove(pcm_path)
            if temp_silk and os.path.exists(temp_silk):
                os.remove(temp_silk)

    # 2. Handle AMR or others using ffmpeg directly
    try:
        cmd = ["ffmpeg", "-y", "-loglevel", "error", "-i", src_path, dest_path]
        subprocess.run(cmd, check=True)
        return True
    except Exception:
        return False


def identify_format_and_fix_ext(file_path):
    """Identifies format and fixes extension based on file header."""
    if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
        return file_path
    try:
        with open(file_path, "rb") as f:
            header = f.read(16)
        ext = ""
        if header.startswith(b"\xff\xd8\xff"):
            ext = ".jpg"
        elif header.startswith(b"\x89PNG"):
            ext = ".png"
        elif header.startswith(b"GIF8"):
            ext = ".gif"
        elif b"ftyp" in header[4:12]:
            ext = ".mp4"
        elif header.startswith(b"#!AMR"):
            ext = ".amr"
        elif header.startswith(b"#!SILK_V3") or header.startswith(b"\x02#!SILK_V3"):
            ext = ".silk"

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
    except Exception as e:
        logging.debug(f"Error identifying format for {file_path}: {e}")
    return file_path


def parse_legacy_micromsg(base_dir, out_conn):
    """Recursively finds media in MicroMsg legacy folders."""
    logging.info(f"Scanning for media in: {base_dir}")
    out_cursor = out_conn.cursor()

    # Search for bin logs first
    for root, dirs, files in os.walk(base_dir):
        if "locallog" in root.lower():
            for f in files:
                if f.endswith(".bin"):
                    extract_contacts_from_bin(os.path.join(root, f), out_conn)

    # We want to find folders that are 32-char hex hashes (MicroMsg owner folders)
    owner_folders = []
    for root, dirs, files in os.walk(base_dir):
        for d in dirs:
            if len(d) == 32 and all(c in "0123456789abcdef" for c in d.lower()):
                folder_full_path = os.path.join(root, d)
                subdirs = os.listdir(folder_full_path)
                if any(t in subdirs for t in ["image2", "voice2", "video", "sns"]):
                    owner_folders.append(folder_full_path)

    if not owner_folders:
        owner_folders = [base_dir]

    media_count = 0
    TARGET_FOLDERS = ["image2", "voice2", "video", "sns", "avatar", "image", "voice"]

    for owner_path in owner_folders:
        owner_hash = os.path.basename(owner_path)
        if len(owner_hash) != 32:
            owner_hash = "legacy_unknown"

        logging.info(f"Processing owner folder: {owner_path} (hash: {owner_hash})")
        source_name = f"legacy_{os.path.basename(base_dir.rstrip(os.sep))}"
        if owner_hash != "legacy_unknown":
            source_name += f"_{owner_hash[:8]}"

        for root, dirs, files in os.walk(owner_path):
            parts = [p.lower() for p in root.split(os.sep)]
            mtype = "unknown"
            in_target = False

            if "sns" in parts:
                in_target = True
                mtype = "moment_media"
            else:
                for t in TARGET_FOLDERS:
                    if t in parts:
                        in_target = True
                        if "image" in t or "avatar" in t:
                            mtype = "image"
                        elif "voice" in t:
                            mtype = "audio"
                        elif "video" in t:
                            mtype = "video"
                        break

            if not in_target:
                continue

            for f in files:
                if f.startswith(".") or f == "index.dat" or f == ".nomedia":
                    continue
                src_path = os.path.join(root, f)
                if not os.path.isfile(src_path):
                    continue
                if os.path.getsize(src_path) < 10:
                    continue

                real_id = owner_hash
                # Fallback to identify contact by hash in filename if possible (some image names are md5 of contact? not sure)
                # But requested to be independent, so we don't have hash_to_id here.
                # We'll just use owner_hash as primary contact for now.

                dest_dir = os.path.join(MEDIA_ROOT, real_id)
                os.makedirs(dest_dir, exist_ok=True)
                dest_path = os.path.join(dest_dir, f)

                try:
                    if not os.path.exists(dest_path):
                        shutil.copy2(src_path, dest_path)

                    final_path = identify_format_and_fix_ext(dest_path)

                    if final_path.lower().endswith((".amr", ".silk", ".aud")):
                        mp3_path = os.path.splitext(final_path)[0] + ".mp3"
                        if not os.path.exists(mp3_path):
                            if convert_audio_to_mp3(final_path, mp3_path):
                                if os.path.exists(final_path):
                                    os.remove(final_path)
                                final_path = mp3_path
                        else:
                            if os.path.exists(final_path):
                                os.remove(final_path)
                            final_path = mp3_path

                    lower_final = final_path.lower()
                    if mtype == "unknown":
                        if lower_final.endswith((".jpg", ".png", ".gif", ".jpeg")):
                            mtype = "image"
                        elif lower_final.endswith(".mp3"):
                            mtype = "audio"
                        elif lower_final.endswith(".mp4"):
                            mtype = "video"
                    elif mtype == "moment_media":
                        if lower_final.endswith(".mp4"):
                            mtype = "video"

                    rel_path = os.path.relpath(final_path, MEDIA_ROOT)
                    file_id = hashlib.md5(src_path.encode()).hexdigest()

                    out_cursor.execute(
                        "INSERT OR IGNORE INTO group6_raw_media (id, username, type, relative_path, original_path, file_size, source) VALUES (?, ?, ?, ?, ?, ?, ?)",
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
                    if media_count % 1000 == 0:
                        logging.info(f"Processed {media_count} media files...")
                except Exception as e:
                    logging.error(f"Error media {src_path}: {e}")

    logging.info(f"Extracted total {media_count} media files from {base_dir}.")


def main():
    if not os.path.exists(ARCHIVES_DIR):
        logging.error(f"Archives directory not found: {ARCHIVES_DIR}")
        return

    conn = init_db()

    for d in os.listdir(ARCHIVES_DIR):
        dir_path = os.path.join(ARCHIVES_DIR, d)
        if os.path.isdir(dir_path) and "WechatBackup" not in d:
            parse_legacy_micromsg(dir_path, conn)

    conn.commit()
    conn.close()
    logging.info("Group 6 legacy archive parsing finished.")


if __name__ == "__main__":
    main()

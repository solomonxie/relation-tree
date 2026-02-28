import os
import subprocess
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

BASE_DIR = "blobs/Wechat3"

def decompress_files():
    if not os.path.exists(BASE_DIR):
        logging.error(f"Directory {BASE_DIR} not found.")
        return

    files = [f for f in os.listdir(BASE_DIR) if os.path.isfile(os.path.join(BASE_DIR, f))]
    logging.info(f"Found {len(files)} files in {BASE_DIR}")

    for f in files:
        ext = os.path.splitext(f)[1].lower()
        if ext not in ['.zip', '.rar', '.7z']:
            continue

        file_path = os.path.join(BASE_DIR, f)
        # Folder name is filename without extension
        folder_name = os.path.splitext(f)[0]
        dest_dir = os.path.join(BASE_DIR, folder_name)

        if os.path.exists(dest_dir):
            logging.info(f"Skipping {f}, folder {folder_name} already exists.")
            continue

        os.makedirs(dest_dir, exist_ok=True)
        logging.info(f"Decompressing {f} into {folder_name}...")

        try:
            if ext == '.zip':
                # Prefer 7z if available as it handles more encodings
                subprocess.run(['7z', 'x', f'-o{dest_dir}', file_path], check=True)
            elif ext in ['.rar', '.7z']:
                subprocess.run(['7z', 'x', f'-o{dest_dir}', file_path], check=True)
            logging.info(f"Successfully decompressed {f}")
        except subprocess.CalledProcessError as e:
            logging.error(f"Failed to decompress {f}: {e}")
            # Cleanup failed directory
            if os.path.exists(dest_dir) and not os.listdir(dest_dir):
                os.rmdir(dest_dir)

if __name__ == "__main__":
    decompress_files()

"""
Master Ingestion Runner
-----------------------
Executes all specialized data parsers in the correct sequence.
"""

import subprocess
import logging
import os

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

SCRIPTS = [
    "scripts/setup_db.py",
    "scripts/parse_old_rldt.py",
    "scripts/parse_wechat_ios.py",
    "scripts/parse_wechat_wcdb.py",
    "scripts/parse_wechat_internal_db.py",
    "scripts/parse_wechat_text.py",
    "scripts/parse_wechat_forensic.py",
    "scripts/parse_wechat_archives.py",
    "scripts/parse_others_pdf.py",
    "scripts/parse_others_qq_text.py",
    "scripts/parse_others_bak.py",
    "scripts/parse_others_generic.py",
    "scripts/merge_dbs.py",
    "scripts/fetch_emails.py",
    "scripts/process_wechat_media.py"
]

def main():
    for script in SCRIPTS:
        if not os.path.exists(script):
            logging.warning(f"Script not found: {script}")
            continue
            
        logging.info(f"--- Running {script} ---")
        try:
            # We use check=True to stop if setup fails, 
            # but maybe continue for others? 
            # For now, let's stop on any failure to be safe.
            subprocess.run(["python3", script], check=True)
        except subprocess.CalledProcessError as e:
            logging.error(f"Script {script} failed: {e}")
            if "setup_db.py" in script:
                logging.error("Database setup failed. Aborting ingestion.")
                break

if __name__ == "__main__":
    main()

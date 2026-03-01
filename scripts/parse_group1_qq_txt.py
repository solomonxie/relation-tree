"""
Group 1: QQ TXT Parser
Source: blobs/qq_txt/*.txt
Features:
2. Attributes messages using the contact mapping.
3. Correctly handles Group vs Person chats.
"""

import logging
import os
import re
from glob import glob
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Configuration for Local LLM
LLM_API_BASE = os.getenv("LLM_API_BASE", "http://localhost:11434/v1")
LLM_API_KEY = os.getenv("LLM_API_KEY", "ollama")
MODEL_NAME = os.getenv("LLM_MODEL", "llama3:latest")

# OUTPUT_DB = "data/db/raw/group1_html.sqlite"
OWNER_NAME = '几何体'


def parse_file(filepath):
    print(f'processing {filepath}')
    name_counter = defaultdict(int)
    group_name = ''
    qqid = '00000000'
    date_ = '1900-01-01'
    time_ = '00:00:00'
    name = ''
    msg_start = 0

    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        lines = f.read().splitlines()

    i = 0
    while i < len(lines):
        line = lines[i].strip()
        # todo: remove `','MS Sans Serif',sans-serif;" color='000000'>` if appears in line
        time_match = re.match(r'^(\d{1,2}:\d{2}:\d{2})$', line)

        if line.startswith('消息对象:'):
            obj_name = line.replace('消息对象:', '').strip()
            qq_match = re.search(r'(\d+)', obj_name)
            if qq_match:
                qqid = qq_match.group(1)
            elif obj_name != OWNER_NAME:
                name_counter[obj_name] += 1
        elif line.startswith('消息分组:'):
            group_name = line.replace('消息分组:', '').strip()
        elif line.startswith('日期:'):
            if msg_start > 0:
                msg = '\n'.join(lines[msg_start:i]).strip()
                print(f'Read message [{name}] [{group_name}] [{date_} {time_}]: {msg}')
                msg_start = 0

            if i + 1 < len(lines):
                date_match = re.search(r'(\d{4}-\d{2}-\d{2})', lines[i+1])
                if date_match:
                    date_ = date_match.group(1)
                    i += 1
        elif time_match:
            if msg_start > 0:
                msg = '\n'.join(lines[msg_start:i-1]).strip()
                print(f'Read message [{name}] [{date_} {time_}]: {msg}')

            time_ = time_match.group(1)
            name = lines[i-1].strip()
            msg_start = i + 1
            if name != OWNER_NAME:
                name_counter[name] += 1
        i += 1

    if msg_start > 0:
        msg = '\n'.join(lines[msg_start:]).strip()
        print(f'Read message [{name}] [{group_name}] [{date_} {time_}]: {msg}')
    return


def main():
    files = glob("blobs/qq_txt/*.txt")
    logging.info(f"Found {len(files)} files to process.")
    for filepath in sorted(files):
        try:
            parse_file(filepath)
        except Exception as e:
            logging.error(f"Failed to process {filepath}: {e}")
            raise
    logging.info("Processing complete.")


if __name__ == '__main__':
    main()

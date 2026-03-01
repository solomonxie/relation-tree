"""
Group 1: QQ TXT Parser (Contact Table Refactor)
Source: blobs/qq_txt/*.txt
Features:
1. Populates 'contacts' table (id, type, name, nicknames, grouping).
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

OUTPUT_DB = "data/db/raw/group1_html.sqlite"
OWNER_NAME = '几何体'


def parse_file(filepath):
    print(f'processing {filepath}')
    name_counter = defaultdict(int)
    group_name = ''
    qqid = '00000000'
    date_ = '1900-01-01'
    time_ = '00:00:00'
    msg = ''
    lines = open(filepath).read().splitlines()
    i = 0
    msg_start = 0
    __import__('pudb').set_trace()
    while i < len(lines):
        line = lines[i].strip()
        time_match = re.match(r'^(\d{1,2}:\d{2}:\d{2})$', line)
        if line.startswith('消息对象:'):
            name = line.replace('消息对象:', '')  # todo: remove special characters
            qq_match = re.match(r'(\d+)', name)
            if qq_match:
                qqid = int(qq_match[0])
            elif name != OWNER_NAME:
                name_counter[name] += 1
        elif line.startswith('消息分组:'):
            group_name = line.replace('消息分组:', '').strip()
        elif line.startswith('日期:'):
            date_match = re.match(r'^\s*(\d{4}-\d{2}-\d{2})$', lines[i+1])
            if date_match:
                date_ = date_match[1]
                i += 1
                msg_start = i + 2
        elif time_match:
            # Stop collection for current message:
            if msg_start > 0:
                msg = '\n'.join(lines[msg_start: i-1]).strip()  # fixme: the slicing isn't right
                print(f'Read message [{name}] [{group_name}] [{date_} {time_}]: {msg}')
            # Start a new message collection:
            msg_start = i + 2
            time_ = time_match[0]
            name = lines[i-1].strip()  # todo: remove special characters
            if name != OWNER_NAME:
                name_counter[name] += 1
        i += 1
    return


def main():
    files = glob("blobs/qq_txt/*.txt")
    logging.info(f"Found {len(files)} files to process.")
    for filepath in files:
        try:
            parse_file(filepath)
        except Exception as e:
            logging.error(f"Failed to process {filepath}: {e}")
            raise
    logging.info("Processing complete.")


if __name__ == '__main__':
    main()

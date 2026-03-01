"""
Group 1: QQ TXT Parser
Source: blobs/qq_txt/*.txt
Features:
2. Attributes messages using the contact mapping.
3. Correctly handles Group vs Person chats.
"""

import os
import re
import sys
import logging
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

OWNER_NAME = '几何体'

def clean_msg(msg):
    # Remove styling tags like ','MS Sans Serif',sans-serif;" color='000000'>
    # Targeting font family, color, and size styling tags commonly found in QQ logs
    msg = re.sub(r"[^>\n]*'(?:MS Sans Serif|Tahoma|Arial|宋体|微软雅黑|Times New Roman)'[^>\n]*>", "", msg)
    # Generic cleanup for remaining font/color markers
    msg = re.sub(r"<font[^>]*>|</font>", "", msg)
    return msg.strip()

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
        time_match = re.match(r'^(\d{1,2}:\d{2}:\d{2})$', line)

        if line.startswith('消息对象:'):
            obj_name = line.replace('消息对象:', '').strip()
            qq_match = re.search(r'(\d+)', obj_name)
            if qq_match:
                qqid = qq_match.group(1)
            if obj_name != OWNER_NAME:
                name_counter[obj_name] += 1
        elif line.startswith('消息分组:'):
            group_name = line.replace('消息分组:', '').strip()
        elif line.startswith('日期:'):
            if msg_start > 0:
                msg = clean_msg('\n'.join(lines[msg_start:i]))
                if msg:
                    print(f'Read message [{name}] [{group_name}] [{date_} {time_}]: {msg}')
                msg_start = 0

            # Check current line first
            date_match = re.search(r'(\d{4}-\d{2}-\d{2})', line)
            if date_match:
                date_ = date_match.group(1)
            elif i + 1 < len(lines):
                # Check next line
                date_match = re.search(r'(\d{4}-\d{2}-\d{2})', lines[i+1])
                if date_match:
                    date_ = date_match.group(1)
                    i += 1
        elif time_match:
            if msg_start > 0:
                msg = clean_msg('\n'.join(lines[msg_start:i-1]))
                if msg:
                    print(f'Read message [{name}] [{group_name}] [{date_} {time_}]: {msg}')

            time_ = time_match.group(1)
            name = lines[i-1].strip()
            msg_start = i + 1
            if name != OWNER_NAME:
                name_counter[name] += 1
        i += 1

    if msg_start > 0:
        msg = clean_msg('\n'.join(lines[msg_start:]))
        if msg:
            print(f'Read message [{name}] [{group_name}] [{date_} {time_}]: {msg}')
    return


def main():
    if len(sys.argv) > 1:
        files = [sys.argv[1]]
    else:
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

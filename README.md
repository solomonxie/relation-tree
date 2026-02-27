# MyNetworks

This repo is to develop an interface (Web, Mobile, CLI) to record my personal relationships with AI enhanced details recording.
More importantly, it also allows to store each person's information and media I've collected as reference.

Some features:
- Person information
- Person analysis
- Family tree / Ancestral trace

## Usage

### 1. Initialize Database
```bash
make init
```

### 2. LLM Information Extraction
To extract information from raw notes using a local LLM (e.g., Ollama):
1. Configure your `.env` file:
   ```env
   LLM_API_BASE=http://localhost:11434/v1
   LLM_API_KEY=ollama
   LLM_MODEL=llama3
   ```
2. Run the extraction script:
   ```bash
   ./venv/bin/python3 scripts/extract_people_info.py blobs/people-notes-canada.txt
   ```

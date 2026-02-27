import os
import json
import sqlite3
import requests
from dotenv import load_dotenv
from collections import defaultdict

load_dotenv()

LLM_API_BASE = os.getenv("LLM_API_BASE", "http://localhost:11434/v1")
LLM_API_KEY = os.getenv("LLM_API_KEY", "ollama")
MODEL_NAME = os.getenv("LLM_MODEL", "llama3")

DB_PATH = "data/db/database.sqlite"
MERGE_PLAN_PATH = "/tmp/merge_plan.json"

def get_potential_groups():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get all persons with their contacts
    cursor.execute("""
        SELECT p.id, p.name, p.display_name, p.nick_name, p.birthdate, p.notes,
               GROUP_CONCAT(c.type || ':' || c.value, '|') as contacts
        FROM persons p
        LEFT JOIN contacts c ON p.id = c.person_id
        GROUP BY p.id
    """)
    
    persons = []
    for row in cursor.fetchall():
        persons.append({
            'id': row[0],
            'name': row[1],
            'display_name': row[2],
            'nick_name': row[3],
            'birthdate': row[4],
            'notes': row[5],
            'contacts': row[6].split('|') if row[6] else []
        })
    
    conn.close()
    
    # Group by exact name or shared phone/email
    groups = defaultdict(set)
    
    # Maps for tracking
    name_map = defaultdict(list)
    contact_map = defaultdict(list)
    
    for p in persons:
        if p['name']:
            name_map[p['name'].strip().lower()].append(p['id'])
        for c in p['contacts']:
            if ':' in c:
                ctype, cval = c.split(':', 1)
                val = cval.strip().lower()
                # Filter out generic/too-short values
                if len(val) > 3 and val not in ['canada', 'china', 'united states']:
                    contact_map[val].append(p['id'])
    
    # Form groups from names
    for name, ids in name_map.items():
        if len(ids) > 1:
            group_key = f"name_{name}"
            for pid in ids: groups[group_key].add(pid)
            
    # Form groups from contacts
    for val, ids in contact_map.items():
        if len(ids) > 1:
            group_key = f"contact_{val}"
            for pid in ids: groups[group_key].add(pid)
            
    # Consolidate overlapping groups
    final_groups = []
    seen_ids = set()
    
    # Sort groups by size to process larger ones first or just iterate
    sorted_group_ids = sorted(groups.values(), key=len, reverse=True)
    
    merged_groups = []
    for g in sorted_group_ids:
        added = False
        for mg in merged_groups:
            if not g.isdisjoint(mg):
                mg.update(g)
                added = True
                break
        if not added:
            merged_groups.append(g)
            
    # Map IDs back to person data
    person_lookup = {p['id']: p for p in persons}
    candidate_data = []
    for mg in merged_groups:
        candidate_data.append([person_lookup[pid] for pid in mg])
        
    return candidate_data

PROMPT_TEMPLATE = """
Analyze the following groups of person records and determine if they represent the SAME physical person.
For each group, decide which records should be merged.

Return a JSON object with a "merges" list. 
Each item in "merges" should have:
- "primary_id": The ID of the record to KEEP.
- "redundant_ids": A list of IDs to be MERGED INTO the primary record and then deleted.

If a group should NOT be merged, omit it from the list.

Candidates:
{candidates_json}

JSON Output Format:
{{
    "merges": [
        {{ "primary_id": 1, "redundant_ids": [2, 3] }}
    ]
}}
"""

def query_llm(candidates):
    payload = {
        "model": MODEL_NAME,
        "messages": [
            {"role": "system", "content": "You are an expert data steward specialized in entity resolution and deduplication."},
            {"role": "user", "content": PROMPT_TEMPLATE.format(candidates_json=json.dumps(candidates, indent=2, ensure_ascii=False))}
        ],
        "temperature": 0.1,
        "response_format": {"type": "json_object"}
    }
    
    try:
        response = requests.post(f"{LLM_API_BASE}/chat/completions", 
                                 headers={"Authorization": f"Bearer {LLM_API_KEY}"},
                                 json=payload)
        response.raise_for_status()
        content = response.json()['choices'][0]['message']['content']
        return json.loads(content)
    except Exception as e:
        print(f"Error querying LLM: {e}")
        return {"merges": []}

def main():
    print("Finding potential merge candidates...")
    candidate_groups = get_potential_groups()
    print(f"Found {len(candidate_groups)} potential merge groups.")
    
    all_merges = []
    
    # Process groups in batches to LLM
    batch_size = 5 # Small batches for better accuracy and token limits
    for i in range(0, len(candidate_groups), batch_size):
        batch = candidate_groups[i:i+batch_size]
        print(f"Processing batch {i//batch_size + 1}/{(len(candidate_groups)-1)//batch_size + 1}...")
        result = query_llm(batch)
        if result and "merges" in result:
            all_merges.extend(result["merges"])
            
    with open(MERGE_PLAN_PATH, 'w', encoding='utf-8') as f:
        json.dump({"merges": all_merges}, f, indent=2, ensure_ascii=False)
        
    print(f"Merge plan written to {MERGE_PLAN_PATH}")

if __name__ == "__main__":
    main()

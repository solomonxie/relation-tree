import sqlite3
import csv

DB_PATH = "data/db/database.sqlite"
OUTPUT_CSV = "/tmp/persons_for_merging.csv"

def export_for_merging():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get persons and their contacts
    cursor.execute("""
        SELECT 
            p.id, p.name, p.display_name, p.nick_name, p.birthdate, p.notes,
            GROUP_CONCAT(c.type || ':' || c.value, ' | ') as all_contacts
        FROM persons p
        LEFT JOIN contacts c ON p.id = c.person_id
        GROUP BY p.id
    """)
    
    rows = cursor.fetchall()
    colnames = [d[0] for d in cursor.description]
    
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(colnames)
        writer.writerows(rows)
        
    conn.close()
    print(f"Exported {len(rows)} records to {OUTPUT_CSV}")

if __name__ == "__main__":
    export_for_merging()

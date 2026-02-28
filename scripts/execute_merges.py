import sqlite3
import json
import os
import argparse

DB_PATH = "data/db/database.sqlite"
MERGE_PLAN_PATH = "/tmp/merge_plan.json"


def execute_merges(plan, dry_run=False):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # List of tables that have person_id or similar
    # career, contacts, education, financial_information, media, person_groups, person_positions, property, relationships

    merges = plan.get("merges", [])
    print(f"Total merges to perform: {len(merges)}")

    for m in merges:
        primary_id = m["primary_id"]
        redundant_ids = m["redundant_ids"]

        print(f"Merging {redundant_ids} into {primary_id}...")

        if dry_run:
            continue

        try:
            # Start transaction for this merge
            cursor.execute("BEGIN TRANSACTION")

            for rid in redundant_ids:
                # 1. Update simple person_id references
                tables_with_person_id = [
                    "career",
                    "contacts",
                    "education",
                    "financial_information",
                    "media",
                    "person_groups",
                    "person_positions",
                    "property",
                ]
                for table in tables_with_person_id:
                    cursor.execute(
                        f"UPDATE {table} SET person_id = ? WHERE person_id = ?",
                        (primary_id, rid),
                    )

                # 2. Update relationships (both person1_id and person2_id)
                cursor.execute(
                    "UPDATE relationships SET person1_id = ? WHERE person1_id = ?",
                    (primary_id, rid),
                )
                cursor.execute(
                    "UPDATE relationships SET person2_id = ? WHERE person2_id = ?",
                    (primary_id, rid),
                )

                # 3. Delete redundant person
                cursor.execute("DELETE FROM persons WHERE id = ?", (rid,))

            conn.commit()
            print(f"Successfully merged {len(redundant_ids)} records into {primary_id}")

        except Exception as e:
            conn.rollback()
            print(f"Error merging {redundant_ids} into {primary_id}: {e}")

    conn.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not os.path.exists(MERGE_PLAN_PATH):
        print(f"Plan file not found: {MERGE_PLAN_PATH}")
        return

    with open(MERGE_PLAN_PATH, "r", encoding="utf-8") as f:
        plan = json.load(f)

    execute_merges(plan, dry_run=args.dry_run)


if __name__ == "__main__":
    main()

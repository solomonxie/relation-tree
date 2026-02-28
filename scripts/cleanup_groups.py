import sqlite3

DB_PATH = "data/db/database.sqlite"


def cleanup():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 1. Fetch all groups
    cursor.execute("SELECT id, name, type FROM groups")
    groups = cursor.fetchall()

    # Map for normalization (more specific first)
    normalization_map = {
        "Young Adult": ("FBC Young Adults", "church group"),
        "YA group": ("FBC Young Adults", "church group"),
        "YA": ("FBC Young Adults", "church group"),
        "Choir": ("FBC Choir", "church group"),
        "Worship team": ("FBC Worship Team", "church group"),
        "Street ministry": ("FBC Street Ministry", "church group"),
        "Street Ministries": ("FBC Street Ministry", "church group"),
        "Alpha marriage": ("FBC Alpha Marriage Group", "church group"),
        "Connection group": ("FBC Connection Group", "church group"),
        "Kitchen service": ("FBC Kitchen Service", "church group"),
        "Asher team": ("FBC Asher Team", "church group"),
        "Youth group": ("FBC Youth Group", "church group"),
        "Children's Ministry": ("FBC Children's Ministry", "church group"),
        "Executive Ministry": ("FBC Executive Ministry", "church group"),
        "MINISTER OF CONGREGATIONAL CARE": ("FBC Congregational Care", "church group"),
        "MINISTER OF DISCIPLESHIP AND COMMUNITY": (
            "FBC Discipleship & Community",
            "church group",
        ),
        "Downtown Young Couples": ("FBC Downtown Young Couples", "church group"),
        "FBC": ("First Baptist Church", "church group"),
        "First Baptist Church": ("First Baptist Church", "church group"),
    }

    for gid, name, gtype in groups:
        new_name, new_type = None, None

        lower_name = name.lower()
        for key, (norm_name, norm_type) in normalization_map.items():
            if key.lower() in lower_name:
                new_name, new_type = norm_name, norm_type
                break

        if new_name:
            # Check if normalized group already exists
            cursor.execute(
                "SELECT id FROM groups WHERE name = ? AND id != ?", (new_name, gid)
            )
            row = cursor.fetchone()
            if row:
                target_id = row[0]
                # Update person_groups to point to the existing normalized group
                cursor.execute(
                    "UPDATE person_groups SET group_id = ? WHERE group_id = ?",
                    (target_id, gid),
                )
                # Delete the redundant group
                cursor.execute("DELETE FROM groups WHERE id = ?", (gid,))
                print(f"Merged group '{name}' into '{new_name}'")
            else:
                # Rename the current group
                cursor.execute(
                    "UPDATE groups SET name = ?, type = ? WHERE id = ?",
                    (new_name, new_type, gid),
                )
                print(f"Normalized group '{name}' to '{new_name}'")

    conn.commit()
    conn.close()


if __name__ == "__main__":
    cleanup()

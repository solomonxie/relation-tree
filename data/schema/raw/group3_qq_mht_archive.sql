DROP TABLE IF EXISTS group3_raw_qq_mht_archive;
CREATE TABLE group3_raw_qq_mht_archive (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_file TEXT,
    sender_name TEXT,
    sender_id INTEGER,
    receiver_name TEXT,
    receiver_id INTEGER,
    nicknames TEXT,
    create_time INTEGER,
    content TEXT
);

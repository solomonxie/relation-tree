DROP TABLE IF EXISTS group2_raw_mhtml;
CREATE TABLE group2_raw_mhtml (
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

DROP TABLE IF EXISTS other_raw_chats;
CREATE TABLE other_raw_chats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_file TEXT,
    username TEXT,
    nickname TEXT,
    create_time INTEGER,
    content TEXT,
    platform TEXT,
    subfolder TEXT,
    msg_hash TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_other_raw_chats_hash ON other_raw_chats (msg_hash);

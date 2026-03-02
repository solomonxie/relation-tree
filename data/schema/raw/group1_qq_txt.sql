DROP TABLE IF EXISTS group1_qq_txt_raw_chats;
CREATE TABLE group1_qq_txt_raw_chats (
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

DROP TABLE IF EXISTS group1_qq_txt_contacts;
CREATE TABLE group1_qq_txt_contacts (
    qqid TEXT PRIMARY KEY,
    name TEXT,
    nicknames TEXT
);

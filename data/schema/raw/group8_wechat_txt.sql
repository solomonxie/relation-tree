CREATE TABLE IF NOT EXISTS group8_raw_contacts (
    username TEXT PRIMARY KEY,
    nickname TEXT,
    remark TEXT,
    type INTEGER
);

CREATE TABLE IF NOT EXISTS group8_raw_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT,
    create_time INTEGER,
    content TEXT,
    local_id INTEGER,
    source TEXT,
    msg_hash TEXT UNIQUE
);

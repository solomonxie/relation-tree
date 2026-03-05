CREATE TABLE IF NOT EXISTS group4_raw_contacts (
    username TEXT PRIMARY KEY,
    nickname TEXT,
    remark TEXT,
    type INTEGER
);

CREATE TABLE IF NOT EXISTS group4_raw_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT,
    create_time INTEGER,
    content TEXT,
    local_id INTEGER,
    source TEXT,
    msg_hash TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS group4_raw_moments (
    id TEXT PRIMARY KEY,
    username TEXT,
    nickname TEXT,
    create_time INTEGER,
    content TEXT,
    msg_hash TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS group4_raw_media (
    id TEXT PRIMARY KEY,
    username TEXT,
    type TEXT,
    relative_path TEXT,
    original_path TEXT,
    file_size INTEGER,
    source TEXT
);

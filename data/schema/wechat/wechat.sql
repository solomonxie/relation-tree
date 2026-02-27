-- wechat.sql
-- WeChat messages, contacts, and moments
-- Created: 2026-02-25
-- Database: wechat.sqlite

CREATE TABLE IF NOT EXISTS contacts (
    username TEXT PRIMARY KEY,
    nickname TEXT,
    type INTEGER
);

CREATE TABLE IF NOT EXISTS messages (
    username TEXT,
    create_time INTEGER,
    content TEXT,
    local_id INTEGER,
    source TEXT,
    PRIMARY KEY (username, local_id, source)
);

CREATE TABLE IF NOT EXISTS moments (
    id TEXT PRIMARY KEY,
    username TEXT,
    nickname TEXT,
    create_time INTEGER,
    content TEXT
);

CREATE TABLE IF NOT EXISTS media (
    id TEXT PRIMARY KEY,
    username TEXT,
    type TEXT, -- 'audio', 'video', 'image', 'document'
    relative_path TEXT,
    original_path TEXT,
    file_size INTEGER,
    source TEXT
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_messages_username ON messages(username);
CREATE INDEX IF NOT EXISTS idx_messages_time ON messages(create_time);
CREATE INDEX IF NOT EXISTS idx_messages_source ON messages(source);
CREATE INDEX IF NOT EXISTS idx_moments_username ON moments(username);
CREATE INDEX IF NOT EXISTS idx_moments_time ON moments(create_time);

-- Full-text search for messages
CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
    username,
    content,
    content=messages,
    content_rowid=rowid
);

-- Triggers to keep FTS in sync
CREATE TRIGGER IF NOT EXISTS messages_ai AFTER INSERT ON messages BEGIN
    INSERT INTO messages_fts(rowid, username, content)
    VALUES (new.rowid, new.username, new.content);
END;

CREATE TRIGGER IF NOT EXISTS messages_ad AFTER DELETE ON messages BEGIN
    DELETE FROM messages_fts WHERE rowid = old.rowid;
END;

CREATE TRIGGER IF NOT EXISTS messages_au AFTER UPDATE ON messages BEGIN
    UPDATE messages_fts
    SET username = new.username,
        content = new.content
    WHERE rowid = new.rowid;
END;

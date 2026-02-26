-- Persons table: Stores basic biographical information
CREATE TABLE IF NOT EXISTS persons (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    folder_hash TEXT UNIQUE, -- Used for organizing media folders
    gender TEXT,
    birthdate DATE,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Media table: Stores metadata for photos, videos, audios, and files
CREATE TABLE IF NOT EXISTS media (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    person_id INTEGER,
    file_path TEXT NOT NULL,
    file_type TEXT, -- e.g., '.jpg', '.mp3', '.mp4', '.pdf'
    original_filename TEXT,
    file_hash TEXT, -- 16-char hash for file naming/integrity
    encryption_status BOOLEAN DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (person_id) REFERENCES persons(id)
);

-- Relationships table: Records connections between individuals
CREATE TABLE IF NOT EXISTS relationships (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    person1_id INTEGER NOT NULL,
    person2_id INTEGER NOT NULL,
    type TEXT, -- e.g., 'parent', 'child', 'spouse', 'friend'
    start_time DATE,
    end_time DATE,
    notes TEXT,
    FOREIGN KEY (person1_id) REFERENCES persons(id),
    FOREIGN KEY (person2_id) REFERENCES persons(id)
);

-- Contacts table: Stores various contact methods for each person
CREATE TABLE IF NOT EXISTS contacts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    person_id INTEGER NOT NULL,
    type TEXT, -- e.g., 'phone', 'email', 'address'
    value TEXT NOT NULL,
    FOREIGN KEY (person_id) REFERENCES persons(id)
);

-- Consolidated schema for persons database

-- Persons table: Stores basic biographical information
CREATE TABLE IF NOT EXISTS persons (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    folder_hash TEXT UNIQUE, -- Used for organizing media folders
    title TEXT,
    display_name TEXT,
    nick_name TEXT,
    other_names TEXT, -- Format: <country>:<name>;<country>:<name>
    gender TEXT,
    birthdate DATE,
    brief TEXT,
    origins TEXT,
    ethnicity TEXT,
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

-- Education table
CREATE TABLE IF NOT EXISTS education (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    person_id INTEGER NOT NULL,
    school TEXT,
    degree TEXT,
    major TEXT,
    start_date DATE,
    end_date DATE,
    notes TEXT,
    FOREIGN KEY (person_id) REFERENCES persons(id)
);

-- Financial information table
CREATE TABLE IF NOT EXISTS financial_information (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    person_id INTEGER NOT NULL,
    type TEXT, -- e.g., 'interact', 'etransfer', 'bank account'
    country TEXT,
    details TEXT,
    FOREIGN KEY (person_id) REFERENCES persons(id)
);

-- Career table
CREATE TABLE IF NOT EXISTS career (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    person_id INTEGER NOT NULL,
    company TEXT,
    role TEXT,
    start_date DATE,
    end_date DATE,
    notes TEXT,
    FOREIGN KEY (person_id) REFERENCES persons(id)
);

-- Groups table
CREATE TABLE IF NOT EXISTS groups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    type TEXT, -- e.g., 'family group', 'church group', 'sport group'
    name TEXT NOT NULL
);

-- Person-Group relationship table
CREATE TABLE IF NOT EXISTS person_groups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    person_id INTEGER NOT NULL,
    group_id INTEGER NOT NULL,
    role TEXT,
    FOREIGN KEY (person_id) REFERENCES persons(id),
    FOREIGN KEY (group_id) REFERENCES groups(id)
);

-- Property table
CREATE TABLE IF NOT EXISTS property (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    person_id INTEGER NOT NULL,
    type TEXT, -- e.g., 'house', 'car', 'bike', 'game console'
    details TEXT, -- e.g., address for houses, model for cars
    FOREIGN KEY (person_id) REFERENCES persons(id)
);

-- Positions/Roles table
CREATE TABLE IF NOT EXISTS positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);

-- Person-Position relationship table
CREATE TABLE IF NOT EXISTS person_positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    person_id INTEGER NOT NULL,
    position_id INTEGER NOT NULL,
    organization TEXT,
    notes TEXT,
    FOREIGN KEY (person_id) REFERENCES persons(id),
    FOREIGN KEY (position_id) REFERENCES positions(id)
);

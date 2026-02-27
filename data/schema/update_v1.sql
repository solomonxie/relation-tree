-- Expand persons table
ALTER TABLE persons ADD COLUMN display_name TEXT;
ALTER TABLE persons ADD COLUMN nick_name TEXT;
ALTER TABLE persons ADD COLUMN other_names TEXT; -- Format: <country>:<name>;<country>:<name>
ALTER TABLE persons ADD COLUMN brief TEXT;
ALTER TABLE persons ADD COLUMN origins TEXT;
ALTER TABLE persons ADD COLUMN ethnicity TEXT;

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

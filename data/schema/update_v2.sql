-- Add title to persons
ALTER TABLE persons ADD COLUMN title TEXT;

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

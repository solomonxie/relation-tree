# Data Schemas

This directory contains SQL schema files for initializing the various databases used in the project.

## Subdirectories

### [persons/](./persons/)
Contains the schema for the main persons database (`database.sqlite`).
- `schema.sql`: The consolidated main schema including persons, contacts, media, education, etc.

### [wechat/](./wechat/)
Contains the schema for the WeChat-specific database (`wechat.sqlite`).
- `wechat.sql`: Schema for WeChat messages, contacts, moments, and media.

## Usage
To initialize the main database, run:
```bash
sqlite3 data/db/database.sqlite < data/schema/persons/schema.sql
```
To initialize the WeChat database, run:
```bash
sqlite3 data/db/wechat.sqlite < data/schema/wechat/wechat.sql
```

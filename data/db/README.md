# Databases

This directory contains the SQLite database files for the project.

## Main Databases

### `database.sqlite`
The primary database containing the consolidated information about people, their relationships, contact info, and organized media metadata. 
- **Tables**: `persons`, `contacts`, `media`, `relationships`, `wechat_messages`, `emails`, `other_messages`.

### `wechat.sqlite`
A specialized database for storing processed WeChat data before it is merged into the main database.
- **Tables**: `messages`, `contacts`, `moments`, `media`.

## Intermediate Databases

### `chats.sqlite` (temporary)
Used during the parsing of miscellaneous chat logs from `blobs/others/`.
- **Tables**: `raw_chats`.

## Logging
Database operation logs are kept in the `../log/` directory.

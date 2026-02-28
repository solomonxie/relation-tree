import os
import imaplib
import email
import sqlite3
import logging
import hashlib
from email.header import decode_header
from dotenv import load_dotenv

load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

MAIN_DB = "data/db/database.sqlite"
EMAIL_BLOB_DIR = "blobs/emails"


def setup_db(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS emails (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        person_id INTEGER,
        message_id TEXT UNIQUE,
        subject TEXT,
        sender TEXT,
        recipient TEXT,
        date TEXT,
        body TEXT,
        blob_path TEXT,
        FOREIGN KEY (person_id) REFERENCES persons(id)
    )
    """)


def get_or_create_person(cursor, name, email_addr):
    cursor.execute(
        "SELECT person_id FROM contacts WHERE type = 'email' AND value = ?",
        (email_addr,),
    )
    row = cursor.fetchone()
    if row:
        return row[0]

    # Try find by name
    cursor.execute("SELECT id FROM persons WHERE name = ?", (name,))
    row = cursor.fetchone()
    if row:
        person_id = row[0]
    else:
        cursor.execute("INSERT INTO persons (name) VALUES (?)", (name,))
        person_id = cursor.lastrowid

    cursor.execute(
        "INSERT INTO contacts (person_id, type, value) VALUES (?, 'email', ?)",
        (person_id, email_addr),
    )
    return person_id


def fetch_emails():
    # Example config from .env: EMAIL_SERVERS='[{"host":"imap.gmail.com","user":"..","pass":".."},{"host":"imap-mail.outlook.com","user":"..","pass":".."}]'
    # For this script, we'll assume basic ENV vars for a single server for demonstration,
    # but structure it to be extensible.

    server_configs = os.getenv("EMAIL_CONFIGS")
    if not server_configs:
        logging.error("EMAIL_CONFIGS not found in .env")
        return

    import json

    configs = json.loads(server_configs)

    os.makedirs(EMAIL_BLOB_DIR, exist_ok=True)
    conn = sqlite3.connect(MAIN_DB)
    cursor = conn.cursor()
    setup_db(cursor)

    for config in configs:
        logging.info(f"Connecting to {config['host']}...")
        try:
            mail = imaplib.IMAP4_SSL(config["host"])
            mail.login(config["user"], config["pass"])
            mail.select("inbox")

            status, messages = mail.search(None, "ALL")
            for num in messages[0].split()[-50:]:  # Process last 50 for demo
                status, data = mail.fetch(num, "(RFC822)")
                for response_part in data:
                    if isinstance(response_part, tuple):
                        msg = email.message_from_bytes(response_part[1])
                        subject, encoding = decode_header(msg["Subject"])[0]
                        if isinstance(subject, bytes):
                            subject = subject.decode(encoding or "utf-8")

                        sender = msg.get("From")
                        date = msg.get("Date")
                        msg_id = msg.get("Message-ID")

                        # Extract name and email
                        from_parsed = email.utils.parseaddr(sender)
                        from_name, from_addr = from_parsed

                        # Save blob
                        safe_id = (
                            hashlib.md5(msg_id.encode()).hexdigest()
                            if msg_id
                            else hashlib.md5(str(date).encode()).hexdigest()
                        )
                        blob_path = os.path.join(EMAIL_BLOB_DIR, f"{safe_id}.eml")
                        with open(blob_path, "wb") as f:
                            f.write(response_part[1])

                        # Get body
                        body = ""
                        if msg.is_multipart():
                            for part in msg.walk():
                                if part.get_content_type() == "text/plain":
                                    body = part.get_payload(decode=True).decode(
                                        errors="replace"
                                    )
                                    break
                        else:
                            body = msg.get_payload(decode=True).decode(errors="replace")

                        person_id = get_or_create_person(
                            cursor, from_name or from_addr, from_addr
                        )

                        cursor.execute(
                            """
                        INSERT OR IGNORE INTO emails (person_id, message_id, subject, sender, date, body, blob_path)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                            (person_id, msg_id, subject, sender, date, body, blob_path),
                        )

            mail.logout()
        except Exception as e:
            logging.error(f"Error with {config['host']}: {e}")

    conn.commit()
    conn.close()


if __name__ == "__main__":
    fetch_emails()

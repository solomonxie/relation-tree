import os
import sqlite3
import subprocess
from rich.console import Console
from rich.table import Table
from rich.prompt import Prompt, IntPrompt

# Configuration
DB_PATH = "data/db/wechat.sqlite"
MEDIA_DIR = "data/media/wechat_media"

console = Console()

def get_db_connection():
    return sqlite3.connect(DB_PATH)

def list_contacts():
    conn = get_db_connection()
    cursor = conn.cursor()
    # Join with some message counts to find active contacts
    query = """
        SELECT c.username, c.nickname, COUNT(m.username) as msg_count
        FROM contacts c
        LEFT JOIN messages m ON c.username = m.username
        GROUP BY c.username
        HAVING msg_count > 0
        ORDER BY msg_count DESC
    """
    cursor.execute(query)
    contacts = cursor.fetchall()
    conn.close()
    
    table = Table(title="WeChat Contacts")
    table.add_column("ID", justify="right", style="cyan", no_wrap=True)
    table.add_column("Nickname", style="magenta")
    table.add_column("Username/Hash", style="green")
    table.add_column("Messages", justify="right", style="yellow")
    
    contact_list = []
    for i, (username, nickname, count) in enumerate(contacts):
        name = nickname or "Unknown"
        table.add_row(str(i+1), name, username, str(count))
        contact_list.append((username, name))
        
    console.print(table)
    return contact_list

def show_contact_info(username, nickname):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM contacts WHERE username = ?", (username,))
    row = cursor.fetchone()
    conn.close()
    
    console.print(f"
[bold cyan]--- Contact Info: {nickname} ---[/bold cyan]")
    if row:
        # Assuming schema: username, nickname, type, etc.
        # Let's just print available data
        console.print(f"Username: {row[0]}")
        console.print(f"Nickname: {row[1]}")
        if len(row) > 2: console.print(f"Type: {row[2]}")
    else:
        console.print("No detailed record found in contacts table.")

def list_and_open_media(username, mtype):
    conn = get_db_connection()
    cursor = conn.cursor()
    # The 'username' in media table might be different from the contact username 
    # (it might be the owner's hash). But our flattened files are prefixed with 
    # the contact's hash if we derived it correctly.
    # Actually, WeChat media is organized by contact hash.
    # Let's search by filename prefix
    
    prefix = f"{username}_"
    if len(username) != 32:
        # If not a hash, we might need to find the hash for this user
        # But usually we store by hash.
        pass

    cursor.execute(
        "SELECT relative_path, content FROM messages WHERE username = ? AND message_type = ?",
        (username, mtype)
    )
    rows = cursor.fetchall()
    conn.close()
    
    if not rows:
        console.print(f"[yellow]No {mtype} files found for this contact.[/yellow]")
        return

    table = Table(title=f"{mtype.capitalize()} Files")
    table.add_column("ID", justify="right", style="cyan")
    table.add_column("File", style="green")
    table.add_column("Info", style="magenta")
    
    files = []
    for i, (path, info) in enumerate(rows):
        table.add_row(str(i+1), path, info)
        files.append(path)
        
    console.print(table)
    
    choice = IntPrompt.ask("Select a file to reveal in Finder (0 to cancel)", default=0)
    if 0 < choice <= len(files):
        abs_path = os.path.abspath(os.path.join(MEDIA_DIR, files[choice-1]))
        if os.path.exists(abs_path):
            console.print(f"Opening: {abs_path}")
            subprocess.run(["open", "-R", abs_path])
        else:
            console.print(f"[red]File not found: {abs_path}[/red]")

def browse_menu(username, nickname):
    while True:
        console.print(f"
[bold green]Menu for {nickname}:[/bold green]")
        console.print("1. Lookup Basic Info")
        console.print("2. Browse Images")
        console.print("3. Browse Voices")
        console.print("4. Browse Videos")
        console.print("0. Back to Contact List")
        
        choice = Prompt.ask("Choose an option", choices=["1", "2", "3", "4", "0"])
        
        if choice == "1":
            show_contact_info(username, nickname)
        elif choice == "2":
            list_and_open_media(username, "image")
        elif choice == "3":
            list_and_open_media(username, "audio")
        elif choice == "4":
            list_and_open_media(username, "video")
        elif choice == "0":
            break

def main():
    if not os.path.exists(DB_PATH):
        console.print(f"[red]Error: Database not found at {DB_PATH}[/red]")
        return

    while True:
        contacts = list_contacts()
        choice = IntPrompt.ask("
Select a contact ID to browse (0 to exit)", default=0)
        
        if choice == 0:
            break
        if 0 < choice <= len(contacts):
            username, nickname = contacts[choice-1]
            browse_menu(username, nickname)
        else:
            console.print("[red]Invalid selection.[/red]")

if __name__ == "__main__":
    main()

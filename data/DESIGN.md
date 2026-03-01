## Instructions

1. **Database Reset:** Each extraction script must delete its target `.sqlite` database at the start of the execution to ensure a clean state and prevent duplicate/stale data.
2. **Unique Raw Tables:** To facilitate traceability and auditing, each group must output into its own unique raw table within its respective database (e.g., `group1_raw_html`).
3. **User Info Extraction:** Senders with combined names and IDs (e.g., `冯泽(610784125)`) must be split into two separate fields: `sender_name` (e.g., `冯泽`) and `sender_id` (e.g., `610784125`).
4. **Consistency:** All message tables should follow a consistent schema for easier merging later.


## Group 1:
**Source:** `blobs/qq_txt/*.txt`
**Script:** `scripts/parse_group1_html.py`
**Pre-processor:** `scripts/preprocess_group1.py` (Converts `blobs/HTML_CHATS/*.htm` to structured TXT)

**Description:**
Legacy QQ message history exports. Pre-processed into structured TXT files where filenames contain metadata: `sender_id_{id}__sender_name_{name}__receiver_name_{me}.txt`. This allows reliable identification of the chat partner even when message headers only contain names.

**Result:**
- Inserted ~283,000 message records into `group1_html.sqlite` (Table: `group1_raw_html`)
- **Example output result (db record):**
source_file|sender_name|sender_id|create_time|content|platform|subfolder|msg_hash
blobs/qq_txt/sender_id_414584470__sender_name_梁雪__receiver_name_几何体.txt|梁雪|414584470|1408014744|专业课11月15、16考|txt_parsed|qq_txt|a1b2c3d4e5f6...

**Verification Strategy:**
```sql
-- Count total records
SELECT COUNT(*) FROM group1_raw_html;
```


## Group 2:
**Source:** `blobs/MHTML_CHATS/*.mht`
**Script:** `scripts/parse_group2_mhtml.py`

**Description:**
Single-file web archives containing QQ chat logs.

**Result:**
- Inserted xx message records into `group2_mhtml.sqlite` (Table: `group2_raw_mhtml`)

**Verification Strategy:**
```sql
-- Count records extracted from MHTML
SELECT COUNT(*) FROM group2_raw_mhtml;
```


## Group 3:
**Source:** `'blobs/PDF_CHATS/*.pdf'`
**Script:** `scripts/parse_group3_pdf.py`

**Description:**
Chat histories exported as PDF files.

**Result:**
- Inserted xx message records into `group3_pdf.sqlite` (Table: `group3_raw_pdf`)

**Verification Strategy:**
```sql
-- Check total messages from PDF sources
SELECT COUNT(*) FROM group3_raw_pdf;
```


## Group 4:
**Source:** `'blobs/QQ/*.bak'`
**Script:** `scripts/parse_group4_qq_bak.py`

**Description:**
Binary backup files from QQ.

**Result:**
- Inserted xx message records into `group4_qq_bak.sqlite` (Table: `group4_raw_qq_bak`)

**Verification Strategy:**
```sql
-- Check for successfully extracted strings
SELECT COUNT(*) FROM group4_raw_qq_bak;
```


## Group 5:
**Source:** `'blobs/Tencent_TT/*.bak'`
**Script:** `scripts/parse_group5_tt_bak.py`

**Description:**
Legacy binary backups from Tencent TT browser.

**Result:**
- Inserted xx message records into `group5_tt_bak.sqlite` (Table: `group5_raw_tt_bak`)

**Verification Strategy:**
```sql
-- Verify record count for TT browser backups
SELECT COUNT(*) FROM group5_raw_tt_bak;
```


## Group 6:
**Source:** `'blobs/TXT_CHATS/*.txt'`
**Script:** `scripts/parse_group6_txt.py`

**Description:**
Plain text chat logs.

**Result:**
- Inserted xx message records into `group6_txt.sqlite` (Table: `group6_raw_txt`)

**Verification Strategy:**
```sql
-- Check records
SELECT COUNT(*) FROM group6_raw_txt;
```


## Group 7:
**Source:** `blobs/Wechat/*`
**Script:** `scripts/parse_group7_wechat_ios.py`

**Description:**
Raw WeChat iOS data folders.

**Result:**
- Inserted xx message records into `group7_wechat_ios.sqlite` (Table: `group7_raw_messages`)

**Verification Strategy:**
```sql
-- Verify total message count from iOS backup
SELECT COUNT(*) FROM group7_raw_messages;
```


## Group 8:
**Source:** `blobs/Wechat_txt/*`
**Script:** `scripts/parse_group8_wechat_txt.py`

**Description:**
Manual text exports from WeChat.

**Result:**
- Inserted xx message records into `group8_wechat_txt.sqlite` (Table: `group8_raw_wechat_txt`)

**Verification Strategy:**
```sql
-- Sample check
SELECT COUNT(*) FROM group8_raw_wechat_txt;
```


## Group 9:
**Source:** `blobs/wechat_20260627/*`
**Script:** `scripts/parse_group9_wechat_forensic.py`

**Description:**
Recent forensic/backup WeChat export.

**Result:**
- Inserted xx message records into `group9_wechat_forensic.sqlite` (Table: `group9_raw_forensic`)

**Verification Strategy:**
```sql
-- Count records
SELECT COUNT(*) FROM group9_raw_forensic;
```


## Group 10:
**Sources:**
- `'blobs/Wechat3/MicroMsg[...]/'` (Legacy 2013-2014)
**Script:** `scripts/parse_group10_wechat_archive.py`

**Description:**
Legacy WeChat folders with `EnMicroMsg.db`.

**Result:**
- Inserted xx message records into `group10_wechat_archive.sqlite` (Table: `group10_raw_archive`)

**Verification Strategy:**
```sql
-- Verify records
SELECT COUNT(*) FROM group10_raw_archive;
```


## Group 11:
**Source:** `'blobs/Wechat3/WechatBackup[2016-03-11]'`
**Script:** `scripts/parse_group11_wechat_ios_2016.py`

**Description:**
iOS backup of WeChat from 2016.

**Result:**
- Inserted xx message records into `group11_wechat_ios_2016.sqlite` (Table: `group11_raw_ios_2016`)

**Verification Strategy:**
```sql
-- Total count for 2016 backup
SELECT COUNT(*) FROM group11_raw_ios_2016;
```


## Group 12:
**Source:** `'blobs/WhatsApp Chat - Jenny/_chat.txt'`
**Script:** `scripts/parse_group12_whatsapp.py`

**Description:**
WhatsApp text export.

**Result:**
- Inserted xx message records into `group12_whatsapp.sqlite` (Table: `group12_raw_whatsapp`)

**Verification Strategy:**
```sql
-- Count messages
SELECT COUNT(*) FROM group12_raw_whatsapp;
```

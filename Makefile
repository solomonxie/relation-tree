PYTHON = ./venv/bin/python3
PIP = ./venv/bin/pip

.PHONY: init parse lookup lookup-person rotate-keys help

init:
	python3 -m venv venv
	$(PIP) install -r requirements.txt
	mkdir -p data/db data/media
	sqlite3 data/db/database.sqlite < data/schema/schema.sql

parse:
	$(PYTHON) scripts/parse_old_rldt.py

# Usage: make lookup target="王伟洁"
lookup-media:
	@$(PYTHON) scripts/lookup_media.py $(target)

# Usage: make lookup-person target="王伟洁"
lookup-person:
	@$(PYTHON) scripts/lookup_person.py $(target)

# Usage: make rotate-keys
rotate-keys:
	@$(PYTHON) scripts/rotate_encryption.py

# Help target to show usage
help:
	@echo "Usage:"
	@echo "  make lookup target=<search_term>        - Search and decrypt media"
	@echo "  make lookup-person target=<search_term> - Search and display person info"
	@echo "  make rotate-keys                        - Change encryption key and re-encrypt files"

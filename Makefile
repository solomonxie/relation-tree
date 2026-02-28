PYTHON = ./venv/bin/python3
PIP = ./venv/bin/pip

.PHONY: init parse lookup lookup-person

init:
	python3 -m venv venv
	$(PIP) install -r requirements.txt
	mkdir -p data/db/raw data/media
	sqlite3 data/db/database.sqlite < data/schema/persons/schema.sql

parse:
	$(PYTHON) scripts/parse_old_rldt.py

# Usage: make lookup-media 123
lookup-media:
	@$(PYTHON) scripts/lookup_media.py $(filter-out $@,$(MAKECMDGOALS))

# Usage: make lookup-person 123
lookup-person:
	@$(PYTHON) scripts/lookup_person.py $(filter-out $@,$(MAKECMDGOALS))


# Usage: make rotate-keys
rotate-keys:
	@$(PYTHON) scripts/rotate_encryption.py


browse-wechat: ## Browse WeChat data interactively
	@$(PYTHON) scripts/browse_wechat.py
	@$(PYTHON) src/main.py

# Help target to show usage
help:
	@echo "Usage:"
	@echo "  make lookup target=<search_term>        - Search and decrypt media"
	@echo "  make lookup-person target=<search_term> - Search and display person info"

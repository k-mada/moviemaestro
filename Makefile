# Local dev shortcuts. `make run` needs no venv activation — targets call the
# venv's binaries directly. Run `make help` for the list.
VENV := venv
PY   := $(VENV)/bin/python
PIP  := $(VENV)/bin/pip
UVICORN := $(VENV)/bin/uvicorn
USER ?= dave

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

$(PY):
	python3.13 -m venv $(VENV)

.PHONY: setup
setup: $(PY) ## Create venv, install deps, and scaffold .env
	$(PIP) install -e ".[dev]"
	@test -f .env || (cp .env.example .env && echo "Created .env — fill in real values")

.PHONY: run
run: ## Start the API with autoreload on http://127.0.0.1:8000
	$(UVICORN) app.main:app --reload

.PHONY: test
test: ## Run the test suite
	$(PY) -m pytest

.PHONY: health
health: ## Ping the local healthcheck
	curl -s localhost:8000/healthz

.PHONY: refresh
refresh: ## Hit POST /refresh-user (override user: make refresh USER=leo604)
	@set -a && . ./.env && set +a && \
	curl -s -X POST localhost:8000/refresh-user \
		-H "Authorization: Bearer $$WORKER_SHARED_SECRET" \
		-H "Content-Type: application/json" \
		-d '{"lbusername": "$(USER)"}'
	@echo

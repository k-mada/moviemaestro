# Local dev shortcuts. uv manages .venv and deps automatically — `uv run`
# syncs before running, so `make run`/`make test` need no separate setup step.
.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-8s\033[0m %s\n", $$1, $$2}'

.PHONY: sync
sync: ## Install/update deps into .venv from uv.lock
	uv sync

.PHONY: run
run: ## Start the API with autoreload on http://127.0.0.1:8000
	uv run uvicorn app.main:app --reload

.PHONY: test
test: ## Run the test suite
	uv run pytest

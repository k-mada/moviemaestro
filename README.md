# moviemaestro

FastAPI service that orchestrates Letterboxd film data scraping for the [bpdiscord](https://github.com/k-mada/bpdiscord) Hater Rankings refresh pipeline.

Pulls user film ratings via [letterboxdpy](https://github.com/nmcassa/letterboxdpy) and writes results to Supabase. Runs as a long-lived service on Railway, triggered by an admin endpoint in bpdiscord.

## Endpoints

- `GET /healthz` — liveness probe
- `POST /start` — kicks off a refresh job. Bearer auth via `WORKER_SHARED_SECRET`. Body `{ "job_id": "<uuid>" }`. Returns 202.

## Local development

Uses [uv](https://docs.astral.sh/uv/) for env + dependency management (`brew install uv`).

```bash
cp .env.example .env      # fill in real values (first time only)
uv run uvicorn app.main:app --reload   # syncs deps into .venv, then serves
```

Serves on http://127.0.0.1:8000 (interactive docs at `/docs`). `uv run` creates
`.venv` and installs from `uv.lock` on first use, so there's no separate setup
step. Shortcuts: `make run`, `make test`.

## Tests

```bash
uv run pytest   # or: make test
```

## Deployment

Railway pulls from `main` and uses [`railway.json`](./railway.json). Required env vars on the service: `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, `WORKER_SHARED_SECRET`.

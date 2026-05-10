# moviemaestro

FastAPI service that orchestrates Letterboxd film data scraping for the [bpdiscord](https://github.com/k-mada/bpdiscord) Hater Rankings refresh pipeline.

Pulls user film ratings via [letterboxdpy](https://github.com/nmcassa/letterboxdpy) and writes results to Supabase. Runs as a long-lived service on Railway, triggered by an admin endpoint in bpdiscord.

## Endpoints

- `GET /healthz` — liveness probe
- `POST /start` — kicks off a refresh job. Bearer auth via `WORKER_SHARED_SECRET`. Body `{ "job_id": "<uuid>" }`. Returns 202.

## Local development

```bash
python3.13 -m venv venv
source venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env  # fill in real values
uvicorn app.main:app --reload
```

## Tests

```bash
pytest
```

## Deployment

Railway pulls from `main` and uses [`railway.json`](./railway.json). Required env vars on the service: `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, `WORKER_SHARED_SECRET`.

# moviemaestro

Service that orchestrates Letterboxd film data scraping for the [bpdiscord](https://github.com/k-mada/bpdiscord) Hater Rankings refresh pipeline.

Pulls user film ratings via [letterboxdpy](https://github.com/nmcassa/letterboxdpy) and writes results to Supabase. Designed to run as a long-lived FastAPI service on Railway, triggered by an admin endpoint in bpdiscord.

## Local development

```bash
python3.13 -m venv venv
source venv/bin/activate
pip install -e .
```

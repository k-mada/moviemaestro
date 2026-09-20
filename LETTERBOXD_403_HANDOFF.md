# Letterboxd 403 / AccessDenied — investigation handoff

Context dump from a debugging session driven from the `bpdiscord` repo. Goal:
figure out why a `/fetcher` user-scrape intermittently fails with
`AccessDeniedError (403)` and only succeeds after retries / a delay / a
different user / a different host. Continue the work from inside **moviemaestro**.

## TL;DR

The 403 is **not a code bug and not user-specific**. It's a **transient,
time-varying IP block** applied by Letterboxd's Cloudflare bot management to
the egress IP. The same code, user, and DB:

| Source | Result |
|---|---|
| Railway egress IP @ 06:04 (original report) | ❌ `403 AccessDeniedError` |
| Local residential IP (this session) | ✅ 557 films, instant, `errors: []` |
| Railway egress IP (this session, 10 runs) | ✅ 10/10 clean |

The block comes and goes. "Run it enough times / wait / try later" works
because you're waiting out the block or winning a probabilistic Cloudflare
roll. A single green run does **not** prove it's fixed — you have to sample
*during* a bad window.

The user-visible symptom: when blocked, `_phase_user_scrape` records the 403
in the job row's `errors[]` and the job still finishes `completed` with **0
films**. So "it didn't work the first time" = a transient 403 surfaced as a
job error instead of being retried.

## Original error (for reference)

```
[06:04:42] phase → user_scrape
[06:05:04] ERROR user_scrape/leo604: AccessDeniedError: {
  "code": 403, "url": "https://letterboxd.com/leo604",
  "message": "IP or VPN Blocked: Letterboxd is blocking this request..." }
[06:05:05] user_scrape complete: 0 films across 1 users
```
Note the ~21s gap before the error — letterboxdpy burning internal
retries/timeouts before giving up. That message string is letterboxdpy's
*interpretation* of a raw Cloudflare 403, not text Letterboxd literally
returned.

## Why a throwaway job_id can't reproduce it

`/scrape-user` returns `202` and spawns the orchestrator as a background task.
The orchestrator polls the job row's `status` between items to detect
cancellation. `job_state.py:50-62` (`is_cancelled`): **if the row is missing,
it returns `True` ("row vanished — treat as cancelled")**, so the orchestrator
exits *before* hitting Letterboxd. A throwaway UUID therefore exits clean with
no scrape. **You must insert a real `running` row and pass its id.**

## Reproduction / measurement harness

Run from the `moviemaestro` repo root (needs `.env` with `SUPABASE_URL`,
`SUPABASE_SERVICE_ROLE_KEY`, `WORKER_SHARED_SECRET` — see Env below). Reads the
result straight back from the job row, so no need to scrape terminal logs.

Swap `WORKER` between local and Railway to A/B the egress IP:
- Local worker: `http://127.0.0.1:8000`
- Railway (live): `https://moviemaestro-production.up.railway.app`

```bash
set -a && . ./.env && set +a
WORKER="http://127.0.0.1:8000"            # or the Railway URL
TESTER="00000000-0000-0000-0000-000000000000"   # sentinel started_by, easy cleanup
LBU="leo604"
SB=(-H "apikey: $SUPABASE_SERVICE_ROLE_KEY" -H "Authorization: Bearer $SUPABASE_SERVICE_ROLE_KEY")

PASS=0; FAIL=0; OTHER=0
for run in $(seq 1 10); do
  JOB=$(uuidgen | tr 'A-Z' 'a-z')
  IC=$(curl -sS -X POST "$SUPABASE_URL/rest/v1/user_scrape_jobs" "${SB[@]}" \
        -H "Content-Type: application/json" -H "Prefer: return=minimal" \
        -d "{\"id\":\"$JOB\",\"lbusername\":\"$LBU\",\"status\":\"running\",\"started_by\":\"$TESTER\"}" \
        -o /dev/null -w "%{http_code}")
  [ "$IC" != "201" ] && { echo "run $run: insert HTTP $IC (skip)"; OTHER=$((OTHER+1)); continue; }
  curl -sS -m 12 -X POST "$WORKER/scrape-user" \
        -H "Authorization: Bearer $WORKER_SHARED_SECRET" -H "Content-Type: application/json" \
        -d "{\"job_id\":\"$JOB\",\"lbusername\":\"$LBU\"}" -o /dev/null
  RESULT="timeout"
  for i in $(seq 1 20); do
    sleep 3
    ROW=$(curl -sS "$SUPABASE_URL/rest/v1/user_scrape_jobs?id=eq.$JOB&select=status,errors,log_tail" "${SB[@]}")
    RESULT=$(printf '%s' "$ROW" | python3 -c '
import sys,json
d=json.load(sys.stdin)
if not d: print("MISSING"); sys.exit()
r=d[0]; s=r["status"]
if s not in ("completed","failed","cancelled"): print("running"); sys.exit()
blob=(json.dumps(r.get("errors"))+(r.get("log_tail") or "")).lower()
print(("BLOCKED:"+s) if (("accessdenied" in blob) or ("403" in blob) or ("blocked" in blob)) else s.upper())')
    case "$RESULT" in running) continue;; *) break;; esac
  done
  # never leave a stuck running row if the trigger failed
  curl -sS -X PATCH "$SUPABASE_URL/rest/v1/user_scrape_jobs?id=eq.$JOB&status=eq.running" "${SB[@]}" \
       -H "Content-Type: application/json" -d '{"status":"failed"}' -o /dev/null
  case "$RESULT" in
    COMPLETED) PASS=$((PASS+1)); echo "run $run: ✅ COMPLETED";;
    BLOCKED*)  FAIL=$((FAIL+1)); echo "run $run: ❌ $RESULT";;
    *)         OTHER=$((OTHER+1)); echo "run $run: ⚠️  $RESULT";;
  esac
done
echo "PASS=$PASS FAIL(blocked)=$FAIL OTHER=$OTHER of 10"
# cleanup all sentinel test rows
curl -sS -X DELETE "$SUPABASE_URL/rest/v1/user_scrape_jobs?started_by=eq.$TESTER" "${SB[@]}" -o /dev/null
```

**Caveat:** this writes to whatever Supabase `.env` points at. The `.env`
created this session points at **prod** (`bvadmlitqvahdatjtpgz`). A successful
scrape upserts real `UserFilms`/`Films`; a blocked run writes ~nothing. To
catch the failure, `/loop` this over hours — it samples the rate and will log
`FAIL` when a bad window hits.

## Code map (moviemaestro)

- `app/main.py`
  - `require_worker_secret` (`:34`) — Bearer auth; **constructs `Settings()`**,
    so a missing env var 500s here *before* the token check (looks like auth
    works but isn't — see Env gotcha).
  - `scrape_user` (`:162`) → `_spawn` (`:87`) → `asyncio.create_task(orchestrator.run(...))`, returns 202.
- `app/settings.py` — `Settings` requires `supabase_url`,
  `supabase_service_role_key`, `worker_shared_secret`. `env_file=".env"` is
  **relative to CWD** → run uvicorn from repo root or it won't find `.env`.
- `app/db.py` — `get_supabase()` is `@lru_cache`d.
- `app/pipeline/orchestrator.py` — `_phase_user_scrape` (`:45`): per-user mode
  scrapes `[lbusername]`; wraps the call in
  `letterboxd_throttle.call(scrape_and_upsert_user_films, ...)`; **catches all
  exceptions per-user, appends to `errors[]`, continues**. This is where the
  403 gets swallowed into the job row.
- `app/pipeline/job_state.py:50-62` — `is_cancelled()`; missing row ⇒ `True`.
- `app/pipeline/letterboxd_throttle.py` — process-wide `asyncio.Semaphore(1)`
  serializing every letterboxdpy call. Docstring notes letterboxdpy already
  uses a **curl_cffi** singleton session (so browser TLS impersonation is
  already in place — JA3 fingerprinting is NOT the missing piece). Docstring
  also names the planned hardening under issue **bpdiscord-yao**: min gap
  between requests, **circuit breaker on AccessDeniedError / HTTP 429**,
  UI-visible cooldown error.
- `app/pipeline/user_films.py:11` — `scrape_and_upsert_user_films` →
  `letterboxdpy.user.User(lbusername).get_films()`. The 403 originates inside
  letterboxdpy here; `AccessDeniedError` is from
  `letterboxdpy.core.exceptions`.

## Recommended fixes (ranked) — all live HERE in moviemaestro

1. **Retry-with-backoff on `AccessDeniedError`/429** — highest leverage,
   because the block is transient. Wrap the scrape (in
   `letterboxd_throttle.call`, or around `get_films()` in `user_films.py`) in a
   few exponential-backoff retries (e.g. 3 tries, 2s/8s/30s jitter). Most
   first-time failures would self-heal before reaching `errors[]`. This is the
   core of **bpdiscord-yao** — check that issue before starting.
2. **Circuit breaker / cooldown** — after N consecutive `AccessDeniedError`,
   pause the whole process for a cooldown instead of hammering a flagged IP
   (which likely *prolongs* the block). Also bpdiscord-yao.
3. **Egress IP** — the root variable. Options: a static/dedicated egress IP for
   the Railway service, or routing letterboxdpy through a residential/rotating
   proxy. Heaviest lift / $$, but it's the actual cause. Do this only if 1–2
   don't get the failure rate low enough.
4. **User-Agent** — letterboxdpy controls this via curl_cffi impersonation;
   verify what it sends before assuming it's a lever. Likely already fine.

Do NOT bother with: adding TLS impersonation (already there), blaming the code
path, or per-user logic (the block is IP-global, not user-specific).

## Env setup for local moviemaestro

`.env` (gitignored) at repo root, loaded by `Settings(env_file=".env")` when
uvicorn runs from root:
```
SUPABASE_URL=...
SUPABASE_SERVICE_ROLE_KEY=...
WORKER_SHARED_SECRET=...
```
Created this session by copying the three keys from
`bpdiscord/src/server/.env` (→ **prod** Supabase). Start: `uvicorn app.main:app --reload`.

**Gotcha:** with no `.env` and no exported vars, every protected endpoint 500s
with a pydantic `ValidationError` (3 missing fields) raised inside the auth
dependency — looks like a server bug, is actually missing config.

## Loose end for the bpdiscord repo (not moviemaestro)

`bpdiscord/CLAUDE.md` documents `WORKER_URL=https://moviemaestro.up.railway.app`
— that bare host 404s ("Application not found"). The live worker (per
`src/server/.env`) is `https://moviemaestro-production.up.railway.app`. Fix the
doc drift in a bpdiscord PR.

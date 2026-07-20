# Letterboxd data flow — current vs. proposed

Decision aid. Shows how each data domain is pulled, which egress it needs,
which endpoint drives it, and how it maps to the existing Supabase tables.
No code changes implied by this doc — it's the map, not the move.

## The core constraint

Letterboxd routes split into two classes by how Cloudflare treats them:

| Route class | Example | Datacenter IP (Railway) | Residential IP (local) |
|---|---|---|---|
| **HTML pages** | `/{user}/films/`, `/film/{slug}/` | ❌ hard-blocked (permanent) | ✅ works |
| **RSS feed** | `/{user}/rss/` | ✅ works | ✅ works |

Everything below follows from this one table. The grid scrape and the film-page
scrape are HTML → must run from **residential** egress. RSS → runs anywhere,
so it stays on **Railway**.

---

## Current architecture (as-is) — why it breaks

One Railway process, three sequential phases, **all three on HTML routes**:

```
                        ┌─────────────────────── RAILWAY (datacenter egress) ───────────────────────┐
  POST /start ─────────▶│                                                                            │
  (all is_discord users)│   orchestrator.run()                                                       │
                        │                                                                            │
  POST /scrape-user ───▶│   phase 1: user_scrape    letterboxdpy User().get_films()                 │
  (one user)            │        │                  GET letterboxd.com/{user}/films/  ❌ BLOCKED     │
                        │        ▼                                                                   │
                        │   phase 2: missing_films  get_missing_films RPC  (Supabase only, fine)     │
                        │        │                                                                   │
                        │        ▼                                                                   │
                        │   phase 3: film_ratings   letterboxdpy Movie(slug)                         │
                        │                           GET letterboxd.com/film/{slug}/  ❌ BLOCKED      │
                        └────────────────────────────────────────────────────────────────────────┬─┘
                                                                                                   │ upsert
                                                                                                   ▼
                                                              Supabase: UserFilms, UserRatings, Films
```

Phases 1 and 3 are the two HTML scrapes. On Railway both now 403 permanently.
`letterboxd_throttle` retries/backs-off, but the block is IP-class-based, not
transient — retries can't win. **The architecture assumes one egress; the fix
is to stop assuming that.**

---

## Proposed architecture — split by egress class

Same three phases, re-homed by what egress they require. RSS is added as a new,
cheap freshness path that Railway *can* serve.

```
        ┌──────────────── RAILWAY (datacenter egress — RSS-safe, HTML-blocked) ────────────────┐
        │                                                                                       │
 POST   │   ┌── Domain B: USER HISTORY — INCREMENTAL ──────────────────────────────────────┐   │
 /refresh-user │   GET letterboxd.com/{user}/rss/   ✅ (last 50 diary events)               │   │
        │   │   parse RSS  ──▶  upsert UserFilms  +  refresh UserRatings histogram          │   │
        │   └───────────────────────────────────────────────────────────────────────────────┘  │
        │                                                                                       │
        │   writes/reads job queue  ◀──────────────────────────────────────────┐               │
        └───────────────────────────────────────────────────────────────────────┼──────────────┘
                                                                                 │ (pull model:
                                          Supabase                               │  no inbound to home)
                    ┌───────────────────────────────────────────────┐           │
                    │  UserFilms   UserRatings   Films               │◀──────────┼──────────┐
                    │  + job queue table (backfill / film-agg tasks) │           │          │
                    └───────────────────────────────────────────────┘           │          │ upsert
                                                                                 │          │
        ┌──────────────── LOCAL WORKER (residential egress — HTML routes work) ──┼──────────┼──┐
        │                                                                        │          │  │
        │   polls queue ────────────────────────────────────────────────────────┘          │  │
        │                                                                                   │  │
        │   ┌── Domain A: USER HISTORY — FULL BACKFILL ────────────────────────────────────┐│  │
        │   │   letterboxdpy User().get_films()                                            ││  │
        │   │   GET letterboxd.com/{user}/films/   ✅ (full history, all rated films)       ││  │
        │   │   ──▶  upsert UserFilms  +  refresh UserRatings histogram  ───────────────────┼┼──┘
        │   └──────────────────────────────────────────────────────────────────────────────┘│
        │                                                                                    │
        │   ┌── Domain C: FILM AGGREGATES ────────────────────────────────────────────────┐ │
        │   │   letterboxdpy Movie(slug)                                                   │ │
        │   │   GET letterboxd.com/film/{slug}/   ✅ (ld+json aggregateRating)             │ │
        │   │   ──▶  upsert Films (+ new rating_count / review_count) ──────────────────────┼─┘
        │   └───────────────────────────────────────────────────────────────────────────────┘
        └───────────────────────────────────────────────────────────────────────────────────┘
```

Split rule: **datacenter does the unblocked high-frequency work (RSS); home does
the blocked low-frequency work (full grid + film pages).** The local worker uses
a *pull* model — it polls a Supabase queue and writes results back — so your home
box never accepts inbound connections and its IP is never exposed.

---

## Three domains at a glance

| # | Domain | Source route | Endpoint / trigger | Egress | Cadence | Completeness |
|---|---|---|---|---|---|---|
| **A** | User history — backfill | `/{user}/films/` (HTML grid) | local worker, from queue | residential | once per new user + periodic reconcile | **full history** |
| **B** | User history — refresh | `/{user}/rss/` (RSS) | `POST /refresh-user` on Railway (or cron) | datacenter | frequent (hourly/daily) | last 50 diary events |
| **C** | Film aggregates | `/film/{slug}/` (HTML, ld+json) | local worker, from queue | residential | slow; only missing/stale slugs | full |

Why the split works: **A** establishes truth, **B** keeps it fresh cheaply, **C**
is shared across all users and barely changes, so it's a slow trickle. A + C are
the only things that *need* your home IP.

---

## Field → column mapping

### Domain A — grid `User().get_films()` → `UserFilms` + `UserRatings`

`get_films()` returns per film: `slug, name, year, url, id, rating, liked`
(verified from a live pull).

| Source field | → Table.column | Notes |
|---|---|---|
| `slug` | `UserFilms.film_slug` | PK part |
| `name` | `UserFilms.title` | |
| `rating` | `UserFilms.rating` | 0.5–5.0 |
| `liked` | `UserFilms.liked` | |
| (`lbusername` arg) | `UserFilms.lbusername` | PK part |
| derived from `rating` | `UserRatings.(username, rating, count)` | histogram, computed in-code |
| `year`, `url`, `id` | *(not persisted today)* | grid-only extras; `id` = Letterboxd film id |

### Domain B — RSS `/{user}/rss/` → `UserFilms` + `UserRatings`

Each `<item>` carries: `filmTitle, filmYear, memberRating, memberLike, rewatch,
watchedDate, tmdb:movieId`, plus slug parsable from `<link>`.

| RSS field | → Table.column | Notes |
|---|---|---|
| slug (from `<link>`) | `UserFilms.film_slug` | ⚠️ strip trailing `/N/` on rewatch links |
| `letterboxd:filmTitle` | `UserFilms.title` | |
| `letterboxd:memberRating` | `UserFilms.rating` | empty when logged-but-unrated |
| `letterboxd:memberLike` (Yes/No) | `UserFilms.liked` | |
| derived from rating | `UserRatings.(username, rating, count)` | same histogram refresh as A |
| `letterboxd:watchedDate` | *(new col, optional)* `UserFilms.watched_date` | not in grid |
| `letterboxd:rewatch` | *(new col, optional)* `UserFilms.rewatch` | not in grid |
| `tmdb:movieId` | *(new col, optional)* `Films.tmdb_id` | free film identity; grid lacks it |

Parser must also **filter `<item>` guid to `letterboxd-watch-`** (drop list/review items).

### Domain C — film page `Movie(slug)` ld+json → `Films`

Already implemented in `film_ratings.py` via `upsert_film` RPC; the two aggregate
counts are the only additions.

| Source field | → Films.column | Status |
|---|---|---|
| `movie.rating` (ld+json `ratingValue`) | `lb_rating` | ✅ today |
| `movie.title` | `title` | ✅ today |
| `movie.url` | `url` | ✅ today |
| `movie.year` | `release_year` | ✅ today |
| `tmdb_link` / `poster` / `banner` | `tmdb_link` / `poster` / `banner` | ✅ today |
| ld+json `ratingCount` (156,556 in test) | *(new col)* `rating_count` | ➕ add |
| ld+json `reviewCount` (24,720 in test) | *(new col)* `review_count` | ➕ add |

Do **not** use `/csi/film/{slug}/rating-histogram/` — it 403s even from
residential. The full film page ld+json is the reliable source.

---

## What moving forward would require (not decisions, just the surface area)

1. **New local worker deployment** — same FastAPI app or a thin variant, run from
   a residential connection (home box / always-on Mac), pulling a Supabase job
   queue. Owns Domains A + C.
2. **New Railway endpoint** `POST /refresh-user` — RSS fetch + parse → UserFilms
   upsert. Owns Domain B. (Or a scheduled cron over known users.)
3. **Queue table** in Supabase — backfill + film-aggregate tasks the local worker
   claims. Replaces "Railway runs the HTML phases directly."
4. **Optional new columns** — `UserFilms.watched_date`, `UserFilms.rewatch`,
   `Films.tmdb_id`, `Films.rating_count`, `Films.review_count`. None are required
   to preserve current behavior; they capture the extras each source now exposes.
5. **Retire the assumption** in `orchestrator.py` that phases 1 & 3 can run on
   Railway. They move to the local worker; Railway keeps phase 2 (pure DB) and
   the new RSS phase.
```

# Worker-side dedupe of duplicate Films by tmdb_link

Date: 2026-10-17
Status: design — revised after review (atomic write+merge)

## Problem

When Letterboxd re-slugs a film (title, release date, or TMDB linkage firms up),
the worker mints a duplicate instead of converging:

1. `user_scrape` upserts `UserFilms` on `(lbusername, film_slug)`. The feed now
   carries the **new** slug, so a new row appears; the old-slug row is orphaned
   (upsert never deletes).
2. `missing_films` returns the new slug (no `Films` row yet).
3. `film_ratings` scrapes it and upserts a `Films` row carrying the **same**
   `tmdb_link` as the old one.

Result: one physical film under two slugs, which every slug-keyed bpdiscord
feature (movie-swap, film pages, stats, 40-club) double-counts. A one-time
backfill (`20261017143344_dedupe_films_by_tmdb.sql`, bpdiscord) cleared the 119
existing groups, but the worker keeps minting new ones — one appeared within a
day of that cleanup.

## Goal

Stop duplicates at the source: whenever the worker scrapes a film, fold any
other `Films` row sharing its `tmdb_link` into the just-scraped slug. A TMDB id
identifies exactly one film, so it is the authoritative key — and it catches
foreign-title and alternate-release variants a title match would miss.

Non-goals: the read path (slug stays the universal key), a scheduled sweep, and
detecting upstream Letterboxd mislinks (see Known limitations).

## Canonical choice

The just-scraped slug (`movie.slug`) is the **live** slug: letterboxdpy follows
Letterboxd's redirect, so it is whatever Letterboxd serves now. That beats the
backfill's `created_at` heuristic, so the worker canonicalizes on `movie.slug`
and merges every other same-`tmdb_link` slug into it.

## Typical path (traced)

A re-slug of film X, users already holding the old slug:

1. `user_scrape` — the feed carries X's new slug, so `UserFilms` gets a new row
   and the old-slug row is orphaned. The phase then recomputes each user's
   `UserRatings` histogram from `UserFilms`, now counting **both** rows —
   overcounting by one.
2. `missing_films` — returns the new slug (no `Films` row); the old slug has one.
3. `film_ratings` — scrapes the new slug. One RPC writes its `Films` row and
   folds the old slug in, atomically.

The merge fixes the duplicate in the same run. It also recomputes `UserRatings`
for affected owners (step 1's histogram is now stale — the merge deleted a
`UserFilms` row step 1 had counted), so the overcount never outlives the
transaction.

**Scope boundary.** The merge only fires for slugs passing through
`film_ratings` (slugs newly missing a `Films` row). A duplicate where *both*
slugs already have `Films` rows re-enters no phase. Post-fix that cannot form
(every `Films` row now goes through the write+merge RPC), so it only concerned
the pre-fix backlog the backfill already drained. Migration and worker are
complementary: one drains the backlog, the other stops new ones.

**Why one atomic RPC.** An earlier draft wrote the `Films` row, then called a
separate merge RPC. A transient merge failure then leaks a duplicate forever:
the `Films` row is already committed, so the slug leaves `missing_films`, never
re-enters `film_ratings`, and no sweep exists to catch it. One transaction fixes
this — a merge failure rolls the write back, the slug stays missing, next run
retries.

## Design

One Postgres RPC writes the scraped film **and** merges its duplicates in a
single transaction, mirroring the existing `upsert_film`. The worker calls it
once per scraped film in place of `upsert_film`. Two repos, two PRs.

### PR 1 — bpdiscord: the RPC migration

New `supabase/migrations/<ts>_add_upsert_film_and_merge_function.sql`. Keeps
`upsert_film` (still used by `tombstone_film`), adds the combined function, and
indexes `Films.tmdb_link` — the merge filters on it once per scraped film, which
without an index is a ~23k-row seq scan per call:

```sql
CREATE INDEX IF NOT EXISTS "idx_films_tmdb_link" ON "Films" ("tmdb_link");
```

```sql
CREATE OR REPLACE FUNCTION "public"."upsert_film_and_merge_duplicates"(
    "p_film_slug"    "text",
    "p_title"        "text",
    "p_lb_rating"    real,
    "p_url"          "text",
    "p_tmdb_link"    "text",
    "p_poster"       "text",
    "p_banner"       "text",
    "p_release_year" integer
)
RETURNS integer
    LANGUAGE "plpgsql"
    AS $$
DECLARE
    v_tmdb   text;
    v_users  text[];
    v_merged integer;
BEGIN
    -- Write + merge in ONE transaction: a merge failure below rolls this upsert
    -- back, so no Films row is ever committed without its merge. (A committed
    -- row leaves missing_films and would never be retried — the duplicate leak.)
    PERFORM "upsert_film"(
        p_film_slug, p_title, p_lb_rating, p_url, p_tmdb_link,
        p_poster, p_banner, p_release_year
    );

    -- Read the STORED key, not p_tmdb_link: upsert_film COALESCEs, so a NULL arg
    -- keeps the row's existing tmdb_link and the merge must key on that.
    SELECT tmdb_link INTO v_tmdb FROM "Films" WHERE film_slug = p_film_slug;

    -- Only a real, non-zero TMDB id is an identity; /tv/0 etc. group unrelated
    -- films — never merge on them.
    IF v_tmdb IS NULL OR v_tmdb !~ '/(movie|tv)/[1-9][0-9]*/?$' THEN
        RETURN 0;
    END IF;

    -- Hot-path short-circuit: no other row shares this tmdb_link, so there is
    -- nothing to merge. The authoritative test — a loser Films row with no
    -- UserFilms owner must still be caught, so we cannot gate on v_users.
    IF NOT EXISTS (
        SELECT 1 FROM "Films"
        WHERE tmdb_link = v_tmdb AND film_slug <> p_film_slug
    ) THEN
        RETURN 0;
    END IF;

    -- Owners of a loser row, captured BEFORE mutating so their histogram can be
    -- recomputed after. NULL if no user holds a loser slug (deletes still run).
    SELECT array_agg(DISTINCT uf.lbusername) INTO v_users
    FROM "UserFilms" uf
    JOIN "Films" lf ON lf.film_slug = uf.film_slug
    WHERE lf.tmdb_link = v_tmdb AND lf.film_slug <> p_film_slug;

    -- Promote one loser row per owner who lacks the canonical row, so they keep
    -- the film; their remaining loser rows are deleted below.
    UPDATE "UserFilms" uf
    SET film_slug = p_film_slug
    FROM (
        SELECT DISTINCT ON (lu.lbusername) lu.lbusername, lu.film_slug AS loser_slug
        FROM "UserFilms" lu
        JOIN "Films" lf ON lf.film_slug = lu.film_slug
        WHERE lf.tmdb_link = v_tmdb
          AND lf.film_slug <> p_film_slug
          AND NOT EXISTS (
              SELECT 1 FROM "UserFilms" c
              WHERE c.lbusername = lu.lbusername AND c.film_slug = p_film_slug)
        ORDER BY lu.lbusername,
                 (lu.rating IS NOT NULL AND lu.rating > 0) DESC,
                 lu.rating DESC NULLS LAST,
                 lu.updated_at DESC NULLS LAST,
                 lu.film_slug ASC
    ) p
    WHERE uf.lbusername = p.lbusername AND uf.film_slug = p.loser_slug;

    DELETE FROM "UserFilms" uf USING "Films" lf
     WHERE uf.film_slug = lf.film_slug
       AND lf.tmdb_link = v_tmdb AND lf.film_slug <> p_film_slug;

    DELETE FROM "FilmRatings" fr USING "Films" lf
     WHERE fr.film_slug = lf.film_slug
       AND lf.tmdb_link = v_tmdb AND lf.film_slug <> p_film_slug;

    -- Loser Films deleted LAST — the deletes above join Films to find losers.
    DELETE FROM "Films"
     WHERE tmdb_link = v_tmdb AND film_slug <> p_film_slug;
    GET DIAGNOSTICS v_merged = ROW_COUNT;

    IF v_users IS NOT NULL THEN
        PERFORM refresh_user_ratings(u) FROM unnest(v_users) AS u;
    END IF;

    RETURN v_merged;
END;
$$;

ALTER FUNCTION "public"."upsert_film_and_merge_duplicates"(
    "text", "text", real, "text", "text", "text", "text", integer
) OWNER TO "postgres";
```

Notes:
- **Hot path.** Runs once per scraped film and almost always finds no duplicate,
  so the `NOT EXISTS` probe short-circuits before any mutation — a no-dup call is
  three index/PK touches (upsert, `v_tmdb` read, probe). Only a real duplicate
  pays for promote/delete/refresh.
- Reuses `upsert_film` (via `PERFORM`) and `refresh_user_ratings(text)` rather
  than re-implementing them, so the paths cannot drift. The histogram recompute
  lives inside the txn so a reader never sees the pre-merge overcount.
- The merge re-resolves the loser set in the `UPDATE` and each `DELETE` (four
  scans of one small group). Left as-is — merges are rare and groups tiny, so a
  temp table would cost more than it saves. Matches the backfill's structure.
- Over-refreshes slightly: pure-promote owners get a no-op recompute. Narrowing
  to delete-victims only isn't worth the logic at these group sizes.
- No `SECURITY DEFINER` — the worker uses the service-role key, like `upsert_film`.
- Idempotent: no duplicate → returns 0; null/placeholder `tmdb_link` → upserts,
  returns 0. Concurrent scrapers of one `tmdb_link` serialize on row locks; the
  loser the second sees is bounded, no advisory lock needed.

### PR 2 — moviemaestro: call the combined RPC

In `app/pipeline/film_ratings.py`, `scrape_and_upsert_film` swaps its
`upsert_film` call for the combined RPC (same params, new name):

```python
supabase.rpc("upsert_film_and_merge_duplicates", {
    "p_film_slug":    movie.slug,
    "p_title":        movie.title,
    "p_lb_rating":    movie.rating,
    "p_url":          movie.url,
    "p_tmdb_link":    getattr(movie, "tmdb_link", None),
    "p_poster":       getattr(movie, "poster", None),
    "p_banner":       getattr(movie, "banner", None),
    "p_release_year": getattr(movie, "year", None),
}).execute()
```

- One unconditional call; the RPC decides server-side whether to merge, so no
  `if tmdb_link` guard.
- `tombstone_film` keeps calling `upsert_film` directly (no `tmdb_link`, nothing
  to merge), so keep `_upsert_film_rpc`.
- No orchestrator changes. An RPC failure raises, is recorded per-item, and —
  write rolled back with the merge — the slug stays missing and retries next run.

## Testing (moviemaestro)

Add an `upsert_film_and_merge_duplicates` stub to `tests/fakes.py` `FakeSupabase`
that runs `_upsert_film_rpc` then emulates the merge over the in-memory tables
(find losers by `tmdb_link`, repoint/delete `UserFilms`, delete loser `Films`,
call `_refresh_user_ratings_rpc` for affected owners), recording the call. Keep
the `upsert_film` stub for `tombstone_film`.

New `tests/test_film_ratings.py`:
1. **Write + merge.** Seed a stale-slug `Films` row + `UserFilms` row sharing a
   `tmdb_link`; scrape a `FakeMovie` with the new canonical slug and matching
   `tmdb_link`. Assert: canonical written, one `Films` row left, `UserFilms`
   repointed, RPC called with the canonical slug.
2. **Owner holds both slugs → histogram corrected.** One user with stale +
   canonical rows at the same rating, `UserRatings` count 2; scrape canonical.
   Assert the duplicate `UserFilms` row is gone and the count drops to 1.
3. **No tmdb_link → write, no merge.** `FakeMovie.tmdb_link is None`; assert the
   `Films` row is upserted and no loser is touched.
4. **No duplicates → no-op merge.** Single matching film; row survives, returns 0.

Existing `test_orchestrator.py` must stay green — its `FakeMovie`s need a
`tmdb_link` attribute so the new path runs end-to-end.

## Rollout order

PR 1 before PR 2, or the worker calls a missing function:
1. Merge PR 1; approve the bpdiscord migrations workflow so it exists in prod.
2. Merge PR 2; redeploy the worker on Railway.

No "function may not exist" catch — deploy order controls the window. If PR 2
lands first, the RPC errors before any write (the old `upsert_film` call is gone
from this path), so the slug is untouched, stays in `missing_films`, and retries
cleanly once PR 1 lands. Loud, and no partial state.

## Known limitations

- **Upstream mislink.** The worker trusts the TMDB id. If Letterboxd mislinks two
  different films to one id (the `1084409` "A Talent for Murder" / "Switzerland"
  case the backfill quarantined), the worker merges them. No clean signal
  separates a mislink from a legitimate foreign-title variant, so this is
  accepted — same as the backfill left it for manual review. Rare, upstream.
- **Null `tmdb_link`.** A row whose `tmdb_link` never parsed is not an identity,
  so it is never merged — and, having a `Films` row, never re-scraped, so it
  stays null. Two such rows for one film would persist as a duplicate.
  Negligible (2 of ~23.6k films); if it grows, a `tmdb` backfill pass (sibling
  to `backfill_years`) is the fix, out of scope here.

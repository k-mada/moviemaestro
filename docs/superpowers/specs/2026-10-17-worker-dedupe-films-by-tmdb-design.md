# Worker-side dedupe of duplicate Films by tmdb_link

Date: 2026-10-17
Status: design — awaiting review

## Problem

When Letterboxd re-slugs a film (its title, release date, or TMDB linkage firms
up), the worker mints a duplicate instead of converging:

1. `user_scrape` upserts `UserFilms` keyed on `(lbusername, film_slug)` from the
   user's RSS feed. After a re-slug the feed carries the **new** slug, so a new
   row appears and the old-slug row is orphaned — upsert never deletes.
2. `missing_films` returns both slugs (neither has a `Films` row yet, or the old
   one does and the new one doesn't).
3. `film_ratings` scrapes each slug and upserts a `Films` row. Both rows carry
   the **same** `tmdb_link`.

Result: one physical film under two slugs. Every slug-keyed feature in bpdiscord
(movie-swap, film pages, stats, the 40-club) double-counts it. A one-time
backfill migration (`20261017143344_dedupe_films_by_tmdb.sql`, bpdiscord)
cleared the existing 119 groups, but the worker keeps producing new ones — a new
duplicate appeared within a day of that cleanup.

## Goal

Stop the worker creating duplicates at the source: whenever it scrapes a film,
fold any other `Films` row sharing that film's `tmdb_link` into the just-scraped
slug. A TMDB id identifies exactly one film, so it is the authoritative key —
it also catches foreign-title and alternate-release variants that a title match
cannot.

Non-goals: changing the read path (slug stays the universal key), a scheduled
sweep, and detecting upstream Letterboxd mislinks (see Known limitations).

## Typical path (traced)

A re-slug of film X, users already holding the old slug:

1. `user_scrape` — the RSS feed now carries X's **new** slug, so `UserFilms`
   gets a new row; the old-slug row is orphaned (upsert never deletes). The
   phase then recomputes each affected user's `UserRatings` histogram from
   `UserFilms` — which now counts **both** rows, overcounting by one.
2. `missing_films` — the new slug has no `Films` row yet, so it is returned; the
   old slug already has one, so it is not.
3. `film_ratings` — the new slug is scraped, its `Films` row is created, and the
   merge RPC fires on the new (live) slug, folding the old slug in.

So the merge **does** trigger on the common case, in the same run. But step 1's
histogram is now stale, because the merge in step 3 deletes the duplicate
`UserFilms` row after step 1 already counted it. The RPC therefore recomputes
`UserRatings` for every affected user as part of the merge (see below), so the
overcount never outlives the transaction.

**Scope boundary.** The merge only fires for slugs that pass through
`film_ratings` — i.e. slugs newly missing a `Films` row. A duplicate where
*both* slugs already have `Films` rows re-enters no phase and is never
re-examined. Post-fix such a duplicate cannot form (every `Films` row is written
by `scrape_and_upsert_film`, which now calls the merge), so this matters only
for the pre-fix backlog — already cleared by the one-time bpdiscord migration
`20261017143344_dedupe_films_by_tmdb.sql`. The worker fix and that migration are
complementary: the migration drains the backlog, the worker stops new ones.

## Canonical choice

The slug the worker just scraped (`movie.slug`) is the **live** Letterboxd slug
— letterboxdpy resolves the requested slug through Letterboxd's redirect, so
`movie.slug` is whatever Letterboxd serves now. That is a stronger canonical
signal than the backfill migration's `created_at` heuristic, so the worker
canonicalizes on `movie.slug` and merges every other same-`tmdb_link` slug into
it.

## Design

A new Postgres RPC owns the merge atomically, mirroring the existing
`upsert_film` RPC pattern; the worker calls it once per scraped film. Two repos,
two PRs.

### PR 1 — bpdiscord: the RPC migration

New `supabase/migrations/<ts>_add_merge_film_duplicates_function.sql`. It also
adds an index on `Films.tmdb_link`, because the worker now calls the merge once
per scraped film and its loser-lookup filters on `tmdb_link` — without the index
that is a ~23k-row seq scan per call, which hurts bulk refreshes:

```sql
CREATE INDEX IF NOT EXISTS "idx_films_tmdb_link" ON "Films" ("tmdb_link");
```

```sql
CREATE OR REPLACE FUNCTION "public"."merge_film_duplicates_by_tmdb"(
    "p_canonical_slug" "text"
)
RETURNS integer
    LANGUAGE "plpgsql"
    AS $$
DECLARE
    v_tmdb   text;
    v_users  text[];
    v_merged integer;
BEGIN
    SELECT tmdb_link INTO v_tmdb FROM "Films" WHERE film_slug = p_canonical_slug;

    -- Only a real, non-zero TMDB movie/tv id is an identity. A null or
    -- placeholder key (e.g. /tv/0) groups unrelated films — never merge on it.
    IF v_tmdb IS NULL OR v_tmdb !~ '/(movie|tv)/[1-9][0-9]*/?$' THEN
        RETURN 0;
    END IF;

    -- Capture every owner of a loser row BEFORE mutating, so their UserRatings
    -- histogram (derived from UserFilms) can be recomputed after the merge.
    -- NULL when no user holds a loser slug; the refresh at the end is then a
    -- no-op, but the loser Films/FilmRatings deletes below still run.
    SELECT array_agg(DISTINCT uf.lbusername) INTO v_users
    FROM "UserFilms" uf
    JOIN "Films" lf ON lf.film_slug = uf.film_slug
    WHERE lf.tmdb_link = v_tmdb AND lf.film_slug <> p_canonical_slug;

    -- Promote one loser row per owner who has no canonical row yet, so the
    -- user keeps the film; the rest of their loser rows are deleted below.
    UPDATE "UserFilms" uf
    SET film_slug = p_canonical_slug
    FROM (
        SELECT DISTINCT ON (lu.lbusername) lu.lbusername, lu.film_slug AS loser_slug
        FROM "UserFilms" lu
        JOIN "Films" lf ON lf.film_slug = lu.film_slug
        WHERE lf.tmdb_link = v_tmdb
          AND lf.film_slug <> p_canonical_slug
          AND NOT EXISTS (
              SELECT 1 FROM "UserFilms" c
              WHERE c.lbusername = lu.lbusername AND c.film_slug = p_canonical_slug)
        ORDER BY lu.lbusername,
                 (lu.rating IS NOT NULL AND lu.rating > 0) DESC,
                 lu.rating DESC NULLS LAST,
                 lu.updated_at DESC NULLS LAST,
                 lu.film_slug ASC
    ) p
    WHERE uf.lbusername = p.lbusername AND uf.film_slug = p.loser_slug;

    DELETE FROM "UserFilms" uf USING "Films" lf
     WHERE uf.film_slug = lf.film_slug
       AND lf.tmdb_link = v_tmdb AND lf.film_slug <> p_canonical_slug;

    DELETE FROM "FilmRatings" fr USING "Films" lf
     WHERE fr.film_slug = lf.film_slug
       AND lf.tmdb_link = v_tmdb AND lf.film_slug <> p_canonical_slug;

    WITH del AS (
        DELETE FROM "Films"
        WHERE tmdb_link = v_tmdb AND film_slug <> p_canonical_slug
        RETURNING 1)
    SELECT count(*) INTO v_merged FROM del;

    -- Recompute the histogram for every affected owner, in this same
    -- transaction, so a reader never sees the pre-merge overcount. A promoted
    -- owner's recompute is a harmless no-op (slug changed, ratings did not).
    IF v_users IS NOT NULL THEN
        PERFORM refresh_user_ratings(u) FROM unnest(v_users) AS u;
    END IF;

    RETURN v_merged;
END;
$$;

ALTER FUNCTION "public"."merge_film_duplicates_by_tmdb"("text") OWNER TO "postgres";
```

Notes:
- `plpgsql` (not `sql`) because the body needs a variable and a validity guard.
- Loser `Films` rows are deleted **last**, since the `UserFilms`/`FilmRatings`
  deletes join `Films` to resolve the loser set.
- Returns the count of merged slugs so the worker can log it.
- Recomputes `UserRatings` for affected owners in the same transaction, because
  the merge deletes duplicate `UserFilms` rows that phase 1's histogram refresh
  already counted. Folding it into the RPC keeps the histogram atomic with the
  merge and avoids N worker round-trips; it reuses the existing
  `refresh_user_ratings(text)` function.
- No `SECURITY DEFINER`: the worker calls with the service-role key, which
  bypasses RLS, exactly like `upsert_film`.
- Idempotent and safe to call every time: no duplicates → empty loser set →
  returns 0.
- Concurrency: two lambdas scraping the same `tmdb_link` serialize on the
  `UPDATE`/`DELETE` row locks; the second finds fewer losers. Bounded cost, no
  correctness impact — consistent with the repo's stance on cross-instance
  races. No advisory lock needed.

### PR 2 — moviemaestro: call the RPC

In `app/pipeline/film_ratings.py`, `scrape_and_upsert_film`, after the existing
`_upsert_film_rpc(...)`:

```python
tmdb_link = getattr(movie, "tmdb_link", None)
# ... existing upsert using movie.slug / tmdb_link ...
if tmdb_link:
    supabase.rpc(
        "merge_film_duplicates_by_tmdb", {"p_canonical_slug": movie.slug}
    ).execute()
```

- Skip the call when `tmdb_link` is absent (nothing to dedupe; saves a
  round-trip). The RPC also guards server-side, so this is an optimization.
- `tombstone_film` is unchanged — a tombstone has no `tmdb_link`.
- No orchestrator changes: the call rides inside the existing per-film step and
  is covered by the same throttle/error handling (a merge failure raises like
  any other and is recorded per-item).

## Testing (moviemaestro)

Extend `tests/fakes.py` `FakeSupabase` with a `merge_film_duplicates_by_tmdb`
RPC stub that emulates the merge against the in-memory tables (find losers by
`tmdb_link` in `tables["Films"]`, repoint/delete `UserFilms`, delete loser
`Films`, then call the existing `_refresh_user_ratings_rpc` for affected
owners), and record the call so tests can assert it.

New `tests/test_film_ratings.py`:
1. **Merge happens on live slug.** Seed `Films` with a stale-slug row + its
   `UserFilms` row sharing a `tmdb_link`; scrape a `FakeMovie` whose `slug` is
   the new canonical and whose `tmdb_link` matches. Assert: one `Films` row
   left (canonical), `UserFilms` repointed, RPC called with the canonical slug.
2. **Owner holding both slugs → histogram corrected.** Seed one user with both
   the stale and canonical rows at the same rating and a stale `UserRatings`
   count of 2 for that rating; scrape the canonical. Assert the duplicate
   `UserFilms` row is gone and the `UserRatings` count drops to 1 — the
   regression this review caught.
3. **No tmdb_link → no merge call.** `FakeMovie.tmdb_link is None`; assert the
   RPC is not called.
4. **No duplicates → no-op.** Single matching film; assert the row survives and
   the call returns 0 / changes nothing.

Existing `test_orchestrator.py` must stay green — its `FakeMovie`s need a
`tmdb_link` attribute (add one) so the new call path is exercised end-to-end.

## Rollout order

PR 1 must be deployed to prod **before** PR 2, or the worker's `rpc()` call
errors against a missing function. Sequence:
1. Merge PR 1; approve the bpdiscord migrations workflow so the function exists
   in prod.
2. Merge PR 2; redeploy the worker on Railway.

No defensive "function may not exist" catch in the worker — the window is
controlled by deploy order, and a hard failure during it is preferable to a
silent swallow.

## Known limitations

- **Upstream mislink.** The worker canonicalizes on the live slug and trusts the
  TMDB id. If Letterboxd mislinks two genuinely different films to one TMDB id
  (the `1084409` "A Talent for Murder" / "Switzerland" case the backfill
  quarantined), the worker would merge them. There is no clean programmatic
  signal to distinguish a mislink from a legitimate foreign-title variant, so
  this is accepted and documented, same as the backfill migration left it for
  manual review. Rare, and an upstream data fault.
- **Null `tmdb_link`.** A `Films` row whose `tmdb_link` never parsed (letterboxdpy
  returned none) is not an identity, so it is never merged — and because it has
  a `Films` row it is never re-scraped, so its `tmdb_link` stays null. Two such
  rows for one film would persist as a duplicate. Negligible in practice (2 of
  ~23.6k films prod-wide carry a null `tmdb_link`); if it grows, a `tmdb`
  backfill pass (sibling to `backfill_years`) is the fix, out of scope here.

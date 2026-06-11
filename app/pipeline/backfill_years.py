"""Stage 4a: backfill Films.release_year for rows where it's NULL.

One batch of work per call:
  1. SELECT a page of slugs where release_year IS NULL (keyset pagination via
     film_slug — see app/pipeline/backfill_years.py:_eligible_slugs).
  2. Scrape Movie.year for each via the process-wide letterboxd serializer.
  3. UPDATE with `release_year IS NULL` guard so concurrent Stage-2 writes
     or parallel backfill workers never clobber an already-populated value.

The caller (typically a Stage-4b driver — currently a manual operator) is
responsible for looping calls until next_after_slug is None, which signals
the table has been swept once from the cursor's position. Each /backfill-
film-years invocation is stateless; cursors are not persisted between runs.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from letterboxdpy.movie import Movie
from supabase import Client

from app.pipeline import letterboxd_throttle

# Films with release_year < 1888 (Lumière brothers) or more than 5 years in
# the future are almost certainly letterboxdpy parsing junk, not real data.
# Reject and surface as a failure rather than write nonsense into the column.
MIN_YEAR = 1888


def _max_year() -> int:
    return datetime.now(timezone.utc).year + 5


@dataclass
class BackfillResult:
    processed: int
    updated: int
    failures: list[dict]
    next_after_slug: str | None


def _eligible_slugs(
    supabase: Client, *, batch_size: int, after_slug: str | None
) -> list[str]:
    q = (
        supabase.table("Films")
        .select("film_slug")
        .is_("release_year", "null")
    )
    if after_slug is not None:
        # Strict > so the boundary slug from the previous page isn't re-scraped.
        q = q.gt("film_slug", after_slug)
    resp = q.order("film_slug").limit(batch_size).execute()
    return [r["film_slug"] for r in (resp.data or [])]


async def run_batch(
    supabase: Client,
    *,
    batch_size: int,
    after_slug: str | None,
    dry_run: bool,
) -> BackfillResult:
    slugs = _eligible_slugs(
        supabase, batch_size=batch_size, after_slug=after_slug
    )
    failures: list[dict] = []
    updated = 0
    max_year = _max_year()

    for slug in slugs:
        try:
            movie = await letterboxd_throttle.call(Movie, slug)
        except Exception as e:  # noqa: BLE001 — letterboxdpy raises a zoo of types
            failures.append({
                "film_slug": slug,
                "error": f"{type(e).__name__}: {e}",
            })
            continue

        year = getattr(movie, "year", None)

        # Letterboxd page loaded but has no parseable year. Not a failure
        # (some entries legitimately lack one) — silently leave the row NULL.
        # The cursor pagination ensures we don't starve later slugs by
        # repeatedly re-visiting this one within a sweep.
        if year is None:
            continue

        if not isinstance(year, int) or not (MIN_YEAR <= year <= max_year):
            failures.append({
                "film_slug": slug,
                "error": f"implausible year {year!r}",
            })
            continue

        if dry_run:
            updated += 1
            continue

        # Guard: `is_("release_year", "null")` translates to a server-side
        # WHERE release_year IS NULL on the PATCH, so a concurrent Stage-2
        # upsert or parallel backfill can't be clobbered. A non-empty
        # resp.data means the guard let the UPDATE through; empty means a
        # concurrent writer beat us — not a failure, just a no-op.
        # updated_at intentionally omitted from the payload — no UPDATE
        # trigger bumps it, so downstream freshness signals stay clean.
        try:
            resp = (
                supabase.table("Films")
                .update({"release_year": year})
                .eq("film_slug", slug)
                .is_("release_year", "null")
                .execute()
            )
        except Exception as e:  # noqa: BLE001
            failures.append({
                "film_slug": slug,
                "error": f"{type(e).__name__}: {e}",
            })
            continue

        if resp.data:
            updated += 1

    next_after_slug = (
        slugs[-1] if len(slugs) == batch_size else None
    )
    return BackfillResult(
        processed=len(slugs),
        updated=updated,
        failures=failures,
        next_after_slug=next_after_slug,
    )

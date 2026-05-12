"""Top-level pipeline driver. One coroutine per refresh job.

Phases run sequentially:
  1. user_scrape   — for each user in Users (is_discord=true), scrape rated
                     films via letterboxdpy, upsert UserFilms.
  2. missing_films — call get_missing_films RPC, find films UserFilms references
                     but Films doesn't have.
  3. film_ratings  — for each missing slug, scrape Letterboxd average rating
                     + metadata via letterboxdpy.movie.Movie, upsert Films.

Cancel handling: between every per-item operation we poll refresh_jobs.status.
If status is 'cancelled', we exit cleanly within ~10s (per-item latency dominates).

Per-item failures append to errors[] and continue. Structural failures
(unhandled exceptions, DB unreachable) flip the row to status='failed'.

All blocking calls go through asyncio.to_thread() so the event loop can
service cancel polling and so concurrency can be added later without
restructuring.
"""

from __future__ import annotations

import asyncio
import logging
from uuid import UUID

from supabase import Client

from letterboxdpy.core.exceptions import ResourceNotFoundError

from app.pipeline.film_ratings import scrape_and_upsert_film, tombstone_film
from app.pipeline.job_state import JobCancelled, JobState
from app.pipeline.missing import get_missing_film_slugs
from app.pipeline.user_films import scrape_and_upsert_user_films
from app.pipeline.users import fetch_users

log = logging.getLogger("moviemaestro.orchestrator")


def _check_cancel(state: JobState) -> None:
    if state.is_cancelled():
        raise JobCancelled()


async def _phase_user_scrape(state: JobState, supabase: Client) -> None:
    state.set_phase("user_scrape", processed=0, total=0, current=None, films_added=0)
    users = await asyncio.to_thread(fetch_users, supabase)
    total = len(users)
    state.update_progress("user_scrape", total=total)
    state.flush_progress()
    state.append_log(f"user_scrape: {total} users to process")

    films_added = 0
    for i, lbusername in enumerate(users, start=1):
        _check_cancel(state)
        state.update_progress("user_scrape", current=lbusername)
        try:
            count = await asyncio.to_thread(
                scrape_and_upsert_user_films, supabase, lbusername
            )
            films_added += count
            state.update_progress(
                "user_scrape",
                processed=i,
                films_added=films_added,
            )
        except Exception as e:  # noqa: BLE001 — record + continue
            state.add_error("user_scrape", lbusername, e)
            state.update_progress("user_scrape", processed=i)

    state.flush_progress()
    state.append_log(f"user_scrape complete: {films_added} films across {total} users")


async def _phase_missing_films(state: JobState, supabase: Client) -> list[str]:
    state.set_phase("missing_films", count=0)
    _check_cancel(state)
    slugs = await asyncio.to_thread(get_missing_film_slugs, supabase)
    state.update_progress("missing_films", count=len(slugs))
    state.flush_progress()
    state.append_log(f"missing_films: {len(slugs)} slugs to fetch")
    return slugs


async def _phase_film_ratings(state: JobState, supabase: Client, slugs: list[str]) -> None:
    state.set_phase("film_ratings", processed=0, total=len(slugs), current=None, tombstoned=0)
    if not slugs:
        state.flush_progress()
        return

    tombstoned = 0
    for i, slug in enumerate(slugs, start=1):
        _check_cancel(state)
        state.update_progress("film_ratings", current=slug)
        try:
            await asyncio.to_thread(scrape_and_upsert_film, supabase, slug)
            state.update_progress("film_ratings", processed=i)
        except ResourceNotFoundError:
            # Slug 404s on Letterboxd (renamed or removed). Insert a tombstone
            # so get_missing_films() stops returning it on subsequent runs.
            # Manual retry: DELETE the Films row.
            await asyncio.to_thread(tombstone_film, supabase, slug)
            tombstoned += 1
            state.append_log(f"film_ratings: {slug} → tombstoned (404)")
            state.update_progress("film_ratings", processed=i, tombstoned=tombstoned)
        except Exception as e:  # noqa: BLE001
            state.add_error("film_ratings", slug, e)
            state.update_progress("film_ratings", processed=i)

    state.flush_progress()
    state.append_log(
        f"film_ratings complete: {len(slugs)} attempted, {tombstoned} tombstoned"
    )


async def run(supabase: Client, job_id: UUID) -> None:
    """Drive the full refresh pipeline for one job. Designed to be spawned via
    asyncio.create_task() — never awaited from the request handler."""
    state = JobState(supabase, job_id)
    log.info("orchestrator starting job %s", job_id)

    try:
        # Defense in depth — the partial unique index already prevents two
        # 'running' rows, but if that ever changed (or a stale row got missed)
        # this catches it without trampling the other job.
        if state.another_job_running():
            state.fail("another job is already running")
            return

        await _phase_user_scrape(state, supabase)
        slugs = await _phase_missing_films(state, supabase)
        await _phase_film_ratings(state, supabase, slugs)

        state.complete()
    except JobCancelled:
        state.mark_cancelled()
    except Exception as e:  # noqa: BLE001
        log.exception("orchestrator crashed")
        state.fail(f"{type(e).__name__}: {e}")

"""Process-wide chokepoint for all letterboxdpy HTTP calls.

Letterboxd publishes no rate limits; enforcement is opaque Cloudflare-based.
letterboxdpy itself maintains a class-level singleton curl_cffi session
(see letterboxdpy.core.scraper.Scraper._session). curl_cffi documents the
session as thread-safe (thread-local libcurl handles via use_thread_local_curl)
but recommends one-per-thread, and bug reports cluster in heavy multi-threaded
use.

Today's bulk refresh runs strictly one job at a time and has never been
rate-limited. The /fetcher migration opens the door to concurrent per-user
jobs in the same process. To preserve the de-facto safe behavior, this
module serializes every letterboxdpy entry point through a process-wide
asyncio.Semaphore(1). Bulk and per-user paths share the same gate.

call() also retries a transient Letterboxd block (Cloudflare IP flag / HTTP
429) with backoff. If the block survives the retries the exception propagates
and the orchestrator turns it into a terminal 'letterboxd_blocked' failure
(fail-fast: it aborts the run rather than grinding through guaranteed-failing
follow-up calls against a flagged IP).
"""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Callable
from typing import TypeVar

from letterboxdpy.core.exceptions import AccessDeniedError, InvalidResponseError

T = TypeVar("T")

log = logging.getLogger("moviemaestro.letterboxd_throttle")

# Start strictly serial. Tune up only after observing prod behavior.
MAX_CONCURRENCY = 1

_semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

# Backoff (seconds) for each retry after a transient block; one entry per
# retry, so N entries = N+1 total attempts. letterboxdpy already retries a 403
# five times over ~20s internally, so these outer waits are deliberately
# longer — they exist only to outlast a block that survived that internal
# retry. Kept short enough that a genuinely-blocked job fails in ~2 min rather
# than hanging a /fetcher user's spinner indefinitely.
_RETRY_BACKOFF_SECONDS: tuple[float, ...] = (15.0, 45.0)


async def _sleep(seconds: float) -> None:
    # Indirection so tests can stub out the wait without real time passing.
    await asyncio.sleep(seconds)


def is_block_error(e: BaseException) -> bool:
    """A transient Letterboxd block: a Cloudflare/IP flag (AccessDeniedError)
    or an HTTP 429 rate-limit (InvalidResponseError code=429).

    A private profile raises PrivateRouteError, which is a real per-user state,
    NOT a block — it must keep flowing to the record-and-continue path.
    """
    return isinstance(e, AccessDeniedError) or (
        isinstance(e, InvalidResponseError) and getattr(e, "code", None) == 429
    )


async def call(fn: Callable[..., T], *args, **kwargs) -> T:
    """Run a blocking letterboxdpy function in a worker thread, serialized
    process-wide, retrying a transient block with backoff.

    Use this anywhere we'd otherwise asyncio.to_thread() a letterboxdpy call.
    Pure-DB calls (fetch_users, get_missing_film_slugs, tombstone_film) do
    NOT go through here — they touch Supabase, not Letterboxd.

    Non-block exceptions propagate immediately (no retry). On a block we retry
    per _RETRY_BACKOFF_SECONDS; if it persists, the final exception propagates.
    The semaphore is held across backoff sleeps — a no-op at MAX_CONCURRENCY=1,
    intentional if it's ever raised (don't let other calls hammer a flagged IP
    while one is backing off).
    """
    attempts = len(_RETRY_BACKOFF_SECONDS) + 1
    async with _semaphore:
        for i in range(attempts):
            try:
                return await asyncio.to_thread(fn, *args, **kwargs)
            except Exception as e:  # noqa: BLE001 — classify then retry-or-raise
                if i == attempts - 1 or not is_block_error(e):
                    raise
                wait = _RETRY_BACKOFF_SECONDS[i] + random.random()  # noqa: S311
                log.warning(
                    "letterboxd block (%s); retry %d/%d in %.1fs",
                    type(e).__name__,
                    i + 1,
                    attempts - 1,
                    wait,
                )
                await _sleep(wait)
    raise AssertionError("unreachable")  # loop always returns or raises

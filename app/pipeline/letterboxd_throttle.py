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

Deferred follow-ups (bpdiscord-yao): minimum gap between requests, circuit
breaker on AccessDeniedError / HTTP 429, UI-visible cooldown error.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")

# Start strictly serial. Tune up only after observing prod behavior and
# landing the hardening in bpdiscord-yao.
MAX_CONCURRENCY = 1

_semaphore = asyncio.Semaphore(MAX_CONCURRENCY)


async def call(fn: Callable[..., T], *args, **kwargs) -> T:
    """Run a blocking letterboxdpy function in a worker thread, serialized
    process-wide.

    Use this anywhere we'd otherwise asyncio.to_thread() a letterboxdpy call.
    Pure-DB calls (fetch_users, get_missing_film_slugs, tombstone_film) do
    NOT go through here — they touch Supabase, not Letterboxd.
    """
    async with _semaphore:
        return await asyncio.to_thread(fn, *args, **kwargs)

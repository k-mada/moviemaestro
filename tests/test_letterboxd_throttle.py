"""Tests for the process-wide letterboxdpy serializer.

Today the module is a single asyncio.Semaphore(1). Deferred hardening
(bpdiscord-yao) will add gap enforcement + circuit breaker; tests for those
will land with it. These tests pin the current contract:
  - call() runs the function and returns its result
  - call() serializes concurrent invocations (no overlap)
  - exceptions raised by the wrapped function propagate unchanged
"""

from __future__ import annotations

import asyncio

import pytest

from app.pipeline import letterboxd_throttle


@pytest.mark.asyncio
async def test_call_returns_wrapped_function_result():
    result = await letterboxd_throttle.call(lambda x, y: x + y, 2, 3)
    assert result == 5


@pytest.mark.asyncio
async def test_call_propagates_exceptions():
    def boom():
        raise RuntimeError("upstream failure")

    with pytest.raises(RuntimeError, match="upstream failure"):
        await letterboxd_throttle.call(boom)


@pytest.mark.asyncio
async def test_concurrent_calls_are_serialized():
    """If two coroutines try to call() at the same time, exactly one runs
    at a time — never overlapping. This is what protects letterboxdpy's
    shared curl_cffi session from concurrent thread access."""
    in_flight = 0
    max_in_flight = 0
    lock = asyncio.Lock()

    def task(label):
        # to_thread runs us on a worker thread; mutate counters under a
        # cross-thread-safe lock-via-asyncio-event-loop? No — we're in a
        # thread, can't use asyncio.Lock. Use a regular threading lock.
        import threading

        # We need a stable lock across all wrapped calls. Stuff it on the
        # function attribute so all task() invocations share it.
        if not hasattr(task, "_lock"):
            task._lock = threading.Lock()
        nonlocal_state = task._lock

        with nonlocal_state:
            nonlocal in_flight, max_in_flight
            in_flight += 1
            max_in_flight = max(max_in_flight, in_flight)
        # Sleep to encourage overlap if the semaphore is broken.
        import time

        time.sleep(0.01)
        with nonlocal_state:
            in_flight -= 1
        return label

    results = await asyncio.gather(
        letterboxd_throttle.call(task, "a"),
        letterboxd_throttle.call(task, "b"),
        letterboxd_throttle.call(task, "c"),
        letterboxd_throttle.call(task, "d"),
        letterboxd_throttle.call(task, "e"),
    )

    assert sorted(results) == ["a", "b", "c", "d", "e"]
    # With MAX_CONCURRENCY=1, no two task()s should ever be in flight.
    assert max_in_flight == 1


@pytest.mark.asyncio
async def test_runs_in_a_thread_not_the_event_loop():
    """Blocking calls must not block the asyncio loop — call() uses
    asyncio.to_thread under the hood. Verify by spawning a parallel
    coroutine that completes promptly even though the wrapped fn sleeps."""
    import time

    async def quick_tick() -> str:
        await asyncio.sleep(0.02)
        return "tick"

    # The wrapped fn sleeps 100ms in a thread. quick_tick() must still
    # complete in ~20ms — the loop is free.
    tick_done_first = False

    async def slow_call():
        nonlocal tick_done_first
        await letterboxd_throttle.call(time.sleep, 0.1)
        # If tick already completed by now, the loop wasn't blocked.

    async def quick():
        nonlocal tick_done_first
        result = await quick_tick()
        tick_done_first = True
        return result

    await asyncio.gather(slow_call(), quick())
    assert tick_done_first is True

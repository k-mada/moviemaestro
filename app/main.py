import asyncio
import logging
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field

from app.db import get_supabase
from app.pipeline import backfill_years, orchestrator, user_rss
from app.settings import Settings, get_settings

# In-process dedupe: a network retry of /start with the same job_id should not
# spawn a second orchestrator. Cross-process safety (multi-replica Railway
# deploys) relies on the partial unique index on refresh_jobs (status) WHERE
# status='running' enforced by the bpdiscord admin endpoint.
_active_jobs: dict[UUID, asyncio.Task] = {}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("moviemaestro")

app = FastAPI(title="moviemaestro", version="0.1.0")

# auto_error=False so missing creds surface as 401 (our custom check) rather
# than FastAPI's default 403 from HTTPBearer.
security = HTTPBearer(auto_error=False)

# Hold strong refs to spawned orchestrator tasks. Without this, asyncio's
# weak-ref task tracking lets the GC kill in-flight tasks unpredictably.
_background_tasks: set[asyncio.Task] = set()


def require_worker_secret(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(security)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> None:
    if credentials is None or credentials.credentials != settings.worker_shared_secret:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid or missing bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )


class StartRequest(BaseModel):
    job_id: UUID


class ScrapeUserRequest(BaseModel):
    job_id: UUID
    lbusername: str


class RefreshUserRequest(BaseModel):
    lbusername: str


class RefreshUserResponse(BaseModel):
    lbusername: str
    watch_items: int
    upserted: int


class StartResponse(BaseModel):
    job_id: UUID
    status: str


class BackfillFailure(BaseModel):
    film_slug: str
    error: str


class BackfillRequest(BaseModel):
    # Ceiling of 200 is a soft guard against blowing through Railway's HTTP
    # edge timeout. At Semaphore(1) serialization + ~1–2 s per Letterboxd
    # page, batch_size=200 is ~5–7 minutes worst case. Stage 4b drives the
    # cadence; pick lower if it tightens.
    batch_size: int = Field(default=100, ge=1, le=200)
    after_slug: str | None = None
    dry_run: bool = False


class BackfillResponse(BaseModel):
    processed: int
    updated: int
    failures: list[BackfillFailure]
    next_after_slug: str | None


@app.get("/healthz")
def healthz() -> dict[str, bool]:
    return {"ok": True}


async def _spawn(
    job_id: UUID, *, table: str, lbusername: str | None = None
) -> StartResponse:
    """Idempotent fire-and-forget orchestrator spawn, shared between /start
    and /scrape-user. The orchestrator updates the job row as it runs; the
    caller observes progress via Realtime, not via this response.

    In-process dedupe via _active_jobs prevents a network-retried POST from
    spawning a second orchestrator for the same job_id within this process.
    Cross-process safety lives in the SQL partial unique indexes on
    refresh_jobs and user_scrape_jobs, which bpdiscord catches at INSERT.
    """
    existing = _active_jobs.get(job_id)
    if existing is not None and not existing.done():
        log.info("spawn ignored — job already running in this process: %s", job_id)
        return StartResponse(job_id=job_id, status="already_running")

    supabase = get_supabase()
    task = asyncio.create_task(
        orchestrator.run(supabase, job_id, table=table, lbusername=lbusername)
    )
    _background_tasks.add(task)
    _active_jobs[job_id] = task

    def _on_done(t: asyncio.Task) -> None:
        _background_tasks.discard(t)
        _active_jobs.pop(job_id, None)

    task.add_done_callback(_on_done)
    return StartResponse(job_id=job_id, status="accepted")


@app.post(
    "/start",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=StartResponse,
    dependencies=[Depends(require_worker_secret)],
)
async def start(payload: StartRequest) -> StartResponse:
    log.info("start request received: job_id=%s", payload.job_id)
    return await _spawn(payload.job_id, table="refresh_jobs")


@app.post(
    "/backfill-film-years",
    response_model=BackfillResponse,
    dependencies=[Depends(require_worker_secret)],
)
async def backfill_film_years(payload: BackfillRequest) -> BackfillResponse:
    log.info(
        "backfill request: batch_size=%d after_slug=%s dry_run=%s",
        payload.batch_size,
        payload.after_slug,
        payload.dry_run,
    )
    result = await backfill_years.run_batch(
        get_supabase(),
        batch_size=payload.batch_size,
        after_slug=payload.after_slug,
        dry_run=payload.dry_run,
    )
    return BackfillResponse(
        processed=result.processed,
        updated=result.updated,
        failures=[BackfillFailure(**f) for f in result.failures],
        next_after_slug=result.next_after_slug,
    )


@app.post(
    "/refresh-user",
    response_model=RefreshUserResponse,
    dependencies=[Depends(require_worker_secret)],
)
async def refresh_user(payload: RefreshUserRequest) -> RefreshUserResponse:
    log.info("refresh-user request received: lbusername=%s", payload.lbusername)
    try:
        result = await user_rss.refresh_user_from_rss(
            get_supabase(), payload.lbusername
        )
    except user_rss.RssFetchError as e:
        # A failed/blocked fetch is a bad gateway, not an empty refresh.
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY, detail=str(e)
        ) from e
    return RefreshUserResponse(
        lbusername=result.lbusername,
        watch_items=result.watch_items,
        upserted=result.upserted,
    )


@app.post(
    "/scrape-user",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=StartResponse,
    dependencies=[Depends(require_worker_secret)],
)
async def scrape_user(payload: ScrapeUserRequest) -> StartResponse:
    log.info(
        "scrape-user request received: job_id=%s lbusername=%s",
        payload.job_id,
        payload.lbusername,
    )
    return await _spawn(
        payload.job_id,
        table="user_scrape_jobs",
        lbusername=payload.lbusername,
    )

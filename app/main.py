import asyncio
import logging
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

from app.db import get_supabase
from app.pipeline import orchestrator
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


class StartResponse(BaseModel):
    job_id: UUID
    status: str


@app.get("/healthz")
def healthz() -> dict[str, bool]:
    return {"ok": True}


@app.post(
    "/start",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=StartResponse,
    dependencies=[Depends(require_worker_secret)],
)
async def start(payload: StartRequest) -> StartResponse:
    log.info("start request received: job_id=%s", payload.job_id)
    # Idempotent: if this job_id is already in flight, ack without re-spawning.
    existing = _active_jobs.get(payload.job_id)
    if existing is not None and not existing.done():
        log.info("start ignored — job already running in this process: %s", payload.job_id)
        return StartResponse(job_id=payload.job_id, status="already_running")

    # Fire-and-forget. The orchestrator updates refresh_jobs as it runs;
    # the caller observes progress via Realtime, not via this response.
    supabase = get_supabase()
    task = asyncio.create_task(
        orchestrator.run(supabase, payload.job_id, table="refresh_jobs")
    )
    _background_tasks.add(task)
    _active_jobs[payload.job_id] = task

    def _on_done(t: asyncio.Task) -> None:
        _background_tasks.discard(t)
        _active_jobs.pop(payload.job_id, None)

    task.add_done_callback(_on_done)
    return StartResponse(job_id=payload.job_id, status="accepted")

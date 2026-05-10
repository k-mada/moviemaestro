import logging
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

from app.settings import Settings, get_settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("moviemaestro")

app = FastAPI(title="moviemaestro", version="0.1.0")

# auto_error=False so missing creds surface as 401 (our custom check) rather
# than FastAPI's default 403 from HTTPBearer.
security = HTTPBearer(auto_error=False)


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
def start(payload: StartRequest) -> StartResponse:
    # Skeleton stub. Pipeline kicks off in bpdiscord-7bh — this will spawn an
    # asyncio.create_task(orchestrator.run(job_id)) and return immediately.
    log.info("start request received: job_id=%s", payload.job_id)
    return StartResponse(job_id=payload.job_id, status="accepted")

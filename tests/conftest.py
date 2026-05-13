import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.settings import Settings, get_settings

TEST_SECRET = "test-secret-do-not-use-in-prod"


def _override_settings() -> Settings:
    return Settings(
        supabase_url="https://test.supabase.co",
        supabase_service_role_key="test-service-role-key",
        worker_shared_secret=TEST_SECRET,
    )


@pytest.fixture
def spawned_jobs(monkeypatch):
    """Captures job_ids that /start would have handed to orchestrator.run."""
    jobs: list[dict] = []

    async def fake_run(supabase, job_id, *, table, lbusername=None):
        jobs.append(
            {"supabase": supabase, "job_id": job_id, "table": table, "lbusername": lbusername}
        )

    monkeypatch.setattr("app.main.orchestrator.run", fake_run)
    monkeypatch.setattr("app.main.get_supabase", lambda: "FAKE_SUPABASE")
    return jobs


@pytest.fixture
def client(spawned_jobs) -> TestClient:
    app.dependency_overrides[get_settings] = _override_settings
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.clear()

from tests.conftest import TEST_SECRET

JOB_ID = "11111111-2222-3333-4444-555555555555"


def _auth() -> dict[str, str]:
    return {"Authorization": f"Bearer {TEST_SECRET}"}


class TestAuth:
    def test_no_authorization_header_returns_401(self, client):
        r = client.post("/start", json={"job_id": JOB_ID})
        assert r.status_code == 401
        assert "bearer" in r.json()["detail"].lower()
        assert r.headers.get("www-authenticate") == "Bearer"

    def test_wrong_bearer_token_returns_401(self, client):
        r = client.post(
            "/start",
            headers={"Authorization": "Bearer wrong-token"},
            json={"job_id": JOB_ID},
        )
        assert r.status_code == 401

    def test_non_bearer_scheme_returns_401(self, client):
        r = client.post(
            "/start",
            headers={"Authorization": f"Basic {TEST_SECRET}"},
            json={"job_id": JOB_ID},
        )
        # HTTPBearer with auto_error=False accepts anything as credentials.
        # Our check compares the .credentials field, which for non-Bearer
        # schemes will be the raw value or empty depending on parser.
        # We accept any 401 response as the contract — never let this through.
        assert r.status_code == 401

    def test_empty_bearer_returns_401(self, client):
        r = client.post(
            "/start",
            headers={"Authorization": "Bearer "},
            json={"job_id": JOB_ID},
        )
        assert r.status_code == 401


class TestSuccess:
    def test_correct_bearer_returns_202(self, client):
        r = client.post("/start", headers=_auth(), json={"job_id": JOB_ID})
        assert r.status_code == 202
        body = r.json()
        assert body["job_id"] == JOB_ID
        assert body["status"] == "accepted"

    def test_orchestrator_is_spawned_with_job_id(self, client, spawned_jobs):
        # /start must hand off to orchestrator.run — the response is just an ack.
        r = client.post("/start", headers=_auth(), json={"job_id": JOB_ID})
        assert r.status_code == 202
        assert len(spawned_jobs) == 1
        assert str(spawned_jobs[0]["job_id"]) == JOB_ID

    def test_failed_auth_does_not_spawn_orchestrator(self, client, spawned_jobs):
        client.post("/start", json={"job_id": JOB_ID})  # no auth
        client.post("/start", headers={"Authorization": "Bearer wrong"}, json={"job_id": JOB_ID})
        assert spawned_jobs == []

    def test_invalid_payload_does_not_spawn_orchestrator(self, client, spawned_jobs):
        client.post("/start", headers=_auth(), json={"job_id": "bogus"})
        assert spawned_jobs == []


class TestIdempotency:
    """Same job_id /start retried in-process should not double-spawn.

    Uses httpx.AsyncClient (not TestClient) because TestClient's portal
    appears to GC pending background tasks between sync requests, defeating
    the in-process state check we're trying to verify."""

    async def test_duplicate_start_returns_already_running(self, monkeypatch):
        import asyncio
        from httpx import ASGITransport, AsyncClient

        from app.main import _active_jobs, app
        from app.pipeline import orchestrator as orch_mod
        from app.settings import get_settings

        spawned: list[str] = []

        async def never_done(supabase, job_id, *, table, lbusername=None):
            spawned.append(str(job_id))
            await asyncio.sleep(60)

        monkeypatch.setattr(orch_mod, "run", never_done)
        monkeypatch.setattr("app.main.get_supabase", lambda: "FAKE")
        app.dependency_overrides[get_settings] = _override_settings_for_async

        try:
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://t") as ac:
                r1 = await ac.post("/start", headers=_auth(), json={"job_id": JOB_ID})
                r2 = await ac.post("/start", headers=_auth(), json={"job_id": JOB_ID})
            assert r1.status_code == 202
            assert r1.json()["status"] == "accepted"
            assert r2.status_code == 202
            assert r2.json()["status"] == "already_running"
            assert len(spawned) == 1
        finally:
            app.dependency_overrides.clear()
            for t in list(_active_jobs.values()):
                t.cancel()
            _active_jobs.clear()


def _override_settings_for_async():
    from app.settings import Settings
    return Settings(
        supabase_url="https://test.supabase.co",
        supabase_service_role_key="k",
        worker_shared_secret=TEST_SECRET,
    )


class TestPayloadValidation:
    def test_missing_job_id_returns_422(self, client):
        r = client.post("/start", headers=_auth(), json={})
        assert r.status_code == 422

    def test_invalid_uuid_returns_422(self, client):
        r = client.post("/start", headers=_auth(), json={"job_id": "not-a-uuid"})
        assert r.status_code == 422

    def test_empty_body_returns_422(self, client):
        r = client.post("/start", headers=_auth())
        assert r.status_code == 422

    def test_extra_fields_ignored(self, client):
        # Extra fields don't break — pydantic default is ignore.
        r = client.post(
            "/start",
            headers=_auth(),
            json={"job_id": JOB_ID, "unexpected": "field"},
        )
        assert r.status_code == 202


class TestAuthEvaluatedBeforePayload:
    """Auth check should come before body validation so we don't leak
    payload-shape information to unauthenticated callers."""

    def test_no_auth_with_invalid_body_still_returns_401(self, client):
        r = client.post("/start", json={"junk": "data"})
        assert r.status_code == 401

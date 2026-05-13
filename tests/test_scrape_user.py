"""Tests for POST /scrape-user — the per-user /fetcher entry point.

Same auth model as /start (shared worker secret). Same idempotency semantics
(in-process _active_jobs dedupe). The only delta from /start is the payload
shape (adds `lbusername`) and what orchestrator.run is called with
(table='user_scrape_jobs', lbusername=<name>).
"""

from tests.conftest import TEST_SECRET

JOB_ID = "22222222-3333-4444-5555-666666666666"
LBUSER = "alice"


def _auth() -> dict[str, str]:
    return {"Authorization": f"Bearer {TEST_SECRET}"}


def _body() -> dict[str, str]:
    return {"job_id": JOB_ID, "lbusername": LBUSER}


class TestAuth:
    def test_no_authorization_header_returns_401(self, client):
        r = client.post("/scrape-user", json=_body())
        assert r.status_code == 401
        assert "bearer" in r.json()["detail"].lower()
        assert r.headers.get("www-authenticate") == "Bearer"

    def test_wrong_bearer_token_returns_401(self, client):
        r = client.post(
            "/scrape-user",
            headers={"Authorization": "Bearer wrong-token"},
            json=_body(),
        )
        assert r.status_code == 401

    def test_empty_bearer_returns_401(self, client):
        r = client.post(
            "/scrape-user",
            headers={"Authorization": "Bearer "},
            json=_body(),
        )
        assert r.status_code == 401


class TestSuccess:
    def test_correct_bearer_returns_202(self, client):
        r = client.post("/scrape-user", headers=_auth(), json=_body())
        assert r.status_code == 202
        body = r.json()
        assert body["job_id"] == JOB_ID
        assert body["status"] == "accepted"

    def test_orchestrator_spawned_with_table_and_lbusername(self, client, spawned_jobs):
        """The handoff to orchestrator.run must pass table='user_scrape_jobs'
        and the lbusername from the payload — otherwise we'd be writing to
        the wrong table or scraping the wrong user."""
        r = client.post("/scrape-user", headers=_auth(), json=_body())
        assert r.status_code == 202
        assert len(spawned_jobs) == 1
        job = spawned_jobs[0]
        assert str(job["job_id"]) == JOB_ID
        assert job["table"] == "user_scrape_jobs"
        assert job["lbusername"] == LBUSER

    def test_failed_auth_does_not_spawn_orchestrator(self, client, spawned_jobs):
        client.post("/scrape-user", json=_body())  # no auth
        client.post(
            "/scrape-user", headers={"Authorization": "Bearer wrong"}, json=_body()
        )
        assert spawned_jobs == []


class TestPayloadValidation:
    def test_missing_lbusername_returns_422(self, client):
        r = client.post("/scrape-user", headers=_auth(), json={"job_id": JOB_ID})
        assert r.status_code == 422

    def test_missing_job_id_returns_422(self, client):
        r = client.post("/scrape-user", headers=_auth(), json={"lbusername": LBUSER})
        assert r.status_code == 422

    def test_invalid_job_id_uuid_returns_422(self, client):
        r = client.post(
            "/scrape-user", headers=_auth(), json={"job_id": "bogus", "lbusername": LBUSER}
        )
        assert r.status_code == 422

    def test_empty_body_returns_422(self, client):
        r = client.post("/scrape-user", headers=_auth(), json={})
        assert r.status_code == 422

    def test_extra_fields_ignored(self, client, spawned_jobs):
        r = client.post(
            "/scrape-user",
            headers=_auth(),
            json={**_body(), "extra_field": "ignored"},
        )
        assert r.status_code == 202
        assert len(spawned_jobs) == 1


class TestAuthEvaluatedBeforePayload:
    def test_no_auth_with_invalid_body_still_returns_401(self, client):
        # We never leak payload-validation errors to unauthorized callers.
        r = client.post("/scrape-user", json={"job_id": "bogus"})
        assert r.status_code == 401



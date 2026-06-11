"""HTTP tests for POST /backfill-film-years.

Auth mirrors tests/test_start.py:TestAuth. Body validation and response
shape get their own coverage here; the run_batch internals are tested in
tests/test_backfill_years.py.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.pipeline import backfill_years
from app.settings import Settings, get_settings
from tests.conftest import TEST_SECRET
from tests.fakes import FakeSupabase


def _override_settings() -> Settings:
    return Settings(
        supabase_url="https://test.supabase.co",
        supabase_service_role_key="test-service-role-key",
        worker_shared_secret=TEST_SECRET,
    )


@pytest.fixture
def sb_and_client(monkeypatch):
    """A FakeSupabase wired into the endpoint, plus a TestClient. Uses a
    local fixture rather than tests/conftest.py:client because that fixture
    stubs get_supabase to a string sentinel, which run_batch can't call."""
    sb = FakeSupabase()
    monkeypatch.setattr("app.main.get_supabase", lambda: sb)

    class _FakeMovie:
        def __init__(self, slug):
            self.slug = slug
            self.year = 2020

    monkeypatch.setattr(backfill_years, "Movie", _FakeMovie)

    app.dependency_overrides[get_settings] = _override_settings
    try:
        yield sb, TestClient(app)
    finally:
        app.dependency_overrides.clear()


def _auth() -> dict[str, str]:
    return {"Authorization": f"Bearer {TEST_SECRET}"}


class TestAuth:
    def test_no_authorization_header_returns_401(self, sb_and_client):
        _, client = sb_and_client
        r = client.post("/backfill-film-years", json={})
        assert r.status_code == 401
        assert r.headers.get("www-authenticate") == "Bearer"

    def test_wrong_bearer_token_returns_401(self, sb_and_client):
        _, client = sb_and_client
        r = client.post(
            "/backfill-film-years",
            headers={"Authorization": "Bearer wrong-token"},
            json={},
        )
        assert r.status_code == 401

    def test_empty_bearer_returns_401(self, sb_and_client):
        _, client = sb_and_client
        r = client.post(
            "/backfill-film-years",
            headers={"Authorization": "Bearer "},
            json={},
        )
        assert r.status_code == 401


class TestSuccess:
    def test_returns_200_with_full_response_shape(self, sb_and_client):
        sb, client = sb_and_client
        sb.tables["Films"].append(
            {"film_slug": "a", "release_year": None}
        )
        r = client.post("/backfill-film-years", headers=_auth(), json={})
        assert r.status_code == 200
        body = r.json()
        assert set(body) == {"processed", "updated", "failures", "next_after_slug"}
        assert body["processed"] == 1
        assert body["updated"] == 1
        assert body["failures"] == []

    def test_default_batch_size_when_body_empty(self, sb_and_client):
        _, client = sb_and_client
        r = client.post("/backfill-film-years", headers=_auth(), json={})
        assert r.status_code == 200

    def test_dry_run_passthrough(self, sb_and_client):
        sb, client = sb_and_client
        sb.tables["Films"].append(
            {"film_slug": "preview", "release_year": None}
        )
        r = client.post(
            "/backfill-film-years",
            headers=_auth(),
            json={"dry_run": True},
        )
        assert r.status_code == 200
        assert r.json()["updated"] == 1
        # ...but the table was untouched.
        assert sb.tables["Films"][0]["release_year"] is None

    def test_after_slug_passthrough(self, sb_and_client):
        sb, client = sb_and_client
        for s in ["a", "b", "c"]:
            sb.tables["Films"].append({"film_slug": s, "release_year": None})
        r = client.post(
            "/backfill-film-years",
            headers=_auth(),
            json={"after_slug": "a", "batch_size": 10},
        )
        body = r.json()
        assert body["processed"] == 2
        assert body["next_after_slug"] is None


class TestValidation:
    def test_batch_size_below_min_returns_422(self, sb_and_client):
        _, client = sb_and_client
        r = client.post(
            "/backfill-film-years",
            headers=_auth(),
            json={"batch_size": 0},
        )
        assert r.status_code == 422

    def test_batch_size_above_max_returns_422(self, sb_and_client):
        _, client = sb_and_client
        r = client.post(
            "/backfill-film-years",
            headers=_auth(),
            json={"batch_size": 500},
        )
        assert r.status_code == 422

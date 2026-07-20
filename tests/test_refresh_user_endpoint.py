"""HTTP tests for POST /refresh-user.

Auth mirrors tests/test_backfill_endpoint.py:TestAuth. Parser + upsert
internals are covered in tests/test_user_rss.py; here we assert the HTTP
surface: auth, response shape, and the 502 fetch-failure mapping.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.pipeline import user_rss
from app.settings import Settings, get_settings
from tests.conftest import TEST_SECRET
from tests.fakes import FakeSupabase
from tests.test_user_rss import FEED


def _override_settings() -> Settings:
    return Settings(
        supabase_url="https://test.supabase.co",
        supabase_service_role_key="test-service-role-key",
        worker_shared_secret=TEST_SECRET,
    )


@pytest.fixture
def sb_and_client(monkeypatch):
    sb = FakeSupabase()
    monkeypatch.setattr("app.main.get_supabase", lambda: sb)
    monkeypatch.setattr(user_rss, "_fetch_rss", lambda lbusername: FEED)
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
        r = client.post("/refresh-user", json={"lbusername": "tester"})
        assert r.status_code == 401
        assert r.headers.get("www-authenticate") == "Bearer"

    def test_wrong_bearer_token_returns_401(self, sb_and_client):
        _, client = sb_and_client
        r = client.post(
            "/refresh-user",
            headers={"Authorization": "Bearer wrong-token"},
            json={"lbusername": "tester"},
        )
        assert r.status_code == 401

    def test_empty_bearer_returns_401(self, sb_and_client):
        _, client = sb_and_client
        r = client.post(
            "/refresh-user",
            headers={"Authorization": "Bearer "},
            json={"lbusername": "tester"},
        )
        assert r.status_code == 401


class TestValidation:
    def test_missing_lbusername_returns_422(self, sb_and_client):
        _, client = sb_and_client
        r = client.post("/refresh-user", headers=_auth(), json={})
        assert r.status_code == 422


class TestSuccess:
    def test_returns_200_with_full_response_shape(self, sb_and_client):
        sb, client = sb_and_client
        r = client.post(
            "/refresh-user", headers=_auth(), json={"lbusername": "tester"}
        )
        assert r.status_code == 200
        body = r.json()
        assert body == {"lbusername": "tester", "watch_items": 3, "upserted": 3}
        assert len(sb.tables["UserFilms"]) == 3

    def test_empty_feed_returns_200_upserted_zero(self, sb_and_client, monkeypatch):
        sb, client = sb_and_client
        empty = "<rss version='2.0'><channel><title>x</title></channel></rss>"
        monkeypatch.setattr(user_rss, "_fetch_rss", lambda lbusername: empty)
        r = client.post(
            "/refresh-user", headers=_auth(), json={"lbusername": "tester"}
        )
        assert r.status_code == 200
        assert r.json()["upserted"] == 0


class TestFetchFailure:
    def test_fetch_error_returns_502(self, sb_and_client, monkeypatch):
        _, client = sb_and_client

        def _boom(lbusername):
            raise user_rss.RssFetchError(lbusername, 403, "RSS returned HTTP 403")

        monkeypatch.setattr(user_rss, "_fetch_rss", _boom)
        r = client.post(
            "/refresh-user", headers=_auth(), json={"lbusername": "private-user"}
        )
        assert r.status_code == 502

    def test_block_served_with_200_returns_502_not_empty_success(
        self, sb_and_client, monkeypatch
    ):
        # A Cloudflare interstitial delivered as HTTP 200 must surface as a
        # gateway failure, not a silent upserted=0 success.
        sb, client = sb_and_client
        html = "<html><head><title>Just a moment...</title></head></html>"
        monkeypatch.setattr(user_rss, "_fetch_rss", lambda lbusername: html)
        r = client.post(
            "/refresh-user", headers=_auth(), json={"lbusername": "tester"}
        )
        assert r.status_code == 502
        assert sb.tables["UserFilms"] == []

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

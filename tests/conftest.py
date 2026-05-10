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
def client() -> TestClient:
    app.dependency_overrides[get_settings] = _override_settings
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.clear()

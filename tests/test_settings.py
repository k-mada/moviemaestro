import pytest
from pydantic import ValidationError

from app.settings import Settings, get_settings


def test_settings_loads_from_env(monkeypatch):
    monkeypatch.setenv("SUPABASE_URL", "https://x.supabase.co")
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "k")
    monkeypatch.setenv("WORKER_SHARED_SECRET", "s")
    s = Settings()
    assert s.supabase_url == "https://x.supabase.co"
    assert s.supabase_service_role_key == "k"
    assert s.worker_shared_secret == "s"


def test_settings_case_insensitive(monkeypatch):
    monkeypatch.setenv("supabase_url", "https://lower.supabase.co")
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "k")
    monkeypatch.setenv("WORKER_SHARED_SECRET", "s")
    s = Settings()
    assert s.supabase_url == "https://lower.supabase.co"


def test_settings_missing_required_raises(monkeypatch, tmp_path):
    # Clear any inherited env + point env_file at an empty dir so .env
    # discovery can't satisfy the requirement either.
    for k in ("SUPABASE_URL", "SUPABASE_SERVICE_ROLE_KEY", "WORKER_SHARED_SECRET"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ValidationError):
        Settings()


def test_get_settings_is_cached(monkeypatch):
    monkeypatch.setenv("SUPABASE_URL", "https://x.supabase.co")
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "k")
    monkeypatch.setenv("WORKER_SHARED_SECRET", "s")
    get_settings.cache_clear()
    a = get_settings()
    b = get_settings()
    assert a is b
    get_settings.cache_clear()

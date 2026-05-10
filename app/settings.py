from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    supabase_url: str
    supabase_service_role_key: str
    worker_shared_secret: str


@lru_cache
def get_settings() -> Settings:
    return Settings()

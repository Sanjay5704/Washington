"""Settings read from environment variables (populated from .env by docker compose)."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value.strip().strip('"')


@dataclass(frozen=True)
class Settings:
    sec_user_agent: str
    sec_max_rps: float
    pg_host: str
    pg_port: int
    pg_db: str
    pg_user: str
    pg_password: str
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    bronze_bucket: str
    telegram_bot_token: str
    telegram_chat_id: str

    @property
    def pg_dsn(self) -> str:
        return (
            f"host={self.pg_host} port={self.pg_port} dbname={self.pg_db} "
            f"user={self.pg_user} password={self.pg_password}"
        )

    @property
    def telegram_enabled(self) -> bool:
        return bool(self.telegram_bot_token and self.telegram_chat_id)


def load_settings() -> Settings:
    return Settings(
        sec_user_agent=_env("SEC_USER_AGENT", ""),
        sec_max_rps=float(_env("SEC_MAX_REQUESTS_PER_SECOND", "6")),
        pg_host=_env("POSTGRES_HOST", "localhost"),
        pg_port=int(_env("POSTGRES_PORT", "5432")),
        pg_db=_env("POSTGRES_DB", "washington"),
        pg_user=_env("POSTGRES_USER", "washington"),
        pg_password=_env("POSTGRES_PASSWORD", ""),
        minio_endpoint=_env("MINIO_ENDPOINT", "http://localhost:9000"),
        minio_access_key=_env("MINIO_ROOT_USER", ""),
        minio_secret_key=_env("MINIO_ROOT_PASSWORD", ""),
        bronze_bucket=_env("BRONZE_BUCKET", "bronze"),
        telegram_bot_token=_env("TELEGRAM_BOT_TOKEN", ""),
        telegram_chat_id=_env("TELEGRAM_CHAT_ID", ""),
    )

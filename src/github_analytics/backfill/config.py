"""Runtime configuration for bounded GitHub history backfills."""

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class BackfillSettings(BaseSettings):
    """Validated GitHub and PostgreSQL settings for the backfill package."""

    model_config = SettingsConfigDict(env_prefix="BACKFILL_", extra="ignore")

    database_url: SecretStr = SecretStr(
        "postgresql://github_analytics:local_only_change_me@localhost:55432/github_analytics"
    )
    database_pool_timeout_seconds: float = Field(default=5.0, gt=0, le=30)
    github_token: SecretStr
    github_repository: str = Field(pattern=r"^[^/\s]+/[^/\s]+$")
    github_repository_id: int = Field(gt=0)
    github_installation_id: int = Field(gt=0)
    github_graphql_url: str = Field(default="https://api.github.com/graphql", min_length=1)
    github_rest_url: str = Field(default="https://api.github.com", min_length=1)
    github_api_version: str = Field(default="2026-03-10", min_length=1)
    github_page_size: int = Field(default=50, ge=1, le=100)
    github_request_timeout_seconds: float = Field(default=20.0, gt=0, le=60)
    github_max_rate_limit_retries: int = Field(default=3, ge=0, le=10)
    github_secondary_backoff_seconds: float = Field(default=60.0, ge=60, le=600)

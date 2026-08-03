"""Validated runtime configuration for scheduled analytics refreshes."""

from pathlib import Path

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class AnalyticsRefreshSettings(BaseSettings):
    """PostgreSQL, source, dbt, and artifact settings used by the refresh package."""

    model_config = SettingsConfigDict(env_prefix="ANALYTICS_REFRESH_", extra="ignore")

    database_url: SecretStr = SecretStr(
        "postgresql://github_analytics:local_only_change_me@localhost:55432/github_analytics"
    )
    database_pool_timeout_seconds: float = Field(default=5.0, gt=0, le=30)
    source_schema: str = Field(default="raw", pattern=r"^[a-z_][a-z0-9_]*$")
    source_identifier: str = Field(default="github_events", pattern=r"^[a-z_][a-z0-9_]*$")
    dbt_executable: str = Field(default="dbt", min_length=1)
    dbt_project_dir: Path = Path("dbt/github_analytics")
    dbt_profiles_dir: Path = Path("dbt/github_analytics")
    dbt_target_dir: Path = Path("dbt/github_analytics/target/analytics_refresh")
    dbt_command_timeout_seconds: float = Field(default=900.0, gt=0, le=3600)
    artifact_max_failures: int = Field(default=10, ge=1, le=25)
    artifact_message_max_chars: int = Field(default=500, ge=100, le=1000)

    @field_validator("dbt_project_dir", "dbt_profiles_dir", "dbt_target_dir")
    @classmethod
    def path_must_not_be_empty(cls, value: Path) -> Path:
        """Reject empty/current-directory values that hide a missing setting."""

        if str(value).strip() in {"", "."}:
            raise ValueError("dbt path must identify an explicit directory")
        return value

    @property
    def source_relation(self) -> str:
        """Return the validated source relation used by both SQL and dbt vars."""

        return f"{self.source_schema}.{self.source_identifier}"

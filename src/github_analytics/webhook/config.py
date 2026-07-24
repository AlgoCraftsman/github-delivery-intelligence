"""Runtime configuration for the GitHub webhook receiver."""

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class WebhookSettings(BaseSettings):
    """Validated settings loaded from the process environment."""

    model_config = SettingsConfigDict(env_prefix="GITHUB_", extra="ignore")

    webhook_secret: SecretStr = Field(min_length=1)
    webhook_max_body_bytes: int = Field(default=1_048_576, gt=0)
    kafka_bootstrap_servers: str = Field(default="localhost:9092", min_length=1)
    kafka_raw_topic: str = Field(default="github.events.raw.v1", min_length=1)
    kafka_publish_timeout_seconds: float = Field(default=5.0, gt=0, le=10)
    kafka_readiness_timeout_seconds: float = Field(default=1.0, gt=0, le=5)

"""Runtime configuration for the independent pull-request monitor."""

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class PrMonitorSettings(BaseSettings):
    """Validated settings for PR projection, stale sweeps, and Kafka offsets."""

    model_config = SettingsConfigDict(env_prefix="PR_MONITOR_", extra="ignore")

    database_url: SecretStr = SecretStr(
        "postgresql://github_analytics:local_only_change_me@localhost:55432/github_analytics"
    )
    database_pool_timeout_seconds: float = Field(default=5.0, gt=0, le=30)
    kafka_bootstrap_servers: str = Field(default="localhost:9092", min_length=1)
    kafka_raw_topic: str = Field(default="github.events.raw.v1", min_length=1)
    kafka_dlq_topic: str = Field(default="github.events.dlq.v1", min_length=1)
    kafka_group_id: str = Field(default="pr-monitor", min_length=1)
    kafka_poll_timeout_seconds: float = Field(default=1.0, gt=0, le=10)
    kafka_dlq_publish_timeout_seconds: float = Field(default=5.0, gt=0, le=10)
    stale_after_hours: float = Field(default=24.0, gt=0)
    stale_sweep_interval_seconds: float = Field(default=60.0, gt=0, le=3600)

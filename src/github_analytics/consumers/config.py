"""Runtime configuration for the raw warehouse writer."""

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class WarehouseSettings(BaseSettings):
    """Validated settings for PostgreSQL landing and Kafka offset handling."""

    model_config = SettingsConfigDict(env_prefix="WAREHOUSE_", extra="ignore")

    database_url: SecretStr = SecretStr(
        "postgresql://github_analytics:local_only_change_me@localhost:55432/github_analytics"
    )
    database_pool_timeout_seconds: float = Field(default=5.0, gt=0, le=30)
    kafka_bootstrap_servers: str = Field(default="localhost:9092", min_length=1)
    kafka_raw_topic: str = Field(default="github.events.raw.v1", min_length=1)
    kafka_dlq_topic: str = Field(default="github.events.dlq.v1", min_length=1)
    kafka_group_id: str = Field(default="warehouse-writer", min_length=1)
    kafka_poll_timeout_seconds: float = Field(default=1.0, gt=0, le=10)
    kafka_dlq_publish_timeout_seconds: float = Field(default=5.0, gt=0, le=10)

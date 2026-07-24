"""Publisher boundary that Day 3 will implement with Kafka acknowledgements."""

from typing import Protocol

from github_analytics.webhook.models import GitHubEventEnvelope


class PublishError(Exception):
    """Raised when an envelope has not reached the durable publish boundary."""


class EnvelopePublisher(Protocol):
    """Publish an envelope, returning only after durable acknowledgement."""

    async def publish(self, envelope: GitHubEventEnvelope) -> None:
        """Publish one validated event or raise ``PublishError``."""

    async def is_ready(self) -> bool:
        """Return whether the publisher's required dependency is reachable."""


class UnconfiguredPublisher:
    """Fail closed until the Day 3 Kafka publisher is configured."""

    async def publish(self, envelope: GitHubEventEnvelope) -> None:
        del envelope
        raise PublishError("envelope publisher is not configured")

    async def is_ready(self) -> bool:
        return False

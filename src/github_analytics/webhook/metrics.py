"""Prometheus metrics owned by one webhook application instance."""

from prometheus_client import CollectorRegistry, Counter, Histogram, generate_latest


class WebhookMetrics:
    """Low-cardinality receiver and durable-publish measurements."""

    def __init__(self) -> None:
        self.registry = CollectorRegistry()
        self.requests = Counter(
            "github_webhook_requests_total",
            "GitHub webhook requests by receiver outcome.",
            ("outcome",),
            registry=self.registry,
        )
        self.publishes = Counter(
            "github_webhook_publish_total",
            "GitHub event publish attempts by acknowledgement outcome.",
            ("outcome",),
            registry=self.registry,
        )
        self.publish_duration = Histogram(
            "github_webhook_publish_duration_seconds",
            "Time spent awaiting the durable event publish boundary.",
            registry=self.registry,
        )

    def render(self) -> bytes:
        """Render this app instance's metrics in Prometheus text format."""

        return generate_latest(self.registry)

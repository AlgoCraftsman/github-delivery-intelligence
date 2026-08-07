"""Application service used by Airflow tasks and failure callbacks."""

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from psycopg_pool import ConnectionPool

from github_analytics.analytics_refresh.config import AnalyticsRefreshSettings
from github_analytics.analytics_refresh.models import RefreshError, RefreshRunIdentity
from github_analytics.analytics_refresh.runner import DbtRunner, default_executor, sanitize_error
from github_analytics.analytics_refresh.storage import PostgresAnalyticsRefreshStorage

Clock = Callable[[], datetime]


class AnalyticsRefreshService:
    """Coordinate durable source, build, success, and failure steps."""

    def __init__(
        self,
        storage: PostgresAnalyticsRefreshStorage,
        runner: DbtRunner,
        settings: AnalyticsRefreshSettings,
        *,
        clock: Clock,
    ) -> None:
        self._storage = storage
        self._runner = runner
        self._settings = settings
        self._clock = clock

    def record_and_check_source(self, identity: RefreshRunIdentity) -> None:
        """Start idempotently, capture the watermark, and run dbt freshness."""

        started_at = self._clock()
        run_id = self._storage.begin_run(
            identity,
            started_at=started_at,
            source_relation=self._settings.source_relation,
        )
        try:
            watermark = self._storage.read_source_watermark(
                schema=self._settings.source_schema,
                identifier=self._settings.source_identifier,
                observed_at=started_at,
            )
            self._storage.record_watermark(run_id, watermark)
            summary = self._runner.run_freshness()
            self._storage.record_source(run_id, summary)
        except Exception as error:
            if isinstance(error, RefreshError) and error.summary is not None:
                self._storage.record_source(run_id, error.summary)
            self._persist_failure(run_id, error, include_result_summary=False)
            raise

    def build(self, identity: RefreshRunIdentity) -> None:
        """Run the contracted dbt build and persist its bounded result summary."""

        try:
            self._storage.resume_build(identity.run_id)
            summary = self._runner.run_build()
            self._storage.record_build(identity.run_id, summary)
        except Exception as error:
            self._persist_failure(identity.run_id, error)
            raise

    def succeed(self, identity: RefreshRunIdentity) -> None:
        """Mark the already-built run successful."""

        self._storage.finish_success(identity.run_id, finished_at=self._clock())

    def fail(self, identity: RefreshRunIdentity, error: BaseException) -> None:
        """Idempotently persist a terminal failure for an Airflow callback."""

        self._persist_failure(identity.run_id, error)

    def _persist_failure(
        self,
        run_id: str,
        error: BaseException,
        *,
        include_result_summary: bool = True,
    ) -> None:
        category = error.category if isinstance(error, RefreshError) else "unexpected_error"
        summary = (
            error.summary if include_result_summary and isinstance(error, RefreshError) else None
        )
        self._storage.finish_failure(
            run_id,
            finished_at=self._clock(),
            category=category,
            error_summary=sanitize_error(str(error)),
            summary=summary,
        )


def run_source_step(identity: RefreshRunIdentity) -> None:
    """Build runtime dependencies and execute the first scheduled step."""

    _with_service(lambda service: service.record_and_check_source(identity))


def run_build_step(identity: RefreshRunIdentity) -> None:
    """Build runtime dependencies and execute the dbt build step."""

    _with_service(lambda service: service.build(identity))


def run_success_step(identity: RefreshRunIdentity) -> None:
    """Build runtime dependencies and persist terminal success."""

    _with_service(lambda service: service.succeed(identity))


def run_failure_step(identity: RefreshRunIdentity, error: BaseException) -> None:
    """Build runtime dependencies and persist terminal failure."""

    _with_service(lambda service: service.fail(identity, error))


def _with_service(operation: Callable[[AnalyticsRefreshService], None]) -> None:
    settings = AnalyticsRefreshSettings()
    with ConnectionPool[Any](
        settings.database_url.get_secret_value(),
        min_size=1,
        max_size=1,
        timeout=settings.database_pool_timeout_seconds,
        open=True,
    ) as pool:
        storage = PostgresAnalyticsRefreshStorage(pool)
        runner = DbtRunner(settings, default_executor)
        operation(
            AnalyticsRefreshService(storage, runner, settings, clock=lambda: datetime.now(UTC))
        )

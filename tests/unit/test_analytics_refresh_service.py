"""Tests for refresh-step ordering, failure durability, and runtime wiring."""

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from github_analytics.analytics_refresh.config import AnalyticsRefreshSettings
from github_analytics.analytics_refresh.models import (
    DbtResultSummary,
    RefreshError,
    RefreshRunIdentity,
    SourceWatermark,
)
from github_analytics.analytics_refresh.service import (
    AnalyticsRefreshService,
    run_build_step,
    run_failure_step,
    run_source_step,
    run_success_step,
)

NOW = datetime(2026, 8, 2, 12, tzinfo=UTC)


def _identity() -> RefreshRunIdentity:
    return RefreshRunIdentity(
        "analytics_refresh", "scheduled__one", NOW, NOW, NOW + timedelta(hours=1)
    )


class FakeStorage:
    def __init__(self) -> None:
        self.calls: list[tuple[Any, ...]] = []

    def begin_run(
        self, identity: RefreshRunIdentity, *, started_at: datetime, source_relation: str
    ) -> str:
        self.calls.append(("begin", identity, started_at, source_relation))
        return identity.run_id

    def read_source_watermark(
        self, *, schema: str, identifier: str, observed_at: datetime
    ) -> SourceWatermark:
        self.calls.append(("watermark", schema, identifier, observed_at))
        return SourceWatermark(NOW, 0)

    def record_watermark(self, run_id: str, watermark: SourceWatermark) -> None:
        self.calls.append(("record_watermark", run_id, watermark))

    def record_source(self, run_id: str, summary: DbtResultSummary) -> None:
        self.calls.append(("record_source", run_id, summary))

    def record_build(self, run_id: str, summary: DbtResultSummary) -> None:
        self.calls.append(("record_build", run_id, summary))

    def resume_build(self, run_id: str) -> None:
        self.calls.append(("resume_build", run_id))

    def finish_success(self, run_id: str, *, finished_at: datetime) -> None:
        self.calls.append(("succeed", run_id, finished_at))

    def finish_failure(self, run_id: str, **kwargs: Any) -> None:
        self.calls.append(("fail", run_id, kwargs))


class FakeRunner:
    def __init__(self, error: BaseException | None = None) -> None:
        self.error = error
        self.calls: list[str] = []
        self.summary = DbtResultSummary("inv", succeeded=1, artifact={"safe": True})

    def run_freshness(self) -> DbtResultSummary:
        self.calls.append("freshness")
        if self.error:
            raise self.error
        return self.summary

    def run_build(self) -> DbtResultSummary:
        self.calls.append("build")
        if self.error:
            raise self.error
        return self.summary


def _service(storage: FakeStorage, runner: FakeRunner) -> AnalyticsRefreshService:
    return AnalyticsRefreshService(
        storage,  # type: ignore[arg-type]
        runner,  # type: ignore[arg-type]
        AnalyticsRefreshSettings(),
        clock=lambda: NOW,
    )


def test_source_build_and_success_steps_persist_in_order() -> None:
    storage = FakeStorage()
    runner = FakeRunner()
    service = _service(storage, runner)

    service.record_and_check_source(_identity())
    service.build(_identity())
    service.succeed(_identity())

    assert [call[0] for call in storage.calls] == [
        "begin",
        "watermark",
        "record_watermark",
        "record_source",
        "resume_build",
        "record_build",
        "succeed",
    ]
    assert runner.calls == ["freshness", "build"]


def test_build_failure_is_persisted_and_reraised() -> None:
    category = "dbt_build_failed"
    summary = DbtResultSummary("failed-invocation", failed=1, artifact={"safe": True})
    error = RefreshError(category, "password=hunter2 failed", summary=summary)
    storage = FakeStorage()
    service = _service(storage, FakeRunner(error))

    with pytest.raises(RefreshError) as captured:
        service.build(_identity())

    assert captured.value is error
    failure = storage.calls[-1]
    assert failure[0] == "fail"
    assert failure[2]["category"] == category
    assert failure[2]["error_summary"] == "password=[REDACTED] failed"
    assert failure[2]["summary"] is summary


def test_freshness_failure_persists_its_summary_in_the_source_field() -> None:
    summary = DbtResultSummary("freshness-invocation", errors=1, artifact={"source": True})
    error = RefreshError("source_freshness_failed", "stale", summary=summary)
    storage = FakeStorage()

    with pytest.raises(RefreshError):
        _service(storage, FakeRunner(error)).record_and_check_source(_identity())

    assert [call[0] for call in storage.calls[-2:]] == ["record_source", "fail"]
    assert storage.calls[-2][2] is summary
    assert storage.calls[-1][2]["summary"] is None


def test_freshness_failure_without_an_artifact_still_becomes_terminal() -> None:
    storage = FakeStorage()

    with pytest.raises(RuntimeError, match="connection failed"):
        _service(storage, FakeRunner(RuntimeError("connection failed"))).record_and_check_source(
            _identity()
        )

    assert storage.calls[-1][0] == "fail"
    assert all(call[0] != "record_source" for call in storage.calls)


def test_unexpected_callback_failure_is_bounded_and_categorized() -> None:
    storage = FakeStorage()
    service = _service(storage, FakeRunner())

    service.fail(_identity(), RuntimeError("x" * 2000))

    failure = storage.calls[-1][2]
    assert failure["category"] == "unexpected_error"
    assert len(failure["error_summary"]) == 1000
    assert failure["summary"] is None


def test_build_retry_resumes_the_same_row_before_invoking_dbt() -> None:
    storage = FakeStorage()
    runner = FakeRunner()

    _service(storage, runner).build(_identity())

    assert [call[0] for call in storage.calls] == ["resume_build", "record_build"]


class FakePool:
    def __class_getitem__(cls, item: Any) -> type["FakePool"]:
        del item
        return cls

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.args = args
        self.kwargs = kwargs

    def __enter__(self) -> "FakePool":
        return self

    def __exit__(self, *args: Any) -> None:
        del args


def test_runtime_wrappers_delegate_with_validated_dependencies(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from github_analytics.analytics_refresh import service as module

    operations: list[str] = []

    class FakeRuntimeService:
        def __init__(self, storage: Any, runner: Any, settings: Any, *, clock: Any) -> None:
            del storage, runner, settings
            assert clock() is not None

        def record_and_check_source(self, identity: RefreshRunIdentity) -> None:
            operations.append(f"source:{identity.run_id}")

        def build(self, identity: RefreshRunIdentity) -> None:
            operations.append(f"build:{identity.run_id}")

        def succeed(self, identity: RefreshRunIdentity) -> None:
            operations.append(f"success:{identity.run_id}")

        def fail(self, identity: RefreshRunIdentity, error: BaseException) -> None:
            operations.append(f"failure:{identity.run_id}:{error}")

    settings = AnalyticsRefreshSettings(
        dbt_project_dir=tmp_path / "project",
        dbt_profiles_dir=tmp_path / "profiles",
        dbt_target_dir=tmp_path / "target",
    )
    monkeypatch.setattr(module, "AnalyticsRefreshSettings", lambda: settings)
    monkeypatch.setattr(module, "ConnectionPool", FakePool)
    monkeypatch.setattr(module, "PostgresAnalyticsRefreshStorage", lambda pool: object())
    monkeypatch.setattr(module, "DbtRunner", lambda configured, executor: object())
    monkeypatch.setattr(module, "AnalyticsRefreshService", FakeRuntimeService)

    run_source_step(_identity())
    run_build_step(_identity())
    run_success_step(_identity())
    run_failure_step(_identity(), RuntimeError("broken"))

    assert [item.split(":", 1)[0] for item in operations] == [
        "source",
        "build",
        "success",
        "failure",
    ]

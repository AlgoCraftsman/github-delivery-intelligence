"""Tests for bounded Day 13 evidence contracts and rendering."""

from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from github_analytics.evidence import (
    EvidenceEnvironment,
    EvidenceReport,
    EvidenceResult,
    EvidenceStatus,
    TimingSummary,
    render_markdown,
    summarize_timings,
    write_report,
)


def _environment() -> EvidenceEnvironment:
    return EvidenceEnvironment(
        name="local Docker workstation",
        operating_system="Windows 11",
        architecture="amd64",
        logical_cpus=8,
        memory_gib=12.5,
        python_version="3.12.13",
        uv_version="0.11.29",
        docker_desktop_version="4.84.0",
        docker_engine_version="29.6.2",
        docker_compose_version="5.3.1",
        kafka_image="apache/kafka:4.3.1",
        postgres_image="postgres:17.10-bookworm",
    )


def _report(*results: EvidenceResult) -> EvidenceReport:
    return EvidenceReport(
        generated_at=datetime(2026, 8, 17, 22, tzinfo=UTC),
        git_revision="a" * 40,
        git_worktree_dirty=True,
        workload="500 signed fixture deliveries with 25 concurrent requests",
        environment=_environment(),
        results=results or (_passed_result(),),
    )


def _passed_result() -> EvidenceResult:
    return EvidenceResult(
        name="receiver burst",
        status=EvidenceStatus.PASSED,
        acceptance="all acknowledged deliveries reach Kafka",
        observed="500 of 500 requests returned 202",
        measurements={"accepted": 500, "lost": 0, "healthy": True},
        timing_scope="receiver HTTP through Kafka acknowledgement",
        timing=summarize_timings([0.001, 0.002, 0.003, 0.004], wall_seconds=0.01),
    )


def test_timing_summary_uses_nearest_rank_percentiles() -> None:
    summary = summarize_timings([0.004, 0.001, 0.003, 0.002], wall_seconds=0.01)

    assert summary == TimingSummary(
        sample_count=4,
        wall_seconds=0.01,
        throughput_per_second=400.0,
        p50_milliseconds=2.0,
        p95_milliseconds=4.0,
        maximum_milliseconds=4.0,
    )


@pytest.mark.parametrize(
    ("samples", "wall_seconds", "message"),
    [
        ([], 1.0, "at least one"),
        ([0.1], 0.0, "wall_seconds"),
        ([float("inf")], 1.0, "samples"),
        ([-0.1], 1.0, "samples"),
    ],
)
def test_timing_summary_rejects_invalid_observations(
    samples: list[float],
    wall_seconds: float,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        summarize_timings(samples, wall_seconds=wall_seconds)


@pytest.mark.parametrize(
    "arguments",
    [
        {"p50_milliseconds": 3, "p95_milliseconds": 2, "maximum_milliseconds": 4},
        {
            "p50_milliseconds": 1,
            "p95_milliseconds": 2,
            "maximum_milliseconds": float("inf"),
        },
    ],
)
def test_timing_contract_rejects_impossible_summaries(arguments: dict[str, float]) -> None:
    with pytest.raises(ValidationError):
        TimingSummary(
            sample_count=1,
            wall_seconds=1,
            throughput_per_second=1,
            **arguments,
        )


@pytest.mark.parametrize(
    ("measurements", "message"),
    [
        ({f"field-{index}": index for index in range(17)}, "16 fields"),
        ({"": 1}, "measurement names"),
        ({"x" * 65: 1}, "measurement names"),
        ({"detail": "x" * 201}, "strings"),
        ({"latency": float("nan")}, "finite"),
    ],
)
def test_result_contract_bounds_measurements(
    measurements: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValidationError, match=message):
        EvidenceResult(
            name="bounded evidence",
            status=EvidenceStatus.PASSED,
            acceptance="bounded",
            observed="bounded",
            measurements=measurements,  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    ("timing_scope", "timing"),
    [
        ("receiver", None),
        (None, summarize_timings([0.001], wall_seconds=0.001)),
    ],
)
def test_result_requires_timing_and_scope_together(
    timing_scope: str | None,
    timing: TimingSummary | None,
) -> None:
    with pytest.raises(ValidationError, match="provided together"):
        EvidenceResult(
            name="scoped timing",
            status=EvidenceStatus.PASSED,
            acceptance="timing is unambiguous",
            observed="scope and measurements are paired",
            timing_scope=timing_scope,
            timing=timing,
        )


def test_report_rejects_duplicate_result_names() -> None:
    result = _passed_result()

    with pytest.raises(ValidationError, match="unique"):
        _report(result, result)


def test_markdown_renders_exact_status_environment_and_timings() -> None:
    unavailable = EvidenceResult(
        name="live | GitHub lifecycle",
        status=EvidenceStatus.UNAVAILABLE,
        acceptance="observe a real PR lifecycle",
        observed="not run\nwithout a configured GitHub App",
    )

    rendered = render_markdown(_report(_passed_result(), unavailable))

    assert "# Day 13 replay, failure-drill, and benchmark evidence" in rendered
    assert "| Logical CPUs available to Docker | 8 |" in rendered
    assert "throughput=400.000/s" in rendered
    assert 'timing_scope="receiver HTTP through Kafka acknowledgement"' in rendered
    assert "live \\| GitHub lifecycle" in rendered
    assert "not run without a configured GitHub App" in rendered
    assert "not a production" in rendered
    assert "with a dirty working tree" in rendered


def test_write_report_creates_matching_json_and_markdown(tmp_path: Path) -> None:
    report = _report()
    json_path = tmp_path / "nested" / "evidence.json"
    markdown_path = tmp_path / "docs" / "evidence.md"

    write_report(report, json_path=json_path, markdown_path=markdown_path)

    assert EvidenceReport.model_validate_json(json_path.read_text(encoding="utf-8")) == report
    assert markdown_path.read_text(encoding="utf-8") == render_markdown(report)

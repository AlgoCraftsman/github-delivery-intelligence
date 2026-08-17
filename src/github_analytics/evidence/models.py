"""Bounded, machine-readable Day 13 evidence and Markdown rendering."""

import json
import math
from collections.abc import Sequence
from enum import StrEnum
from pathlib import Path

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

MetricValue = bool | int | float | str | None


class EvidenceStatus(StrEnum):
    """Whether one evidence claim was observed, disproved, or not exercised."""

    PASSED = "passed"
    FAILED = "failed"
    UNAVAILABLE = "unavailable"


class TimingSummary(BaseModel):
    """Nearest-rank timing summary for one named workload."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    sample_count: int = Field(gt=0)
    wall_seconds: float = Field(gt=0)
    throughput_per_second: float = Field(ge=0)
    p50_milliseconds: float = Field(ge=0)
    p95_milliseconds: float = Field(ge=0)
    maximum_milliseconds: float = Field(ge=0)

    @model_validator(mode="after")
    def validate_ordering(self) -> "TimingSummary":
        """Reject summaries whose percentiles cannot describe real samples."""

        values = (
            self.wall_seconds,
            self.throughput_per_second,
            self.p50_milliseconds,
            self.p95_milliseconds,
            self.maximum_milliseconds,
        )
        if not all(math.isfinite(value) for value in values):
            raise ValueError("timing values must be finite")
        if not self.p50_milliseconds <= self.p95_milliseconds <= self.maximum_milliseconds:
            raise ValueError("timing percentiles must be ordered")
        return self


class EvidenceResult(BaseModel):
    """One bounded, reviewer-facing validation result."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str = Field(min_length=1, max_length=80)
    status: EvidenceStatus
    acceptance: str = Field(min_length=1, max_length=240)
    observed: str = Field(min_length=1, max_length=500)
    measurements: dict[str, MetricValue] = Field(default_factory=dict)
    timing_scope: str | None = Field(default=None, min_length=1, max_length=120)
    timing: TimingSummary | None = None

    @model_validator(mode="after")
    def validate_measurements(self) -> "EvidenceResult":
        """Keep arbitrary evidence fields small enough for durable review."""

        if len(self.measurements) > 16:
            raise ValueError("evidence measurements are limited to 16 fields")
        for key, value in self.measurements.items():
            if not key.strip() or len(key) > 64:
                raise ValueError("measurement names must be nonempty and at most 64 characters")
            if isinstance(value, str) and len(value) > 200:
                raise ValueError("measurement strings are limited to 200 characters")
            if isinstance(value, float) and not math.isfinite(value):
                raise ValueError("measurement floats must be finite")
        if (self.timing is None) != (self.timing_scope is None):
            raise ValueError("timing and timing_scope must be provided together")
        return self


class EvidenceEnvironment(BaseModel):
    """Non-secret environment identity required to interpret local measurements."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str = Field(min_length=1, max_length=120)
    operating_system: str = Field(min_length=1, max_length=200)
    architecture: str = Field(min_length=1, max_length=40)
    logical_cpus: int = Field(gt=0)
    memory_gib: float = Field(gt=0)
    python_version: str = Field(min_length=1, max_length=40)
    uv_version: str = Field(min_length=1, max_length=80)
    docker_desktop_version: str = Field(min_length=1, max_length=120)
    docker_engine_version: str = Field(min_length=1, max_length=40)
    docker_compose_version: str = Field(min_length=1, max_length=80)
    kafka_image: str = Field(min_length=1, max_length=200)
    postgres_image: str = Field(min_length=1, max_length=200)


class EvidenceReport(BaseModel):
    """Versioned Day 13 report containing only observed or explicit unavailable claims."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: int = Field(default=1, ge=1)
    generated_at: AwareDatetime
    git_revision: str = Field(pattern=r"^[0-9a-f]{40}$")
    git_worktree_dirty: bool
    workload: str = Field(min_length=1, max_length=300)
    environment: EvidenceEnvironment
    results: tuple[EvidenceResult, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_result_names(self) -> "EvidenceReport":
        """Make every evidence row addressable without ambiguous duplicate names."""

        names = [result.name for result in self.results]
        if len(names) != len(set(names)):
            raise ValueError("evidence result names must be unique")
        return self


def summarize_timings(
    samples_seconds: Sequence[float],
    *,
    wall_seconds: float,
) -> TimingSummary:
    """Summarize nonnegative seconds using deterministic nearest-rank percentiles."""

    if not samples_seconds:
        raise ValueError("at least one timing sample is required")
    if not math.isfinite(wall_seconds) or wall_seconds <= 0:
        raise ValueError("wall_seconds must be finite and positive")
    if any(not math.isfinite(sample) or sample < 0 for sample in samples_seconds):
        raise ValueError("timing samples must be finite and nonnegative")

    ordered = sorted(samples_seconds)
    return TimingSummary(
        sample_count=len(ordered),
        wall_seconds=round(wall_seconds, 6),
        throughput_per_second=round(len(ordered) / wall_seconds, 3),
        p50_milliseconds=round(_nearest_rank(ordered, 0.50) * 1_000, 3),
        p95_milliseconds=round(_nearest_rank(ordered, 0.95) * 1_000, 3),
        maximum_milliseconds=round(ordered[-1] * 1_000, 3),
    )


def render_markdown(report: EvidenceReport) -> str:
    """Render the exact report data as a concise reviewer-facing evidence table."""

    environment = report.environment
    lines = [
        "# Day 13 replay, failure-drill, and benchmark evidence",
        "",
        (
            f"Generated at `{report.generated_at.isoformat()}` from commit "
            f"`{report.git_revision}` with a "
            f"{'dirty' if report.git_worktree_dirty else 'clean'} working tree."
        ),
        "",
        "This report records a local single-broker Docker Desktop run. It demonstrates",
        "at-least-once processing with idempotent durable effects; it is not a production",
        "capacity or high-availability claim.",
        "",
        "## Workload",
        "",
        _escape_markdown(report.workload),
        "",
        "## Environment",
        "",
        "| Field | Observed value |",
        "|---|---|",
    ]
    environment_rows = (
        ("Name", environment.name),
        ("Operating system", environment.operating_system),
        ("Architecture", environment.architecture),
        ("Logical CPUs available to Docker", str(environment.logical_cpus)),
        ("Memory available to Docker", f"{environment.memory_gib:.2f} GiB"),
        ("Python", environment.python_version),
        ("uv", environment.uv_version),
        ("Docker Desktop", environment.docker_desktop_version),
        ("Docker Engine", environment.docker_engine_version),
        ("Docker Compose", environment.docker_compose_version),
        ("Kafka image", environment.kafka_image),
        ("PostgreSQL image", environment.postgres_image),
    )
    lines.extend(
        f"| {_escape_markdown(field)} | {_escape_markdown(value)} |"
        for field, value in environment_rows
    )
    lines.extend(
        [
            "",
            "## Evidence",
            "",
            "| Check | Status | Acceptance condition | Observed result | Measurements |",
            "|---|---|---|---|---|",
        ]
    )
    for result in report.results:
        measurements = _format_measurements(result)
        lines.append(
            "| "
            + " | ".join(
                _escape_markdown(value)
                for value in (
                    result.name,
                    result.status.value,
                    result.acceptance,
                    result.observed,
                    measurements,
                )
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "## Interpretation limits",
            "",
            "- Timings use nearest-rank p50/p95 over the named scope. Receiver timing covers",
            "  local in-process HTTP transport through real Kafka acknowledgement; warehouse",
            "  timing covers consumer processing through the durable effect and offset commit.",
            "- Failure drills stop and restart existing Compose services without deleting",
            "  containers or named volumes.",
            "- `unavailable` means the evidence was not observed; it must not be inferred",
            "  from tests, CI failures, or synthetic data.",
            "",
        ]
    )
    return "\n".join(lines)


def write_report(
    report: EvidenceReport,
    *,
    json_path: Path,
    markdown_path: Path,
) -> None:
    """Write matching machine-readable and reviewer-facing report artifacts."""

    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(report.model_dump_json(indent=2) + "\n", encoding="utf-8")
    markdown_path.write_text(render_markdown(report), encoding="utf-8")


def _nearest_rank(ordered: Sequence[float], percentile: float) -> float:
    index = max(0, math.ceil(percentile * len(ordered)) - 1)
    return ordered[index]


def _format_measurements(result: EvidenceResult) -> str:
    parts = [
        f"{key}={json.dumps(value, sort_keys=True)}"
        for key, value in sorted(result.measurements.items())
    ]
    if result.timing is not None:
        timing = result.timing
        parts.extend(
            (
                f"timing_scope={json.dumps(result.timing_scope)}",
                f"samples={timing.sample_count}",
                f"wall={timing.wall_seconds:.3f}s",
                f"throughput={timing.throughput_per_second:.3f}/s",
                f"p50={timing.p50_milliseconds:.3f}ms",
                f"p95={timing.p95_milliseconds:.3f}ms",
                f"max={timing.maximum_milliseconds:.3f}ms",
            )
        )
    return "; ".join(parts) if parts else "none"


def _escape_markdown(value: str) -> str:
    return " ".join(value.replace("|", "\\|").split())

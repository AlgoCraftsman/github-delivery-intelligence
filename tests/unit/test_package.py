import importlib
import importlib.metadata
import tomllib
from pathlib import Path

import pytest

import github_analytics

ROOT = Path(__file__).resolve().parents[2]


def test_package_version_matches_installed_metadata() -> None:
    assert github_analytics.__version__ == importlib.metadata.version(
        "github-delivery-intelligence"
    )


def test_package_has_an_honest_source_tree_version(monkeypatch: pytest.MonkeyPatch) -> None:
    def missing_distribution(distribution_name: str) -> str:
        raise importlib.metadata.PackageNotFoundError(distribution_name)

    with monkeypatch.context() as patch:
        patch.setattr(importlib.metadata, "version", missing_distribution)
        module = importlib.reload(github_analytics)
        assert module.__version__ == "0+source"

    importlib.reload(github_analytics)


def test_project_metadata_and_ci_pin_the_release_toolchain() -> None:
    configuration = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")

    assert configuration["project"]["license"] == "MIT"
    assert configuration["tool"]["uv"]["required-version"] == ">=0.12.1,<0.13"
    assert 'UV_VERSION: "0.12.5"' in workflow

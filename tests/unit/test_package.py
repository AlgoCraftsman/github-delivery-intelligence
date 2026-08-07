import importlib
import importlib.metadata

import pytest

import github_analytics


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

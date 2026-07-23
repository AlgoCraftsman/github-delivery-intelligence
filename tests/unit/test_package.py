from importlib.metadata import version

from github_analytics import __version__


def test_package_version_matches_installed_metadata() -> None:
    assert __version__ == version("github-delivery-intelligence")

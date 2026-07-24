"""Tests for GitHub webhook signature authentication."""

from github_analytics.webhook.security import has_valid_signature


def test_signature_matches_github_documented_vector() -> None:
    assert has_valid_signature(
        body=b"Hello, World!",
        secret="It's a Secret to Everybody",
        signature="sha256=757107ea0eb2509fc211221cce984b8a37570b6d7586c22c46f4379c8b043e17",
    )


def test_signature_rejects_nonmatching_digest() -> None:
    assert not has_valid_signature(
        body=b"Hello, World!",
        secret="It's a Secret to Everybody",
        signature="sha256=0000000000000000000000000000000000000000000000000000000000000000",
    )

"""Security primitives for authenticating GitHub webhook requests."""

import hashlib
import hmac


def has_valid_signature(*, body: bytes, secret: str, signature: str) -> bool:
    """Verify GitHub's HMAC-SHA256 signature using constant-time comparison."""

    expected = (
        "sha256="
        + hmac.new(
            secret.encode("utf-8"),
            msg=body,
            digestmod=hashlib.sha256,
        ).hexdigest()
    )
    return hmac.compare_digest(expected, signature)

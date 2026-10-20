"""Canonical JSON encoding and hashing.

Every persisted payload, idempotency fingerprint, source version and snapshot hash is
computed from the canonical form, so structurally equal values always produce the same
bytes and the same digest.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, TypeAlias

JsonObject: TypeAlias = dict[str, Any]
"""A decoded JSON object. Values are validated at runtime, not by the type checker."""


def canonical(value: object) -> str:
    """Serialize ``value`` as canonical JSON.

    Keys are sorted, separators carry no whitespace and non-ASCII text is kept as is.
    ``allow_nan=False`` and the absence of a ``default`` hook make NaN, Infinity and
    arbitrary Python objects raise instead of being stored in a form that cannot be
    compared or hashed reliably later.
    """
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )


def digest(value: object) -> str:
    """Return the SHA-256 hex digest of the canonical JSON form of ``value``."""
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def text_hash(text: str) -> str:
    """Return the SHA-256 hex digest of raw source text, as cited by evidence selectors."""
    return hashlib.sha256(text.encode()).hexdigest()


def utc_now() -> str:
    """Return the current time as an ISO 8601 UTC timestamp, for event records."""
    return datetime.now(timezone.utc).isoformat()

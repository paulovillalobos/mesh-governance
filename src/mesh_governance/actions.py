"""The versioned action envelope accepted by :meth:`MeshStore.execute`."""

from __future__ import annotations

from typing import Any, TypeAlias

from .encoding import JsonObject, digest

Request: TypeAlias = dict[str, Any]
"""An action envelope. Its shape is checked at runtime by :meth:`MeshStore.execute`."""

API_VERSION = "mesh.actions.v1"

ENVELOPE_FIELDS = frozenset(
    {"api_version", "action", "object_id", "expected_revision", "idempotency_key", "parameters"}
)
"""The exact top-level keys of a request. Extra keys, such as an injected role, are rejected."""

CAPABILITIES: dict[str, str] = {
    "importSourceVersion": "import",
    "proposeAssertion": "propose",
    "reviewAssertion": "review",
    "adjudicateDispute": "adjudicate",
    "correctAssertion": "correct",
    "invalidateMapping": "correct",
    "publishSnapshot": "publish",
    "rollbackPublication": "publish",
    "setSourceVisibility": "admin",
}
"""Each public action name and the capability a principal needs to run it."""

ALIASES: dict[str, str] = {
    "proposeMapping": "proposeAssertion",
    "reviewMapping": "reviewAssertion",
}
"""Legacy action names, resolved before the capability check."""


def command(
    action: str,
    oid: str,
    parameters: JsonObject,
    revision: int = 0,
    key: str | None = None,
) -> Request:
    """Build a request envelope.

    Args:
        action: Public action name (or alias), e.g. ``"reviewAssertion"``.
        oid: Identity of the object the action targets.
        parameters: Action-specific parameters.
        revision: The revision the caller expects the object to be at.
        key: Idempotency key. Defaults to a digest of the action, object, revision and
            parameters, so an identical call made twice is replayed rather than repeated.
    """
    return {
        "api_version": API_VERSION,
        "action": action,
        "object_id": oid,
        "expected_revision": revision,
        "idempotency_key": key or digest([action, oid, revision, parameters]),
        "parameters": parameters,
    }

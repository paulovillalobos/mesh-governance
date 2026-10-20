"""Contract errors and the small guard helpers used to raise them."""

from __future__ import annotations

from typing import Literal, TypeAlias, TypeGuard

ErrorCode: TypeAlias = Literal[
    "invalid_input",
    "permission",
    "version_conflict",
    "stale_dependency",
    "not_found",
    "independence",
    "quality_gate",
    "integrity",
]
"""Stable, machine-readable failure categories.

- ``invalid_input``: the request is malformed or violates a domain rule.
- ``permission``: the principal lacks a capability or may not see a source.
- ``version_conflict``: an expected revision, case revision or cutoff no longer matches,
  or an idempotency key was reused with different content.
- ``stale_dependency``: the request depends on superseded or invalidated evidence.
- ``not_found``: the named assertion or snapshot does not exist.
- ``independence``: a proposer or opinion author tried to judge their own work.
- ``quality_gate``: a reviewed publication lacks exact-revision human approval.
- ``integrity``: a stored snapshot no longer matches its recorded hash.
"""


class ContractError(Exception):
    """A request violated the store's contract.

    Raised inside :meth:`MeshStore.execute` it rolls back every write of the request.
    ``code`` is one of :data:`ErrorCode`; ``detail`` is a human-readable explanation.
    """

    def __init__(self, code: ErrorCode, detail: str) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail


def require(condition: object, detail: str, code: ErrorCode = "invalid_input") -> None:
    """Raise :class:`ContractError` with ``code`` and ``detail`` unless ``condition`` is truthy."""
    if not condition:
        raise ContractError(code, detail)


def nonempty(value: object) -> TypeGuard[str]:
    """Return whether ``value`` is a string containing at least one non-whitespace character."""
    return isinstance(value, str) and bool(value.strip())


def require_text(value: object, detail: str, code: ErrorCode = "invalid_input") -> str:
    """Return ``value`` if it is a non-empty string, otherwise raise :class:`ContractError`."""
    if nonempty(value):
        return value
    raise ContractError(code, detail)

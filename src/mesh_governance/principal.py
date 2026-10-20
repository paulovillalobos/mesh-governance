"""Identities supplied by trusted host code."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Principal:
    """An actor and the capabilities the host has granted it.

    The store never authenticates anyone and never reads roles from a request body:
    host code constructs the principal and passes it alongside each request.

    Attributes:
        id: Stable actor identity. Idempotency keys, proposer checks and review
            independence are all scoped to it.
        capabilities: Capability names such as ``"review"`` or ``"publish"``; see
            :data:`mesh_governance.actions.CAPABILITIES`. ``"read_private"`` additionally
            allows reading private sources.
        kind: What the actor is. Only ``"human"`` reviews and rulings count as human
            approval; ``"ai"``, ``"model"`` and ``"service"`` actors never do.
    """

    id: str
    capabilities: frozenset[str]
    kind: str = "service"


PUBLIC = Principal("anonymous", frozenset(), "public")
"""An unauthenticated reader: no capabilities, public sources only."""

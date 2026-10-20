"""Mesh governed local store. No inference, paid API, or production-auth claims.

Principals are supplied by trusted host code, never by an action's JSON body.
SQLite is the single-operator implementation; do not expose its file or attach
this library to an unauthenticated mutation endpoint. Published exports remain
exploratory unless their exact revisions have independent human approval.
"""

from .actions import ALIASES, API_VERSION, CAPABILITIES, Request, command
from .encoding import JsonObject, canonical, digest
from .errors import ContractError, ErrorCode
from .principal import PUBLIC, Principal
from .store import MeshStore
from .validation import DECISIONS

__all__ = [
    "ALIASES",
    "API_VERSION",
    "CAPABILITIES",
    "DECISIONS",
    "PUBLIC",
    "ContractError",
    "ErrorCode",
    "JsonObject",
    "MeshStore",
    "Principal",
    "Request",
    "canonical",
    "command",
    "digest",
]

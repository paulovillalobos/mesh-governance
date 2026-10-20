"""Stateless checks on assertion payloads, evidence selectors and time values.

These functions only look at the values passed to them. Checks that need the database
(source access, current source heads) live in :meth:`MeshStore.validate`, which calls
into this module in a fixed order so the first failing rule determines the error.
"""

from __future__ import annotations

import json
import math
from collections.abc import Container
from datetime import datetime
from typing import Any

from .encoding import JsonObject, canonical
from .errors import ContractError, require, require_text

DECISIONS = frozenset({"supported", "uncertain", "no_mapping", "insufficient_evidence"})
"""Allowed ``semantic_state`` values of an assertion."""

PROFILES = frozenset({"cordis-sdg", "observation"})
"""Registered domain profiles. Each profile adds its own predicate and object rules."""

ASSERTION_FIELDS = frozenset(
    {
        "profile",
        "subject",
        "predicate",
        "object",
        "source_version",
        "evidence",
        "semantic_state",
        "modality",
        "valid_time",
        "detail",
    }
)

TEMPORAL_KINDS = frozenset({"unknown", "not_applicable", "instant", "interval"})

SDG_MODALITIES = frozenset(
    {None, "planned", "reported_activity", "reported_result", "reported_impact"}
)

TEXT_SELECTOR_FIELDS = frozenset({"kind", "start", "end", "quote", "text_hash"})
FIELD_SELECTOR_FIELDS = frozenset({"kind", "field", "value", "text_hash"})


def parse_utc_timestamp(value: object) -> float:
    """Parse an explicit-UTC ISO 8601 timestamp and return it as POSIX seconds.

    Accepts a trailing ``Z`` or a ``+00:00`` offset. Naive timestamps are rejected
    rather than interpreted in the host time zone, so results never depend on where
    the code runs; non-zero offsets are rejected so stored boundaries stay comparable
    as written.
    """
    text = require_text(value, "Time boundary required")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ContractError("invalid_input", "Use ISO8601 timestamps") from exc
    offset = parsed.utcoffset()
    if offset is None:
        raise ContractError("invalid_input", "Time zone required")
    require(offset.total_seconds() == 0, "Use explicit UTC boundaries")
    return parsed.timestamp()


def validate_valid_time(time: Any) -> None:
    """Check an assertion's ``valid_time``.

    ``unknown`` and ``not_applicable`` carry no boundaries: an unknown validity is
    never widened to "always" or narrowed to the ingestion time. ``instant`` needs
    ``at``; ``interval`` needs a non-empty half-open range ``[from, to)``.
    """
    require(isinstance(time, dict) and time.get("kind") in TEMPORAL_KINDS, "Invalid temporal kind")
    kind = time["kind"]
    if kind == "instant":
        keys = {"kind", "at"}
    elif kind == "interval":
        keys = {"kind", "from", "to"}
    else:
        keys = {"kind"}
    require(set(time) == keys, "Unexpected temporal fields; unknown is not infinity")
    for key in keys - {"kind"}:
        parse_utc_timestamp(time[key])
    if kind == "interval":
        require(
            parse_utc_timestamp(time["from"]) < parse_utc_timestamp(time["to"]),
            "Empty/reversed interval",
        )


def temporal_state(valid_time: Any, valid_at: object) -> str:
    """Classify a stored ``valid_time`` against the query instant ``valid_at``.

    Returns ``"temporal_unknown"``, ``"applicable"`` or ``"outside_interval"``. The
    query instant is validated first, even when the stored validity is unknown.
    """
    point = parse_utc_timestamp(valid_at)
    kind = valid_time["kind"]
    if kind in {"unknown", "not_applicable"}:
        return "temporal_unknown"
    if kind == "instant":
        applicable = parse_utc_timestamp(valid_time["at"]) == point
    else:
        start = parse_utc_timestamp(valid_time["from"])
        applicable = start <= point < parse_utc_timestamp(valid_time["to"])
    return "applicable" if applicable else "outside_interval"


def check_selector(ref: Any, source: JsonObject) -> None:
    """Check that one evidence selector matches the cited source exactly.

    A ``text`` selector names a character range, the quoted text and the source text
    hash; a ``field`` selector names a top-level key of a JSON source, its value and the
    text hash. A match proves the citation is consistent with the stored source
    version, not that the claim it supports is true.
    """
    require(isinstance(ref, dict), "Invalid selector")
    if ref.get("kind") == "text":
        require(set(ref) == TEXT_SELECTOR_FIELDS, "Invalid text selector")
        start, end = ref["start"], ref["end"]
        # type() rather than isinstance(): booleans are not valid offsets.
        require(
            type(start) is int and type(end) is int and 0 <= start < end <= len(source["text"]),
            "Invalid evidence range",
        )
        require(
            source["text"][start:end] == ref["quote"] and ref["text_hash"] == source["text_hash"],
            "Evidence/hash mismatch",
        )
    elif ref.get("kind") == "field":
        require(set(ref) == FIELD_SELECTOR_FIELDS, "Invalid structured selector")
        try:
            value = json.loads(source["text"])[ref["field"]]
        except (ValueError, KeyError, TypeError) as exc:
            raise ContractError(
                "invalid_input", "Structured evidence field does not exist"
            ) from exc
        require(
            canonical(value) == canonical(ref["value"]) and ref["text_hash"] == source["text_hash"],
            "Structured evidence mismatch",
        )
    else:
        raise ContractError("invalid_input", "Unregistered evidence selector")


def check_sdg_profile(payload: JsonObject, concepts: Container[str]) -> None:
    """Rules for ``cordis-sdg``: a documented contribution to a known taxonomy concept."""
    require(payload["predicate"] == "documented_contribution", "Unregistered research predicate")
    require(
        isinstance(payload["object"], str) and payload["object"] in concepts,
        "Unknown official concept",
    )
    require(payload["modality"] in SDG_MODALITIES, "Unknown contribution modality")
    require(
        payload["semantic_state"] != "supported" or payload["modality"] is not None,
        "Supported contribution requires modality",
    )
    require(
        all(x["kind"] == "text" for x in payload["evidence"]),
        "Research profile requires text selectors",
    )


def check_observation_profile(payload: JsonObject) -> None:
    """Rules for ``observation``: a finite Celsius reading backed by a structured field."""
    require(
        payload["predicate"] == "reported_temperature"
        and payload["modality"] == "reported_measurement",
        "Unregistered observation predicate/modality",
    )
    quantity = payload["object"]
    require(
        isinstance(quantity, dict) and set(quantity) == {"value", "unit"},
        "Quantity requires value and unit",
    )
    require(
        type(quantity["value"]) in {int, float}
        and math.isfinite(quantity["value"])
        and quantity["unit"] == "Cel",
        "Only finite Celsius measurements in this profile",
    )
    require(
        any(
            x["kind"] == "field" and x["field"] == "temperature" and x["value"] == quantity["value"]
            for x in payload["evidence"]
        ),
        "Measurement must match the structured source",
    )

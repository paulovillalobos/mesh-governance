"""The governed SQLite store: source versions, assertion revisions, reviews and snapshots."""

from __future__ import annotations

import copy
import json
import os
import sqlite3
from collections.abc import Callable, Iterable, Sequence
from typing import Any, ClassVar, TypeAlias

from .actions import ALIASES, API_VERSION, CAPABILITIES, ENVELOPE_FIELDS, Request
from .encoding import JsonObject, canonical, digest, text_hash, utc_now
from .errors import ContractError, nonempty, require
from .principal import PUBLIC, Principal
from .schema import CONFIGURATION_DDL, SCHEMA, immutability_triggers
from .validation import (
    ASSERTION_FIELDS,
    DECISIONS,
    PROFILES,
    check_observation_profile,
    check_sdg_profile,
    check_selector,
    parse_utc_timestamp,
    temporal_state,
    validate_valid_time,
)

Handler: TypeAlias = "Callable[[MeshStore, Principal, Request], JsonObject]"

VISIBILITIES = frozenset({"public", "private"})
SOURCE_FIELDS = frozenset({"subject", "text", "metadata", "visibility"})
REVIEW_FIELDS = frozenset({"verdict", "dimension", "reason"})
REVIEW_VERDICTS = frozenset({"uphold", "reject", "request_evidence"})
DISPUTE_DIMENSIONS = frozenset(
    {"activity", "concept", "scope", "modality", "evidence", "time", "identity", "granularity"}
)
REVIEW_CONTEXT_FIELDS = ("independence", "entered_by")
ADJUDICATION_FIELDS = frozenset({"expected_case_revision", "opinion_ids", "outcome", "reason"})
ADJUDICATION_OUTCOMES = frozenset({"uphold", "reject", "request_evidence", "unresolved"})
ADJUDICATION_CONTEXT_FIELDS = ("entered_by",)
MANIFEST_FIELDS = frozenset(
    {"assertion_ids", "source_versions", "mode", "expected_seq", "operational_findings"}
)
PUBLICATION_MODES = frozenset({"exploratory", "reviewed"})
FINDING_STATES = frozenset({"not_assessed", "processing_error"})
MAX_IDEMPOTENCY_KEY_LENGTH = 200
TAXONOMY_TRAVERSAL_LIMIT = 10000

NAVIGATION_RULE = "R1.1"
NAVIGATION_RULE_HASH = digest({"rule": NAVIGATION_RULE, "meaning": "navigation_only_no_impact"})


def split_context(parameters: JsonObject, allowed: Iterable[str]) -> tuple[JsonObject, JsonObject]:
    """Separate optional provenance fields from action parameters.

    Returns ``(context, rest)``: the ``allowed`` keys present in ``parameters`` and
    every other key.
    """
    context = {k: parameters[k] for k in allowed if k in parameters}
    rest = {k: v for k, v in parameters.items() if k not in context}
    return context, rest


def is_valid_finding(finding: Any, sources: JsonObject) -> bool:
    """Whether an operational finding records a processing outcome, never a semantic one.

    A finding says a published source was not assessed or failed processing. It must
    leave ``semantic_decision`` empty, so an error cannot be read as "no mapping".
    """
    return bool(
        isinstance(finding, dict)
        and finding.get("source_version") in sources
        and finding.get("processing_state") in FINDING_STATES
        and finding.get("semantic_decision") is None
        and nonempty(finding.get("reason"))
    )


class MeshStore:
    """A governed local store for evidence-backed assertions.

    All mutations go through :meth:`execute`, which checks the caller's capabilities,
    applies idempotency, and runs the action in a single SQLite write transaction.
    The remaining public methods are reads.

    The store guarantees that:

    - source versions, assertion revisions, reviews, adjudications, invalidations,
      events and snapshots are append-only (enforced by triggers, see
      :mod:`mesh_governance.schema`);
    - every assertion cites the current version of its own subject's source, with
      exact quotes or field values and the source hash;
    - a proposer never reviews its own revision and an adjudicator never rules on
      its own opinion;
    - a source update marks dependent assertions stale until they are corrected;
    - a reviewed snapshot contains only revisions with exact-revision human approval.

    Principals come from trusted host code. The store does not authenticate remote
    callers and must not sit directly behind an unauthenticated endpoint.

    Args:
        path: SQLite database file, or ``":memory:"``. File databases are created
            with mode ``0600``.
        concepts: Taxonomy concepts, each a JSON object with a unique ``uri`` and an
            optional ``broader`` list of parent URIs. The taxonomy is validated before
            any file is touched and then pinned to the database by hash.

    Raises:
        ContractError: If concept URIs are duplicated, the taxonomy has a cycle or a
            dangling parent, or the database was pinned to a different taxonomy.
    """

    def __init__(self, path: str | os.PathLike[str], concepts: Sequence[JsonObject]) -> None:
        self.concepts: dict[str, JsonObject] = {x["uri"]: copy.deepcopy(x) for x in concepts}
        require(len(self.concepts) == len(concepts), "Duplicate concept identities")
        self.taxonomy_hash = digest(concepts)
        # Validate before creating a database or pinning an unusable vocabulary.
        for uri in self.concepts:
            self.ancestor_paths(uri)
        self.db = sqlite3.connect(str(path), timeout=15, isolation_level=None)
        try:
            self.db.row_factory = sqlite3.Row
            self.db.executescript(SCHEMA)
            if str(path) != ":memory:":
                os.chmod(path, 0o600)
            self._pin_taxonomy_and_install_guards()
        except BaseException:
            self.db.rollback()
            self.db.close()
            raise

    def _pin_taxonomy_and_install_guards(self) -> None:
        """Record the taxonomy hash and create the immutability triggers atomically.

        ``BEGIN IMMEDIATE`` takes the write lock before reading the stored hash, so two
        processes opening the same new file cannot pin different taxonomies, and a
        store is never left with configuration but without its guards.
        """
        self.db.execute("BEGIN IMMEDIATE")
        self.db.execute(CONFIGURATION_DDL)
        pinned = self.db.execute(
            "SELECT value FROM configuration WHERE key='taxonomy_hash'"
        ).fetchone()
        require(
            pinned is None or pinned[0] == self.taxonomy_hash,
            "Taxonomy changed; use an explicit migration/new store",
            "stale_dependency",
        )
        self.db.execute(
            "INSERT OR IGNORE INTO configuration VALUES('taxonomy_hash',?)", (self.taxonomy_hash,)
        )
        for statement in immutability_triggers():
            self.db.execute(statement)
        self.db.commit()

    def close(self) -> None:
        """Close the database connection."""
        self.db.close()

    def seq(self) -> int:
        """Return the sequence number of the latest committed event, or 0 if none."""
        latest: int = self.db.execute("SELECT coalesce(max(seq),0) FROM events").fetchone()[0]
        return latest

    def event(self, actor: Principal, action: str, payload: JsonObject) -> int:
        """Append an event and return its sequence number.

        Internal: call only from an action handler, inside :meth:`execute`'s transaction.
        """
        seq = self.db.execute(
            "INSERT INTO events(actor,action,at,payload) VALUES(?,?,?,?)",
            (actor.id, action, utc_now(), canonical(payload)),
        ).lastrowid
        assert seq is not None  # Always set after a successful INSERT.
        return seq

    def source(self, version: str, actor: Principal) -> JsonObject:
        """Return a source version with its current visibility.

        Raises ``permission`` if the version does not exist or is private and ``actor``
        lacks ``read_private``; the two cases are indistinguishable on purpose.
        """
        row = self.db.execute(
            "SELECT s.*,a.visibility FROM sources s JOIN source_access a USING(version) "
            "WHERE version=?",
            (version,),
        ).fetchone()
        require(
            row is not None
            and (row["visibility"] == "public" or "read_private" in actor.capabilities),
            "Source unavailable to this actor",
            "permission",
        )
        return dict(row)

    # ------------------------------------------------------------------ commands

    def execute(self, actor: Principal, request: Request) -> JsonObject:
        """Run one action request atomically and return its result.

        The envelope is checked before anything is written: ``actor`` must be a
        :class:`Principal` holding the action's capability, and ``request`` must have
        exactly the keys in :data:`~mesh_governance.actions.ENVELOPE_FIELDS`. Roles are
        never read from the request body.

        Transactions: the action runs inside ``BEGIN IMMEDIATE``. SQLite's default
        deferred transaction takes the write lock only at the first write, so two
        connections could both read the same state and then conflict when upgrading.
        Taking the write lock up front serializes writers for the whole read-check-write
        sequence: expected-revision and case-revision checks cannot be invalidated
        between the check and the write, and a waiting writer blocks (up to the 15 s
        busy timeout) instead of failing. Any exception, including ``KeyboardInterrupt``,
        rolls back every write of the request.

        Idempotency: ``(actor.id, idempotency_key)`` identifies a command. The stored
        response is returned on replay without writing a new event. The key is bound to
        a fingerprint (the digest of the canonical request), so reusing a key with
        different content raises ``version_conflict`` instead of silently returning the
        earlier result. Keys are scoped per actor so two actors cannot collide, and the
        capability check runs before the replay lookup, so a revoked actor cannot replay.

        Raises:
            ContractError: If the envelope, capabilities or action rules are violated.
        """
        require(
            isinstance(actor, Principal) and nonempty(actor.id),
            "Trusted principal required",
            "permission",
        )
        require(
            isinstance(request, dict) and set(request) == ENVELOPE_FIELDS,
            "Invalid action envelope",
        )
        requested: str = request["action"]
        action = ALIASES.get(requested, requested)
        require(
            action in CAPABILITIES and CAPABILITIES[action] in actor.capabilities,
            "Capability required",
            "permission",
        )
        require(request["api_version"] == API_VERSION, "Unsupported action API")
        key = request["idempotency_key"]
        require(nonempty(key) and len(key) <= MAX_IDEMPOTENCY_KEY_LENGTH, "Invalid idempotency key")
        require(isinstance(request["parameters"], dict), "Parameters must be an object")
        expected = request["expected_revision"]
        require(
            type(expected) is int and expected >= 0,
            "Expected revision must be a nonnegative integer",
        )
        require(nonempty(request["object_id"]), "Object identity required")
        # Persist only canonical JSON; NaN/Infinity and arbitrary Python types fail.
        fingerprint = digest(request)
        self.db.execute("BEGIN IMMEDIATE")
        try:
            prior = self.db.execute(
                "SELECT * FROM commands WHERE actor=? AND key=?", (actor.id, key)
            ).fetchone()
            result: JsonObject
            if prior:
                require(
                    prior["request_hash"] == fingerprint,
                    "Idempotency key reused with different content",
                    "version_conflict",
                )
                result = json.loads(prior["response"])
            else:
                result = self._HANDLERS[action](self, actor, request)
                self.db.execute(
                    "INSERT INTO commands VALUES(?,?,?,?)",
                    (actor.id, key, fingerprint, canonical(result)),
                )
            self.db.commit()
            return result
        except BaseException:
            self.db.rollback()
            raise

    # ------------------------------------------------------------------ sources

    def _import_source_version(self, actor: Principal, req: Request) -> JsonObject:
        """``importSourceVersion``: store an immutable source version for a subject.

        The version id is the digest of subject, text and metadata, so importing the
        same content again returns the original event. A new version becomes the
        subject's head and marks every current assertion on that subject stale.
        """
        p = req["parameters"]
        require(set(p) == SOURCE_FIELDS, "Invalid source fields")
        require(
            nonempty(p["subject"])
            and isinstance(p["text"], str)
            and isinstance(p["metadata"], dict),
            "Invalid source",
        )
        require(p["visibility"] in VISIBILITIES, "Invalid visibility")
        version = digest({"subject": p["subject"], "text": p["text"], "metadata": p["metadata"]})
        existing = self.db.execute("SELECT * FROM sources WHERE version=?", (version,)).fetchone()
        if existing:
            self.source(version, actor)
            return {
                "source_version": version,
                "event_id": existing["created_seq"],
                "duplicate": True,
            }
        require(req["expected_revision"] == 0, "Source versions are immutable")
        seq = self.event(
            actor, "importSourceVersion", {"subject": p["subject"], "source_version": version}
        )
        self.db.execute(
            "INSERT INTO sources VALUES(?,?,?,?,?,?)",
            (version, p["subject"], p["text"], text_hash(p["text"]), canonical(p["metadata"]), seq),
        )
        self.db.execute("INSERT INTO source_access VALUES(?,?)", (version, p["visibility"]))
        self.db.execute(
            "INSERT INTO source_heads VALUES(?,?) "
            "ON CONFLICT(subject) DO UPDATE SET version=excluded.version",
            (p["subject"], version),
        )
        affected = self.db.execute(
            "SELECT r.* FROM revisions r JOIN heads h USING(assertion_id,revision) "
            "WHERE r.subject=?",
            (p["subject"],),
        ).fetchall()
        for row in affected:
            self.db.execute("UPDATE heads SET stale=1 WHERE assertion_id=?", (row["assertion_id"],))
            self.db.execute(
                "INSERT INTO invalidations VALUES(?,?,?,?)",
                (
                    row["assertion_id"],
                    row["revision"],
                    "source_changed: reevaluation required",
                    seq,
                ),
            )
        return {"source_version": version, "event_id": seq, "invalidated_assertions": len(affected)}

    def _set_source_visibility(self, actor: Principal, req: Request) -> JsonObject:
        """``setSourceVisibility``: make a source version public or private.

        Restricting a source hides it, and everything citing it, from snapshot views
        and blocks rolling back to snapshots that contain it.
        """
        require(req["parameters"].get("visibility") in VISIBILITIES, "Invalid visibility")
        self.source(req["object_id"], actor)
        visibility = req["parameters"]["visibility"]
        self.db.execute(
            "UPDATE source_access SET visibility=? WHERE version=?", (visibility, req["object_id"])
        )
        seq = self.event(
            actor,
            "setSourceVisibility",
            {"source_version": req["object_id"], "visibility": visibility},
        )
        return {"event_id": seq}

    # ------------------------------------------------------------------ validation

    def validate_time(self, time: Any) -> None:
        """Check an assertion's ``valid_time``; see :func:`validation.validate_valid_time`."""
        validate_valid_time(time)

    @staticmethod
    def timestamp(value: object) -> float:
        """Parse an explicit-UTC timestamp; see :func:`validation.parse_utc_timestamp`."""
        return parse_utc_timestamp(value)

    def validate(self, payload: Any, actor: Principal, current: bool = True) -> None:
        """Check an assertion payload against its source, taxonomy and profile.

        Checks run in a fixed order: field set, semantic state and profile; source
        access and subject; (if ``current``) that the cited version is still the
        subject's head; valid time; evidence selectors; then profile rules. Evidence
        must match the source exactly, which establishes consistency with the cited
        text, not the truth of the claim; that is what review is for.
        """
        require(isinstance(payload, dict), "Assertion must be an object")
        require(set(payload) == ASSERTION_FIELDS, "Invalid assertion fields")
        require(payload["semantic_state"] in DECISIONS, "Unknown semantic state")
        require(payload["profile"] in PROFILES, "Unregistered domain profile")
        source = self.source(payload["source_version"], actor)
        require(source["subject"] == payload["subject"], "Source belongs to a different subject")
        if current:
            head = self.db.execute(
                "SELECT version FROM source_heads WHERE subject=?", (payload["subject"],)
            ).fetchone()
            require(
                head and head[0] == payload["source_version"],
                "Source superseded",
                "stale_dependency",
            )
        self.validate_time(payload["valid_time"])
        require(
            isinstance(payload["detail"], dict) and isinstance(payload["evidence"], list),
            "Invalid assertion details/evidence",
        )
        require(
            payload["semantic_state"] != "supported" or bool(payload["evidence"]),
            "Supported assertions require evidence",
        )
        for ref in payload["evidence"]:
            check_selector(ref, source)
        if payload["profile"] == "cordis-sdg":
            check_sdg_profile(payload, self.concepts)
        else:
            check_observation_profile(payload)

    # ------------------------------------------------------------------ assertions

    def head(self, aid: str, actor: Principal) -> tuple[JsonObject, JsonObject]:
        """Return the current revision row (with its ``stale`` flag) and decoded payload.

        Raises ``not_found`` for an unknown assertion and ``permission`` if ``actor``
        cannot read the cited source.
        """
        row = self.db.execute(
            "SELECT r.*,h.stale FROM revisions r JOIN heads h USING(assertion_id,revision) "
            "WHERE assertion_id=?",
            (aid,),
        ).fetchone()
        require(row is not None, "Assertion not found", "not_found")
        payload: JsonObject = json.loads(row["payload"])
        self.source(payload["source_version"], actor)
        return dict(row), payload

    def write_revision(
        self,
        actor: Principal,
        aid: str,
        revision: int,
        payload: JsonObject,
        action: str,
        reason: str | None = None,
    ) -> JsonObject:
        """Append a revision, point the head at it and open an unreviewed case.

        A new revision never inherits reviews: its case starts at ``unreviewed``.
        Internal: call only from an action handler, inside :meth:`execute`'s transaction.
        """
        seq = self.event(
            actor, action, {"assertion_id": aid, "revision": revision, "reason": reason}
        )
        self.db.execute(
            "INSERT INTO revisions VALUES(?,?,?,?,?,?)",
            (aid, revision, payload["profile"], payload["subject"], canonical(payload), seq),
        )
        self.db.execute(
            "INSERT INTO heads VALUES(?,?,0) "
            "ON CONFLICT(assertion_id) DO UPDATE SET revision=excluded.revision,stale=0",
            (aid, revision),
        )
        self.db.execute(
            "INSERT INTO cases VALUES(?,?,0,'unreviewed',NULL,NULL,NULL)", (aid, revision)
        )
        return {"assertion_id": aid, "revision": revision, "event_id": seq}

    def proposer(self, aid: str, revision: int) -> str | None:
        """Return the actor who wrote ``revision`` of an assertion, if it exists."""
        row = self.db.execute(
            "SELECT e.actor FROM revisions r JOIN events e ON e.seq=r.created_seq "
            "WHERE r.assertion_id=? AND r.revision=?",
            (aid, revision),
        ).fetchone()
        actor: str | None = row["actor"] if row else None
        return actor

    def context(self, seq: int, p: JsonObject, allowed: Iterable[str]) -> JsonObject:
        """Record optional provenance fields (``independence``, ``entered_by``) for an event.

        ``independence`` must be ``blind`` or ``informed``; ``entered_by`` names who
        transcribed the action when it is not the acting principal. The context row is
        append-only. Returns the parameters without the provenance fields.
        """
        extra, rest = split_context(p, allowed)
        require(extra.get("independence", "blind") in {"blind", "informed"}, "Invalid independence")
        require("entered_by" not in extra or nonempty(extra["entered_by"]), "Invalid entered_by")
        if extra:
            self.db.execute("INSERT INTO action_context VALUES(?,?)", (seq, canonical(extra)))
        return rest

    def _propose_assertion(self, actor: Principal, req: Request) -> JsonObject:
        """``proposeAssertion``: create revision 1 of a new assertion.

        Proposing identical content under the same id again returns the existing
        revision; different content under a used id is a ``version_conflict``.
        """
        require(req["expected_revision"] == 0, "New assertion starts at revision zero")
        self.validate(req["parameters"], actor)
        existing = self.db.execute(
            "SELECT * FROM revisions WHERE assertion_id=? AND revision=1", (req["object_id"],)
        ).fetchone()
        if existing:
            require(
                existing["payload"] == canonical(req["parameters"]),
                "Assertion identity reused",
                "version_conflict",
            )
            return {
                "assertion_id": req["object_id"],
                "revision": 1,
                "event_id": existing["created_seq"],
                "duplicate": True,
            }
        return self.write_revision(
            actor, req["object_id"], 1, req["parameters"], "proposeAssertion"
        )

    def _correct_assertion(self, actor: Principal, req: Request) -> JsonObject:
        """``correctAssertion``: append a corrected revision of the current head.

        The correction must name the current revision and may not change the
        assertion's identity (profile, subject, predicate and, for ``cordis-sdg``, the
        concept). Corrections clear the stale flag only by revalidating against the
        current source.
        """
        row, old = self.head(req["object_id"], actor)
        require(
            row["revision"] == req["expected_revision"],
            "Assertion revision changed",
            "version_conflict",
        )
        p = req["parameters"]
        require(
            set(p) == {"replacement", "reason"} and nonempty(p["reason"]),
            "Correction requires replacement and reason",
        )
        self.validate(p["replacement"], actor)
        for field in ("profile", "subject", "predicate"):
            require(
                old[field] == p["replacement"][field], "Correction cannot change assertion identity"
            )
        if old["profile"] == "cordis-sdg":
            require(
                old["object"] == p["replacement"]["object"],
                "Use a new assertion for a different concept",
            )
        return self.write_revision(
            actor,
            req["object_id"],
            row["revision"] + 1,
            p["replacement"],
            "correctAssertion",
            p["reason"],
        )

    def _invalidate_mapping(self, actor: Principal, req: Request) -> JsonObject:
        """``invalidateMapping``: mark the current revision stale with a reason."""
        row, _ = self.head(req["object_id"], actor)
        require(
            row["revision"] == req["expected_revision"],
            "Assertion revision changed",
            "version_conflict",
        )
        require(nonempty(req["parameters"].get("reason")), "Invalidation requires a reason")
        seq = self.event(
            actor, "invalidateMapping", {"assertion_id": req["object_id"], **req["parameters"]}
        )
        self.db.execute("UPDATE heads SET stale=1 WHERE assertion_id=?", (req["object_id"],))
        self.db.execute(
            "INSERT INTO invalidations VALUES(?,?,?,?)",
            (req["object_id"], row["revision"], req["parameters"]["reason"], seq),
        )
        return {"event_id": seq}

    # ------------------------------------------------------------------ review

    def _review_assertion(self, actor: Principal, req: Request) -> JsonObject:
        """``reviewAssertion``: record one opinion on the exact current revision.

        The proposer may not review its own revision and stale revisions cannot be
        reviewed. Each review is kept; only an actor's latest opinion counts. The case
        revision is bumped on every opinion so an adjudication prepared against an
        older set of opinions is rejected. Disagreeing opinions, or a new opinion after
        a ruling, reopen the case.
        """
        oid = req["object_id"]
        row, _ = self.head(oid, actor)
        revision = row["revision"]
        require(
            revision == req["expected_revision"],
            "Review must name the exact current revision",
            "version_conflict",
        )
        require(not row["stale"], "Review cannot approve stale evidence", "stale_dependency")
        require(
            actor.id != self.proposer(oid, revision),
            "The proposer cannot review its own assertion",
            "independence",
        )
        extra, p = split_context(req["parameters"], REVIEW_CONTEXT_FIELDS)
        require(
            set(p) == REVIEW_FIELDS and p["verdict"] in REVIEW_VERDICTS and nonempty(p["reason"]),
            "Invalid review",
        )
        require(p["dimension"] in DISPUTE_DIMENSIONS, "Invalid dispute dimension")
        seq = self.event(
            actor, "reviewAssertion", {"assertion_id": oid, "revision": revision, **p, **extra}
        )
        self.context(seq, extra, REVIEW_CONTEXT_FIELDS)
        self.db.execute(
            "INSERT INTO reviews(assertion_id,revision,actor,actor_kind,verdict,dimension,"
            "reason,seq) VALUES(?,?,?,?,?,?,?,?)",
            (oid, revision, actor.id, actor.kind, p["verdict"], p["dimension"], p["reason"], seq),
        )
        reviews = self.opinions(oid, revision)
        old_case = self.db.execute(
            "SELECT * FROM cases WHERE assertion_id=? AND revision=?", (oid, revision)
        ).fetchone()
        # Same-actor changes preserve history but only the latest opinion is effective.
        disagreement = len({x["verdict"] for x in reviews}) > 1
        state = "open" if disagreement or old_case["resolution"] else "reviewed"
        self.db.execute(
            "UPDATE cases SET case_revision=case_revision+1,state=?,resolution=NULL,"
            "resolver_kind=NULL,resolved_seq=NULL WHERE assertion_id=? AND revision=?",
            (state, oid, revision),
        )
        return {
            "event_id": seq,
            "revision": revision,
            "case_revision": old_case["case_revision"] + 1,
            "case_state": state,
        }

    def opinions(self, aid: str, revision: int) -> list[JsonObject]:
        """Return each reviewer's latest opinion on a revision, ordered by first review."""
        rows = self.db.execute(
            "SELECT * FROM reviews WHERE assertion_id=? AND revision=? ORDER BY seq",
            (aid, revision),
        ).fetchall()
        latest = {x["actor"]: dict(x) for x in rows}
        return list(latest.values())

    def _adjudicate_dispute(self, actor: Principal, req: Request) -> JsonObject:
        """``adjudicateDispute``: rule on the current set of opinions for a revision.

        The ruling is a compare-and-swap on the case revision and must list every
        current opinion id, so a new opinion invalidates a pending ruling. The
        adjudicator cannot be the proposer or an opinion author. ``uphold`` and
        ``reject`` resolve the case; other outcomes leave it unresolved.
        """
        oid = req["object_id"]
        row, _ = self.head(oid, actor)
        revision = row["revision"]
        require(
            req["expected_revision"] == revision and not row["stale"],
            "Stale adjudication target",
            "version_conflict",
        )
        case = self.db.execute(
            "SELECT * FROM cases WHERE assertion_id=? AND revision=?", (oid, revision)
        ).fetchone()
        extra, p = split_context(req["parameters"], ADJUDICATION_CONTEXT_FIELDS)
        require(set(p) == ADJUDICATION_FIELDS, "Invalid adjudication")
        require(
            type(p["expected_case_revision"]) is int
            and p["expected_case_revision"] == case["case_revision"],
            "New opinion or adjudication changed the case",
            "version_conflict",
        )
        opinions = self.opinions(oid, revision)
        require(
            bool(opinions) and p["opinion_ids"] == sorted(x["id"] for x in opinions),
            "Adjudication must consider every current opinion",
            "stale_dependency",
        )
        require(
            actor.id != self.proposer(oid, revision)
            and actor.id not in {x["actor"] for x in opinions},
            "The adjudicator cannot be the proposer or an opinion author",
            "independence",
        )
        require(p["outcome"] in ADJUDICATION_OUTCOMES and nonempty(p["reason"]), "Invalid outcome")
        seq = self.event(
            actor, "adjudicateDispute", {"assertion_id": oid, "revision": revision, **p, **extra}
        )
        self.context(seq, extra, ADJUDICATION_CONTEXT_FIELDS)
        self.db.execute(
            "INSERT INTO adjudications(assertion_id,revision,case_revision,actor,actor_kind,"
            "outcome,reason,opinions,seq) VALUES(?,?,?,?,?,?,?,?,?)",
            (
                oid,
                revision,
                case["case_revision"],
                actor.id,
                actor.kind,
                p["outcome"],
                p["reason"],
                canonical(p["opinion_ids"]),
                seq,
            ),
        )
        state = "adjudicated" if p["outcome"] in {"uphold", "reject"} else "unresolved"
        self.db.execute(
            "UPDATE cases SET case_revision=case_revision+1,state=?,resolution=?,resolver_kind=?,"
            "resolved_seq=? WHERE assertion_id=? AND revision=?",
            (state, p["outcome"], actor.kind, seq, oid, revision),
        )
        return {"event_id": seq, "case_revision": case["case_revision"] + 1, "case_state": state}

    def review_state(self, aid: str, revision: int) -> str:
        """Summarize the review status of one exact revision.

        Returns one of ``unreviewed``, ``ai_reviewed``, ``human_approved``,
        ``needs_evidence``, ``rejected`` or ``disputed``. An open or unresolved case is
        ``disputed``; a ruling decides the status; otherwise the latest opinions do. A
        rejection counts only if a human made it: an AI-only rejection is a dispute
        awaiting adjudication. Only human opinions or rulings yield ``human_approved``.
        """
        case = self.db.execute(
            "SELECT * FROM cases WHERE assertion_id=? AND revision=?", (aid, revision)
        ).fetchone()
        if case["state"] in {"open", "unresolved"}:
            return "disputed"
        if case["resolution"]:
            if case["resolution"] == "reject":
                return "rejected"
            return "human_approved" if case["resolver_kind"] == "human" else "ai_reviewed"
        opinions = self.opinions(aid, revision)
        if not opinions:
            return "unreviewed"
        if any(x["verdict"] == "reject" for x in opinions):
            # A rejection needs a human: an AI-only rejection opens a dispute for adjudication.
            human_rejection = any(
                x["verdict"] == "reject" and x["actor_kind"] == "human" for x in opinions
            )
            return "rejected" if human_rejection else "disputed"
        if any(x["verdict"] == "request_evidence" for x in opinions):
            return "needs_evidence"
        return (
            "human_approved" if any(x["actor_kind"] == "human" for x in opinions) else "ai_reviewed"
        )

    # ------------------------------------------------------------------ queries

    def query(
        self,
        actor: Principal,
        aid: str,
        known_at: int | None = None,
        valid_at: str | None = None,
    ) -> JsonObject:
        """Return an assertion as it was known at an event cutoff.

        Two time axes are kept apart. ``known_at`` is a committed event sequence number
        (knowledge time): the latest revision recorded at or before it is returned,
        together with whether it had been invalidated by then. ``valid_at`` is an
        explicit-UTC instant (validity time) tested against the revision's
        ``valid_time``; ``temporal_state`` is ``not_requested`` when it is omitted.
        Current source access rules apply to old revisions too.
        """
        require(
            known_at is None or (type(known_at) is int and 0 <= known_at <= self.seq()),
            "Invalid committed cutoff",
        )
        cutoff = self.seq() if known_at is None else known_at
        row = self.db.execute(
            "SELECT * FROM revisions WHERE assertion_id=? AND created_seq<=? "
            "ORDER BY revision DESC LIMIT 1",
            (aid, cutoff),
        ).fetchone()
        require(row is not None, "No assertion at this cutoff", "not_found")
        payload = json.loads(row["payload"])
        self.source(payload["source_version"], actor)  # Current ACL also applies to old revisions.
        applicability = "not_requested"
        if valid_at is not None:
            applicability = temporal_state(payload["valid_time"], valid_at)
        next_row = self.db.execute(
            "SELECT created_seq FROM revisions WHERE assertion_id=? AND revision>? "
            "AND created_seq<=? ORDER BY revision LIMIT 1",
            (aid, row["revision"], cutoff),
        ).fetchone()
        invalidated = self.db.execute(
            "SELECT reason FROM invalidations WHERE assertion_id=? AND revision=? AND seq<=? "
            "ORDER BY seq DESC LIMIT 1",
            (aid, row["revision"], cutoff),
        ).fetchone()
        return {
            "assertion_id": aid,
            "revision": row["revision"],
            "known_at": cutoff,
            "recorded_from": row["created_seq"],
            "recorded_to": next_row[0] if next_row else None,
            "temporal_state": applicability,
            "dependency_state": "stale" if invalidated else "current_at_cutoff",
            "invalidation_reason": invalidated[0] if invalidated else None,
            "payload": payload,
        }

    def ancestor_paths(self, uri: str) -> list[list[str]]:
        """Return every path from ``uri`` to each of its ancestors in the taxonomy.

        Each path starts at ``uri``. Raises on unknown concepts, dangling parents and
        cycles, and caps the traversal so a pathological taxonomy cannot hang startup.
        """
        require(uri in self.concepts, "Unknown concept")
        result: list[list[str]] = []
        queue: list[tuple[str, list[str]]] = [(uri, [uri])]
        while queue:
            node, path = queue.pop()
            require(
                len(result) + len(queue) < TAXONOMY_TRAVERSAL_LIMIT,
                "Taxonomy traversal limit reached",
            )
            for parent in self.concepts[node].get("broader", []):
                require(parent in self.concepts and parent not in path, "Dangling/cyclic taxonomy")
                route = [*path, parent]
                result.append(route)
                queue.append((parent, route))
        return result

    def derive_navigation(self, actor: Principal, aids: Iterable[str]) -> list[JsonObject]:
        """Derive navigation links from approved assertions to their ancestor concepts.

        Rule R1.1: a current, human-approved, supported ``cordis-sdg`` assertion about a
        concept also places its subject under each broader concept, for navigation
        only (no impact claim). Each derived link lists every justifying assertion with
        its revision, source version, taxonomy hash, path and modality. Duplicate ids
        are counted once.
        """
        conclusions: dict[tuple[str, str], JsonObject] = {}
        for aid in dict.fromkeys(aids):
            row, p = self.head(aid, actor)
            if (
                row["stale"]
                or p["profile"] != "cordis-sdg"
                or p["semantic_state"] != "supported"
                or self.review_state(aid, row["revision"]) != "human_approved"
            ):
                continue
            for path in self.ancestor_paths(p["object"]):
                concept = path[-1]
                conclusion = conclusions.setdefault(
                    (p["subject"], concept),
                    {
                        "subject": p["subject"],
                        "concept": concept,
                        "origin": "derived_navigation",
                        "justifications": [],
                    },
                )
                conclusion["justifications"].append(
                    {
                        "rule": NAVIGATION_RULE,
                        "rule_hash": NAVIGATION_RULE_HASH,
                        "assertion_id": aid,
                        "revision": row["revision"],
                        "source_version": p["source_version"],
                        "taxonomy_hash": self.taxonomy_hash,
                        "path": path,
                        "modality": p["modality"],
                    }
                )
        return list(conclusions.values())

    # ------------------------------------------------------------------ publication

    def _publish_snapshot(self, actor: Principal, req: Request) -> JsonObject:
        """``publishSnapshot``: freeze a content-addressed snapshot and make it active.

        The manifest names the sources and assertions to include and the event cutoff
        it was prepared against (a compare-and-swap on :meth:`seq`). Every source must
        be public and current, every assertion current and revalidated. ``reviewed``
        mode additionally requires exact-revision human approval for each assertion
        and no outstanding operational findings; ``exploratory`` mode records each
        assertion's review status instead.
        """
        p = req["parameters"]
        require(set(p) == MANIFEST_FIELDS, "Invalid publication manifest")
        require(
            p["mode"] in PUBLICATION_MODES
            and type(p["expected_seq"]) is int
            and p["expected_seq"] == self.seq(),
            "Publication cutoff changed",
            "version_conflict",
        )
        require(
            isinstance(p["assertion_ids"], list)
            and len(p["assertion_ids"]) == len(set(p["assertion_ids"])),
            "Duplicate assertions",
        )
        require(isinstance(p["operational_findings"], list), "Operational findings must be a list")
        require(
            isinstance(p["source_versions"], list)
            and len(p["source_versions"]) == len(set(p["source_versions"])),
            "Duplicate sources",
        )
        sources: JsonObject = {}
        for version in p["source_versions"]:
            source = self.source(version, actor)
            require(
                source["visibility"] == "public",
                "Private evidence cannot enter a public snapshot",
                "permission",
            )
            head = self.db.execute(
                "SELECT version FROM source_heads WHERE subject=?", (source["subject"],)
            ).fetchone()
            require(
                head[0] == version, "Publication contains a superseded source", "stale_dependency"
            )
            sources[version] = {
                "subject": source["subject"],
                "text": source["text"],
                "text_hash": source["text_hash"],
                "metadata": json.loads(source["metadata"]),
            }
        records = []
        for aid in p["assertion_ids"]:
            row, payload = self.head(aid, actor)
            require(not row["stale"], "Stale assertion cannot be published", "stale_dependency")
            require(payload["source_version"] in sources, "Publication missing source")
            self.validate(payload, actor)
            review = self.review_state(aid, row["revision"])
            if p["mode"] == "reviewed":
                require(
                    review == "human_approved",
                    "Reviewed publication requires exact-revision human approval",
                    "quality_gate",
                )
            records.append(
                {
                    "assertion_id": aid,
                    "revision": row["revision"],
                    "review_status": review,
                    "payload": payload,
                }
            )
        require(
            all(is_valid_finding(x, sources) for x in p["operational_findings"]),
            "Invalid operational finding",
        )
        require(
            p["mode"] != "reviewed" or not p["operational_findings"],
            "Reviewed collection has unresolved assessments",
            "quality_gate",
        )
        body = {
            "schema": "mesh.snapshot.v1",
            "mode": p["mode"],
            "cutoff": self.seq(),
            "taxonomy_hash": self.taxonomy_hash,
            "concepts": list(self.concepts.values()),
            "sources": sources,
            "assertions": records,
            "operational_findings": p["operational_findings"],
            "derivations": self.derive_navigation(actor, p["assertion_ids"]),
        }
        snapshot_hash = digest(body)
        snapshot_id = "snapshot:" + snapshot_hash
        seq = self.event(
            actor,
            "publishSnapshot",
            {"snapshot_id": snapshot_id, "mode": p["mode"], "hash": snapshot_hash},
        )
        self.db.execute(
            "INSERT OR IGNORE INTO snapshots VALUES(?,?,?,?)",
            (snapshot_id, canonical(body), snapshot_hash, seq),
        )
        self.db.execute(
            "INSERT INTO active_snapshot VALUES(1,?) "
            "ON CONFLICT(singleton) DO UPDATE SET id=excluded.id",
            (snapshot_id,),
        )
        return {"snapshot_id": snapshot_id, "sha256": snapshot_hash, "event_id": seq}

    def snapshot(self, actor: Principal = PUBLIC, snapshot_id: str | None = None) -> JsonObject:
        """Return a snapshot (the active one by default) filtered to what ``actor`` may see.

        The stored body is re-hashed before use (``integrity`` on mismatch). Sources the
        actor can no longer read are removed, along with the assertions, findings and
        derivation justifications that depend on them. ``original_sha256`` is the
        published hash; ``view_sha256`` is the hash of the filtered view.
        """
        if snapshot_id is None:
            active = self.db.execute("SELECT id FROM active_snapshot WHERE singleton=1").fetchone()
            require(active is not None, "No published snapshot", "not_found")
            snapshot_id = active[0]
        row = self.db.execute("SELECT * FROM snapshots WHERE id=?", (snapshot_id,)).fetchone()
        require(row is not None, "Snapshot not found", "not_found")
        body = json.loads(row["content"])
        require(digest(body) == row["hash"], "Snapshot hash mismatch", "integrity")
        allowed = {}
        for version, source in body["sources"].items():
            try:
                self.source(version, actor)
                allowed[version] = source
            except ContractError as exc:
                if exc.code != "permission":
                    raise
        body["sources"] = allowed
        body["assertions"] = [
            x for x in body["assertions"] if x["payload"]["source_version"] in allowed
        ]
        body["operational_findings"] = [
            x for x in body["operational_findings"] if x["source_version"] in allowed
        ]
        eligible = {(x["assertion_id"], x["revision"]) for x in body["assertions"]}
        derived = []
        for item in body["derivations"]:
            item["justifications"] = [
                x for x in item["justifications"] if (x["assertion_id"], x["revision"]) in eligible
            ]
            if item["justifications"]:
                derived.append(item)
        body["derivations"] = derived
        return {
            "snapshot_id": snapshot_id,
            "original_sha256": row["hash"],
            "view_sha256": digest(body),
            "historical_publication": True,
            **body,
        }

    def _rollback_publication(self, actor: Principal, req: Request) -> JsonObject:
        """``rollbackPublication``: make an earlier snapshot active again.

        Allowed only if the snapshot could still be published today: every original
        source must still be public, every assertion still at the same non-stale
        revision, and every review status unchanged. The check uses the original
        stored body, not a permission-filtered view.
        """
        snapshot_id = req["object_id"]
        body = self.snapshot(actor, snapshot_id)
        # Rollback must revalidate ALL original resources, not a permission-filtered view.
        row = self.db.execute("SELECT content FROM snapshots WHERE id=?", (snapshot_id,)).fetchone()
        original = json.loads(row[0])
        require(
            set(body["sources"]) == set(original["sources"]),
            "Rollback source access revoked",
            "permission",
        )
        for version in original["sources"]:
            require(
                self.source(version, actor)["visibility"] == "public",
                "Rollback cannot republish restricted source",
                "permission",
            )
        for item in original["assertions"]:
            current, _ = self.head(item["assertion_id"], actor)
            require(
                not current["stale"] and current["revision"] == item["revision"],
                "Rollback would restore stale knowledge",
                "stale_dependency",
            )
            require(
                self.review_state(item["assertion_id"], item["revision"]) == item["review_status"],
                "Review changed since publication",
                "stale_dependency",
            )
        self.db.execute("UPDATE active_snapshot SET id=? WHERE singleton=1", (snapshot_id,))
        seq = self.event(actor, "rollbackPublication", {"snapshot_id": snapshot_id})
        return {"snapshot_id": snapshot_id, "event_id": seq}

    # ------------------------------------------------------------------ dispatch

    # Every key of actions.CAPABILITIES maps to exactly one handler. execute() resolves
    # ALIASES and checks the capability before looking up the handler here.
    _HANDLERS: ClassVar[dict[str, Handler]] = {
        "importSourceVersion": _import_source_version,
        "setSourceVisibility": _set_source_visibility,
        "proposeAssertion": _propose_assertion,
        "correctAssertion": _correct_assertion,
        "invalidateMapping": _invalidate_mapping,
        "reviewAssertion": _review_assertion,
        "adjudicateDispute": _adjudicate_dispute,
        "publishSnapshot": _publish_snapshot,
        "rollbackPublication": _rollback_publication,
    }

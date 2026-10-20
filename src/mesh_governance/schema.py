"""SQLite schema and the triggers that make history append-only.

Tables fall into two groups:

- History tables (:data:`IMMUTABLE_TABLES`) are append-only. ``BEFORE UPDATE`` and
  ``BEFORE DELETE`` triggers abort any attempt to change them, so a bug or a direct SQL
  session cannot rewrite what was recorded; it surfaces as ``sqlite3.IntegrityError``.
- Pointer tables (``source_heads``, ``source_access``, ``heads``, ``cases``,
  ``active_snapshot``) hold the current state derived from that history and are updated
  in place, always inside the same transaction as the history row that justifies the
  change.

Column order matters: the store inserts with positional ``VALUES(...)``.
"""

from __future__ import annotations

SCHEMA = """
PRAGMA foreign_keys=ON;
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
CREATE TABLE IF NOT EXISTS events(
 seq INTEGER PRIMARY KEY AUTOINCREMENT,
 actor TEXT NOT NULL,
 action TEXT NOT NULL,
 at TEXT NOT NULL,
 payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS commands(
 actor TEXT,
 key TEXT,
 request_hash TEXT NOT NULL,
 response TEXT NOT NULL,
 PRIMARY KEY(actor,key));
CREATE TABLE IF NOT EXISTS sources(
 version TEXT PRIMARY KEY,
 subject TEXT NOT NULL,
 text TEXT NOT NULL,
 text_hash TEXT NOT NULL,
 metadata TEXT NOT NULL,
 created_seq INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS source_heads(
 subject TEXT PRIMARY KEY,
 version TEXT NOT NULL REFERENCES sources(version));
CREATE TABLE IF NOT EXISTS source_access(
 version TEXT PRIMARY KEY REFERENCES sources(version),
 visibility TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS revisions(
 assertion_id TEXT,
 revision INTEGER,
 profile TEXT NOT NULL,
 subject TEXT NOT NULL,
 payload TEXT NOT NULL,
 created_seq INTEGER NOT NULL REFERENCES events(seq),
 PRIMARY KEY(assertion_id,revision));
CREATE TABLE IF NOT EXISTS heads(
 assertion_id TEXT PRIMARY KEY,
 revision INTEGER NOT NULL,
 stale INTEGER NOT NULL DEFAULT 0,
 FOREIGN KEY(assertion_id,revision) REFERENCES revisions(assertion_id,revision));
CREATE TABLE IF NOT EXISTS reviews(
 id INTEGER PRIMARY KEY,
 assertion_id TEXT,
 revision INTEGER,
 actor TEXT NOT NULL,
 actor_kind TEXT NOT NULL,
 verdict TEXT NOT NULL,
 dimension TEXT NOT NULL,
 reason TEXT NOT NULL,
 seq INTEGER NOT NULL,
 FOREIGN KEY(assertion_id,revision) REFERENCES revisions(assertion_id,revision));
CREATE TABLE IF NOT EXISTS cases(
 assertion_id TEXT,
 revision INTEGER,
 case_revision INTEGER NOT NULL,
 state TEXT NOT NULL,
 resolution TEXT,
 resolver_kind TEXT,
 resolved_seq INTEGER,
 PRIMARY KEY(assertion_id,revision),
 FOREIGN KEY(assertion_id,revision) REFERENCES revisions(assertion_id,revision));
CREATE TABLE IF NOT EXISTS adjudications(
 id INTEGER PRIMARY KEY,
 assertion_id TEXT,
 revision INTEGER,
 case_revision INTEGER NOT NULL,
 actor TEXT NOT NULL,
 actor_kind TEXT NOT NULL,
 outcome TEXT NOT NULL,
 reason TEXT NOT NULL,
 opinions TEXT NOT NULL,
 seq INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS invalidations(
 assertion_id TEXT NOT NULL,
 revision INTEGER NOT NULL,
 reason TEXT NOT NULL,
 seq INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS action_context(
 seq INTEGER PRIMARY KEY REFERENCES events(seq),
 context TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS snapshots(
 id TEXT PRIMARY KEY,
 content TEXT NOT NULL,
 hash TEXT NOT NULL,
 seq INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS active_snapshot(
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 id TEXT NOT NULL REFERENCES snapshots(id));
CREATE INDEX IF NOT EXISTS review_target ON reviews(assertion_id,revision,seq);
CREATE INDEX IF NOT EXISTS revision_subject ON revisions(subject,created_seq);
"""
"""Tables and indexes, applied with ``executescript`` when a store opens."""

CONFIGURATION_DDL = (
    "CREATE TABLE IF NOT EXISTS configuration(key TEXT PRIMARY KEY,value TEXT NOT NULL)"
)
"""Store-level settings. Created inside the initialization transaction, see ``MeshStore``."""

IMMUTABLE_TABLES = (
    "events",
    "commands",
    "sources",
    "revisions",
    "reviews",
    "adjudications",
    "invalidations",
    "snapshots",
    "action_context",
)
"""Append-only history tables guarded by :func:`immutability_triggers`."""


def immutability_triggers() -> list[str]:
    """Return ``CREATE TRIGGER`` statements rejecting UPDATE and DELETE on history tables."""
    return [
        f"CREATE TRIGGER IF NOT EXISTS immutable_{table}_{verb} BEFORE {verb} ON {table} "
        "BEGIN SELECT RAISE(ABORT,'Immutable record'); END"
        for table in IMMUTABLE_TABLES
        for verb in ("UPDATE", "DELETE")
    ]

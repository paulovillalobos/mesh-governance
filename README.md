# Mesh Governance

[![Tests](https://github.com/paulovillalobos/mesh-governance/actions/workflows/tests.yml/badge.svg)](https://github.com/paulovillalobos/mesh-governance/actions/workflows/tests.yml)

A Python + SQLite evidence-governance library extracted from **Nexus / Mesh**. It preserves source versions and assertion history, records independent reviews, and controls which revisions can enter a published snapshot.

**42 acceptance tests · Python standard library only at runtime · Typed (`mypy --strict`) · Synthetic fixtures · No API keys**

## Run the tests

Requires Python 3.10+ with SQLite support. The library has no runtime dependencies.

With [uv](https://docs.astral.sh/uv/), which installs the pinned dev tools from `uv.lock`:

```sh
git clone https://github.com/paulovillalobos/mesh-governance.git
cd mesh-governance
uv sync
uv run pytest -v
uv run ruff check src tests
uv run mypy --strict
```

With the standard library only, no installation:

```sh
PYTHONPATH=src python3 -B -m unittest discover -s tests -v
```

Tests use temporary databases and synthetic source text, then clean up after themselves. CI runs lint, format check, `mypy --strict` and both test commands on Python 3.10 and 3.12; a successful run reports `42 passed` under pytest and `Ran 42 tests` / `OK` under unittest. The host-timezone regression requires `time.tzset` and is skipped on platforms without it.

## The problem

A system can store a claim while losing the information needed to judge it: the exact source version, the cited passage, who proposed it, which revision was reviewed, and whether later evidence made it stale.

This library makes those dependencies explicit:

```text
Source version → Assertion revision → Independent review → Published snapshot
       ↓
Source update → Dependent assertions become stale → Reassessment required
```

## Engineering decisions

| Concern | Implementation |
| --- | --- |
| Repeated or conflicting requests | Actor-scoped idempotency keys and request fingerprints inside a SQLite transaction. |
| Concurrent writers | Each command runs inside `BEGIN IMMEDIATE`, so its read-check-write sequence holds the write lock; any exception rolls back every write. |
| History preservation | Append-only source, revision and review records, with database triggers rejecting updates and deletes. |
| Evidence consistency | Exact source spans and hashes; assertions must reference the correct subject and current source version. |
| Independent review | The proposer cannot review their own revision; an adjudicator cannot rule on their own opinion. |
| Changing knowledge | Source updates invalidate dependent assertions. Queries distinguish knowledge cutoffs from validity time. |
| Publication | Reviewed publication requires human approval of the exact revision. Exploratory snapshots retain their review status. |
| Rollback | Historical publication cannot be restored when its evidence is restricted, its assertions are stale, or its reviews changed. |

## Layout

```text
src/mesh_governance/
  store.py        MeshStore: command execution, action handlers, reads and snapshots
  validation.py   Stateless checks for assertions, evidence selectors and UTC time values
  schema.py       SQLite tables and the append-only triggers
  actions.py      Request envelope, action → capability table, aliases, command()
  principal.py    Principal: identity and capabilities supplied by host code
  errors.py       ContractError and its stable error codes
  encoding.py     Canonical JSON and SHA-256 helpers
tests/
  test_mesh_ontology.py   Acceptance tests (unittest; also collected by pytest)
```

## Start reading here

- [`MeshStore.execute`](src/mesh_governance/store.py): transactions, idempotency and the explicit action-handler table. Its docstring explains why the write lock is taken up front and why keys are bound to request fingerprints.
- [`MeshStore.validate`](src/mesh_governance/store.py) and [`validation.py`](src/mesh_governance/validation.py): evidence and time checks. The review, publication and rollback handlers in `store.py` implement governance.
- [`schema.py`](src/mesh_governance/schema.py): which tables are append-only history and which are current-state pointers.
- [Acceptance tests](tests/test_mesh_ontology.py): `StoreTests.setUp` and its helpers show a source moving through the command interface. Tests cover concurrent reviews, role injection, stale evidence, disputed decisions, temporal corrections and rollback.
- [Provenance](PROVENANCE.json): source commit, original extraction hashes and current file hashes, with subsequent public changes recorded.

## Changes since extraction

The initial public extract contained 39 tests and preserved the selected source files unchanged. Two later passes changed it.

**Hardening (2026-09-30).** Fixed two issues found during review and added three regression tests:

- Invalid taxonomies are rejected before any database writes. Configuration and immutability guards are installed in one transaction, and failed initialization closes its connection. A file-backed test checks that a corrected taxonomy can initialize normally after rejected cyclic or dangling input.
- Query instants now follow the same explicit-UTC contract as stored validity boundaries. Use an ISO8601 timestamp ending in `Z` or `+00:00` for `valid_at`; naive, non-UTC or malformed values raise `ContractError`. Tests check invalid inputs, equivalent UTC forms and the same query under different host timezones.

**Readability refactor (2026-10-20).** No behaviour change. The single module became the typed `mesh_governance` package above; action handlers use snake_case names and are dispatched through an explicit table keyed by the public action names; public classes and methods have docstrings; the code is formatted and linted with ruff. The tests moved to `tests/` with only their import line changed and formatting applied: the same 42 tests and assertions. The import path is now `mesh_governance` instead of `mesh_ontology`; `MeshStore`, `command`, `Principal` and `ContractError` keep their public method names and signatures.

These changes apply to this public sample; the original source commit remains recorded in the provenance file.

## Scope and limitations

This is a local SQLite library. Trusted host code must supply `Principal` identities and capabilities; the module does not authenticate remote users. Do not attach it directly to an unauthenticated mutation endpoint.

Matching a quotation and its hash establishes source consistency, not the truth of an interpretation. The tests explicitly demonstrate that an authentic quote can accompany an unsupported claim. Review is a separate operation, and an AI review does not count as human approval.

The sample runs without a model. It does not include the full Nexus interface, research pipeline, datasets, deployment configuration or original repository history. The tests establish the checked local behaviors, not distributed-database scalability or real-world classification accuracy.

## Project context

This is the governance core of Nexus / Mesh, a larger research tool I built. I
pulled it out on its own so the evidence, revision and review rules can be read
and run without the rest of the system.

## License

[MIT](LICENSE)

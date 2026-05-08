# CLAUDE.md

## Project Overview

target-ducklake is a Singer target for DuckLake, built on the Meltano Singer SDK. It loads data into DuckLake-backed storage (GCS, S3, or local) using DuckDB as the in-memory engine and a configurable catalog database (Postgres, SQLite, MySQL, or DuckDB).

## Commands

```bash
# Install dependencies
uv sync

# Run tests (no external database required)
uv run pytest tests/ -v

# Lint
ruff check --fix --exit-non-zero-on-fix --show-fixes

# Format
ruff format

# Type check
mypy

# Pre-commit hooks
pre-commit run --all-files

# Run the target
uv run target-ducklake --version
```

## Architecture

**Data flow:** Records → preprocess (Decimal→float) → flatten → batch → write parquet → insert/merge into DuckLake → cleanup temp files

Key files:
- `target_ducklake/target.py` — `Targetducklake(SQLTarget)`: config parsing, entry point
- `target_ducklake/sinks.py` — `ducklakeSink(SQLSink)`: record processing, batching, parquet temp files, load methods (append/merge/overwrite)
- `target_ducklake/connector.py` — `DuckLakeConnector(SQLConnector)`: DuckDB connection, DDL, insert/merge operations, centralized retry logic in `execute()`
- `target_ducklake/flatten.py` — Schema/record flattening, auto-timestamp casting, column name truncation
- `target_ducklake/parquet_utils.py` — PyArrow schema conversion, datetime normalization
- `target_ducklake/logger.py` — Logging config

## Code Style

- Ruff for linting (all rules enabled) and formatting
- Google-style docstrings
- mypy for type checking
- `from __future__ import annotations` in all modules
- Classes: PascalCase, functions: snake_case, constants: UPPER_SNAKE_CASE
- Private methods: leading underscore

## Testing

Tests are in `tests/test_comprehensive.py`. All tests are unit tests using mocks — no external database connections needed. Run with `uv run pytest tests/ -v`.

## Retry Logic

All queries go through `DuckLakeConnector.execute()`, which retries on two categories of transient errors with exponential backoff:

1. **`duckdb.IOException`** — covers Postgres catalog connectivity errors and S3/GCS storage errors (via `duckdb.HTTPException`, a subclass)
2. **`duckdb.Error` with transient message** — covers SSL/connection drops that DuckLake wraps in a plain `duckdb.Error` (e.g., "SSL connection has been closed unexpectedly"). Detected by `is_transient_error()` in `connector.py`, which checks against `_TRANSIENT_ERROR_PATTERNS`.

All other exceptions are raised immediately without retry.

- **Max retries:** 3 attempts (configurable via `max_retries` parameter)
- **Backoff:** Exponential — `2^attempt` seconds (2s, 4s, 8s)
- **Retried errors:** `duckdb.IOException` + `duckdb.Error` matching `_TRANSIENT_ERROR_PATTERNS`
- **Not retried:** Programming errors, type mismatches, constraint violations, non-transient `duckdb.Error`, etc.
- **DML safety:** `insert_into_table` and `merge_into_table` wrap operations in `BEGIN TRANSACTION` / `COMMIT`, so failed attempts are rolled back before retry
- **No reconnect needed:** On retry, we do NOT need to DETACH + re-ATTACH the DuckLake catalog. The DuckDB `postgres` extension uses a connection pool (`PostgresConnectionPool`) that calls `PQreset()` on bad connections when they are returned to the pool. After a failed query, the next retry gets a repaired or fresh Postgres connection automatically. See: [postgres_connection_pool.cpp#L93-L99](https://github.com/duckdb/duckdb-postgres/blob/main/src/storage/postgres_connection_pool.cpp#L93-L99)
- **`ducklake_max_retry_count` does NOT cover connectivity errors.** DuckLake's internal retry loop only retries concurrency conflicts (primary key/unique violations, conflict errors) during transaction commit. Postgres "Connection refused" errors are NOT retried by DuckLake — our `execute()` retry is the only layer handling these. See: [ducklake_transaction.cpp#L2518-L2531](https://github.com/duckdb/ducklake/blob/main/src/storage/ducklake_transaction.cpp#L2518-L2531)

## Column Name Truncation

When `catalog_type` is `postgres`, column names longer than 63 characters are automatically truncated to match PostgreSQL's identifier length limit (NAMEDATALEN). This is a workaround for [ducklake#619](https://github.com/duckdb/ducklake/issues/619).

- Controlled by `max_column_length` config (default 63, set to 0 to disable)
- Only applies when `catalog_type == "postgres"`
- Collision handling: if truncation produces duplicate names, columns are shortened further and given `_{i}` suffixes
- Implementation: `truncate_column_names()` in `flatten.py`, called via `ducklakeSink._apply_column_name_truncation()` in `sinks.py`
- The truncation mapping is also applied to `key_properties` and to each record in `process_record()`

## GCS Authentication (Definite-specific)

`ducklake` and `postgres` are always installed from `DEFINITE_EXTENSION_REPO` (`https://storage.googleapis.com/def-duckdb-extensions`), regardless of which startup-script path runs. Because those builds are unsigned, every DuckDB connection is created with `allow_unsigned_extensions=True`.

When `storage_type` is `GCS`, the connector supports two authentication modes:

| HMAC keys provided? | Behavior |
|---|---|
| Both `public_key` + `secret_key` set | **Legacy path** — Definite-hosted ducklake/postgres + `TYPE gcs` HMAC secret over httpfs |
| Neither set | **ADC path** — Definite-hosted ducklake/postgres/gcs + `TYPE GCP, PROVIDER credential_chain` secret (uses GKE service account) |

The ADC path (`_use_definite_gcp_credential_chain()`) exists because Definite stopped provisioning HMAC keys for new DuckLake integrations (2026-04-08). Key details:

- Adds `INSTALL gcs FROM DEFINITE_EXTENSION_REPO` on top of the always-installed ducklake/postgres — the public httpfs-based ducklake doesn't support `TYPE GCP` secrets
- Data paths are rewritten from `gs://` to `gcss://` (required by the native `gcs` extension)
- Mirrors the canonical pattern from defapi `integration_config.py:135-146`
- Implementation: `_build_gcp_credential_chain_script()` in `connector.py`, with shared `_build_attach_statement()` for the ATTACH logic
- S3 and local storage paths still install ducklake/postgres from `DEFINITE_EXTENSION_REPO` but otherwise behave like before

### Why `SET GLOBAL` for `pg_pool_*` (not plain `SET`)

The startup script uses `SET GLOBAL pg_pool_max_connections=64;` (and the other `pg_pool_*` options) instead of plain `SET`. This is required on `duckdb-postgres` builds older than [commit f81dd35](https://github.com/duckdb/duckdb-postgres/commit/f81dd35) (2026-04-20):

- Before that fix, `pg_pool_*` options were registered with default (SESSION) scope. Plain `SET pg_pool_*` would only update the user's current DuckDB session.
- DuckLake's internal child `Connection` that runs the postgres `ATTACH` does **not** inherit user-session settings — it reads pg_pool_* from the GLOBAL scope only.
- Result: with plain `SET`, the postgres extension's connection pool is built using defaults (`pg_pool_max_connections=10`, no reaper thread), regardless of what the script asked for.

`SET GLOBAL` writes to the global scope, so DuckLake's child connection sees the configured values when it builds the pool. We can switch back to plain `SET` once we're guaranteed to be on a post-f81dd35 build — until then, keep `GLOBAL`.

### Startup script ordering (do not reorder)

The shared lines emitted by `_common_setup_lines()` rely on a specific order that DuckDB's extension manager and the SQL semantics enforce:

1. **`LOAD postgres;` (Definite fork) must come before the first `ATTACH` that references a postgres-typed catalog** — including the metadata `ATTACH 'postgres:...'` that DuckLake issues during catalog initialization. DuckDB's `ExtensionManager::BeginLoad` short-circuits on already-loaded extensions ([`extension_manager.cpp:73-110`](https://github.com/duckdb/duckdb/blob/main/src/main/extension_manager.cpp#L73-L110)), and `loaded_extensions_info` is keyed only by extension name (no repo/version). Once postgres is loaded, no later `LOAD postgres` or `INSTALL postgres FROM <other-repo>` swaps the in-memory binary — `INSTALL` only writes to disk ([`extension_install.cpp`](https://github.com/duckdb/duckdb/blob/main/src/main/extension/extension_install.cpp)). So the *first* postgres binary the manager registers is the one bound for the lifetime of that `DatabaseInstance`. If `ATTACH` triggers `TryAutoLoadExtension` first ([`database_manager.cpp:174-187`](https://github.com/duckdb/duckdb/blob/main/src/main/database_manager.cpp#L174-L187)), DuckDB may pull upstream postgres instead of our Definite build, and `META_ROLE` (which exists only on the Definite fork) silently stops working.

2. `LOAD postgres;` must come before any `SET ... pg_pool_*` statement — the pg_pool_* options are registered by the postgres extension and don't exist as settings until it's loaded.

3. `SET GLOBAL pg_pool_*` must come before `ATTACH 'ducklake:postgres:...'` — DuckLake's internal child connection that runs the ATTACH reads pg_pool_* once at attach time. Setting them afterward has no effect on the pool that's already been built.

4. `ATTACH` is appended by the caller after `_common_setup_lines()` returns.

Net rule: **`LOAD postgres` first, before everything else that touches postgres state.** Tests check substring presence, not order, so reordering will pass CI and silently break META_ROLE and/or pg_pool_* in production. To recover from a wrong-binary load you have to create a fresh `DatabaseInstance` (close + reopen the connection) — there is no in-process swap.

## Branch Model: `main` vs `definite`

This repo has two long-lived branches:

- **`main`** — public, general-purpose target-ducklake. External users depend on it.
- **`definite`** — Definite-internal branch. Adds the GCS ADC path (`_use_definite_gcp_credential_chain`, `DEFINITE_EXTENSION_REPO`, `gcss://` URI rewrite) and the `meta_role` config. Tracks `main` as upstream.

### Branch naming

- **`def/<topic>`** — Definite-only branches (PR'd into `definite`).
- **`general/<topic>`** — branches with shared changes that go into both `main` and `definite` (PR'd into `main` first, then synced into `definite`).
- **`sync/main-into-definite-YYYY-MM-DD`** — sync branches that bring `main` into `definite`.

The prefix makes the intended PR base obvious from the branch name and reduces the risk of `gh pr create` defaulting to the wrong base.

### Decision tree

```text
Is the change Definite-specific (touches Definite-only code, only useful to Definite)?
├── YES → branch `def/<topic>` off `definite`, PR into `definite`. Done.
└── NO (shared)
    ├── 1. branch `general/<topic>` off `main`, PR into `main`, merge.
    └── 2. then branch `sync/main-into-definite-YYYY-MM-DD` off `definite`,
           `git merge origin/main`, resolve conflicts (keep BOTH the new shared
           change AND the Definite-only code), PR into `definite`.
```

### Scenario 1: Definite-only change

```bash
git checkout definite && git pull
git checkout -b def/some-feature
# edits + commit
git push -u origin def/some-feature
gh pr create --base definite --title "..." --body "..."
```

The critical flag is `--base definite`. The default is `main`, so it's easy to accidentally PR into the public branch.

### Scenario 2: Shared change (both branches)

**Part A — land on `main`:**

```bash
git checkout main && git pull
git checkout -b general/some-fix
# edits + commit
git push -u origin general/some-fix
gh pr create --base main --title "..." --body "..."
# (review + merge via GitHub UI)
```

**Part B — sync into `definite`:**

```bash
git checkout definite && git pull
git checkout -b sync/main-into-definite-YYYY-MM-DD
git merge origin/main
# resolve any conflicts, then:
git push -u origin sync/main-into-definite-YYYY-MM-DD
gh pr create --base definite --title "sync main into definite" --body "..."
```

### Conflict-resolution rules during sync

When `git merge origin/main` hits a conflict during a Scenario 2 sync:

- **Conflict in a Definite-specific block** (e.g., shared change touches `connector.py` near `_build_gcp_credential_chain_script`): resolve by hand, keeping BOTH the new shared change AND the Definite block. Do NOT `--ours` here — that drops the shared change.
- **Conflict in a file with no Definite code**: just resolve the textual conflict normally.

### Gotchas

- **Always check `--base` on PRs.** A shared change PR'd into `definite` will never reach `main` and won't help upstream users.
- **Do not `git merge -s ours origin/main` for Scenario 2 syncs.** That strategy was used exactly once, for the initial split (PR #66, where `main`'s removal commit had to be absorbed without re-applying the deletion). Using it for shared-change syncs would silently drop real shared changes.
- **Sync promptly.** The longer `definite` lags `main`, the more conflicts pile up. Aim for same-day or weekly batched syncs.
- **Do not squash-merge sync PRs.** Squashing destroys the merge-commit metadata git uses to recognize main↔definite as related, which makes the *next* sync conflict-heavy. Use "Create a merge commit" on sync PRs (regular feature PRs can squash).
- **If a Definite-only commit later turns out useful upstream:** cherry-pick it onto a `general/` branch off `main`, PR into `main`, then sync as Scenario 2.

## Key Dependencies

- `duckdb>=1.5.2,<1.6.0`, `singer-sdk~=0.46.4`, `pyarrow>=20.0.0`, `polars>=1.31.0`, `sqlalchemy>=2.0.41`

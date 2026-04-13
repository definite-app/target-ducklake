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

When `storage_type` is `GCS`, the connector supports two authentication modes:

| HMAC keys provided? | Behavior |
|---|---|
| Both `public_key` + `secret_key` set | **Legacy path** — public ducklake extension + `TYPE gcs` HMAC secret |
| Neither set | **ADC path** — Definite-hosted ducklake/gcs extensions + `TYPE GCP, PROVIDER credential_chain` secret (uses GKE service account) |

The ADC path (`_use_definite_gcp_credential_chain()`) exists because Definite stopped provisioning HMAC keys for new DuckLake integrations (2026-04-08). Key details:

- Extensions are installed from `DEFINITE_EXTENSION_REPO` (`https://storage.googleapis.com/def-duckdb-extensions`) — the public httpfs-based ducklake doesn't support `TYPE GCP` secrets
- `allow_unsigned_extensions` is enabled on the DuckDB connection for the Definite-hosted builds
- Data paths are rewritten from `gs://` to `gcss://` (required by the native `gcs` extension)
- Mirrors the canonical pattern from defapi `integration_config.py:135-146`
- Implementation: `_build_gcp_credential_chain_script()` in `connector.py`, with shared `_build_attach_statement()` for the ATTACH logic
- S3 and local storage paths are completely unchanged

## Key Dependencies

- `duckdb==1.5.1`, `singer-sdk~=0.46.4`, `pyarrow>=20.0.0`, `polars>=1.31.0`, `sqlalchemy>=2.0.41`

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
- `target_ducklake/flatten.py` — Schema/record flattening, auto-timestamp casting
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

All queries go through `DuckLakeConnector.execute()`, which retries on `duckdb.IOException` (covers Postgres catalog connectivity errors and S3/GCS storage errors via `duckdb.HTTPException`, a subclass). Non-IOException errors are raised immediately without retry.

- **Max retries:** 3 attempts (configurable via `max_retries` parameter)
- **Backoff:** Exponential — `2^attempt` seconds (2s, 4s, 8s)
- **Retried errors:** Only `duckdb.IOException` (transient infrastructure/connectivity failures)
- **Not retried:** Programming errors, type mismatches, constraint violations, etc.
- **DML safety:** `insert_into_table` and `merge_into_table` wrap operations in `BEGIN TRANSACTION` / `COMMIT`, so failed attempts are rolled back before retry
- **No reconnect needed:** On retry, we do NOT need to DETACH + re-ATTACH the DuckLake catalog. The DuckDB `postgres` extension uses a connection pool (`PostgresConnectionPool`) that calls `PQreset()` on bad connections when they are returned to the pool. After a failed query, the next retry gets a repaired or fresh Postgres connection automatically. See: [postgres_connection_pool.cpp#L93-L99](https://github.com/duckdb/duckdb-postgres/blob/main/src/storage/postgres_connection_pool.cpp#L93-L99)
- **`ducklake_max_retry_count` does NOT cover connectivity errors.** DuckLake's internal retry loop only retries concurrency conflicts (primary key/unique violations, conflict errors) during transaction commit. Postgres "Connection refused" errors are NOT retried by DuckLake — our `execute()` retry is the only layer handling these. See: [ducklake_transaction.cpp#L2518-L2531](https://github.com/duckdb/ducklake/blob/main/src/storage/ducklake_transaction.cpp#L2518-L2531)

## Key Dependencies

- `duckdb~=1.4.0`, `singer-sdk~=0.46.4`, `pyarrow>=20.0.0`, `polars>=1.31.0`, `sqlalchemy>=2.0.41`

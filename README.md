# target-ducklake

`target-ducklake` is a Singer target for ducklake. This target is in development and may not be stable. I built this with GCS in mind, but it should work with S3 and local storage, though this has not been tested extensively.

Supports append, merge, and overwrite load methods (default is merge). The load method can be configured using the `load_method` setting. If no key properties are provided and `overwrite_if_no_pk` is true, data will be overwritten. Otherwise, the configured load method determines the behavior.

## TODOs

- support deleting data from the target or setting _sdc_deleted_at field (example use-case: syncing from Postgres using log-based change data capture)

## Configuration

### Accepted Config Options

| Setting | Type | Required | Default | Description |
|---------|------|----------|---------|-------------|
| `catalog_url` | string | ✅ | - | URL connection string to your catalog database |
| `data_path` | string | ✅ | - | GCS, S3, or local folder path for data storage |
| `storage_type` | string | ✅ | `"local"` | Type of storage: GCS, S3, or local |
| `catalog_type` | string | ✅ | `"postgres"` | Type of catalog database: postgres, sqlite, mysql, or duckdb. See <https://ducklake.select/docs/stable/duckdb/usage/choosing_a_catalog_database> |
| `meta_schema` | string | ❌ | - | Schema name in the catalog database to use for Ducklake metadata tables. If not provided will use the default schema for the catalog database (eg `public` for Postgres) |
| `public_key` | string | ❌ | - | Public key for private GCS and S3 storage authentication (optional) |
| `secret_key` | string | ❌ | - | Secret key for private GCS and S3 storage authentication (optional) |
| `region` | string | ❌ | - | AWS region for S3 storage type (required when using S3 with explicit credentials) |
| `default_target_schema` | string | ❌ | - | Default database schema where data should be written. If not provided schema will attempt to be inferred from the stream name (inferring schema only works for database extractors ) |
| `target_schema_prefix` | string | ❌ | - | Prefix to add to the target schema name. If not provided, no prefix will be added |
| `add_record_metadata` | boolean | ❌ | `false` | When True, automatically adds Singer Data Capture (SDC) metadata columns to target tables |
| `load_method` | string | ❌ | `"merge"` | Method to use for loading data into the target table: append, merge, or overwrite |
| `flatten_max_level` | integer | ❌ | `0` | Maximum depth for flattening nested fields. Set to 0 to disable flattening |
| `temp_file_dir` | string | ❌ | `"temp_files/"` | Directory path for storing temporary parquet files |
| `max_batch_size` | integer | ❌ | `10000` | Maximum number of records to process in a single batch |
| `fallback_on_insert_error` | boolean | ❌ | `false` | When true, quarantines failed batches to Parquet instead of failing immediately |
| `fallback_dir` | string | ❌ | `<data_path>/_failed_batches/` | Directory where quarantined batch artifacts are written |
| `fallback_include_payload` | boolean | ❌ | `true` | Also write the raw Singer RECORD payloads as `records.jsonl` when quarantining |
| `advance_state_on_fallback` | boolean | ❌ | `false` | Continue processing and advance state after quarantine instead of re-raising |
| `partition_fields` | object | ❌ | - | Object mapping stream names to arrays of partition column definitions. Each stream key maps directly to an array of column definitions |
| `auto_cast_timestamps` | boolean | ❌ | `false` | When True, automatically attempts to cast timestamp-like fields to timestamp types in ducklake |
| `sanitize_timezones` | boolean | ❌ | follows `auto_cast_timestamps` | Normalize timezone-aware values and UTC±HH:MM strings before Arrow ingestion |
| `sanitize_dates` | boolean | ❌ | `false` | Normalize date and datetime-like strings to ISO-8601 before ingestion |
| `dates_to_varchar` | boolean | ❌ | `false` | Force timestamp-like columns to VARCHAR to avoid timezone and formatting issues |
| `dates_to_varchar_streams` | array | ❌ | - | List of stream names whose timestamp-like columns should be coerced to VARCHAR |
| `dates_to_varchar_columns` | object | ❌ | - | Mapping of stream name to explicit column names to coerce to VARCHAR |
| `dates_to_varchar_glob` | array | ❌ | - | Glob patterns that match column names to coerce to VARCHAR |
| `validate_records` | boolean | ❌ | `false` | Whether to validate the schema of the incoming streams |
| `overwrite_if_no_pk` | boolean | ❌ | `false` | When True, truncates the target table before inserting records if no primary keys are defined in the stream. Overrides load_method. |

### Example Meltano YAML Configuration

```yaml
plugins:
    # ... other plugins ...
    loaders:
    - name: target-ducklake
      namespace: target_ducklake
      pip_url: https://github.com/definite-app/target-ducklake.git
      config:
        catalog_url: postgres://test:test@localhost:5432/test_db
        data_path: gs://test-bucket/test-path
        storage_type: GCS # Optional (default local)
        public_key: GOOG1234567890 # Optional (required for private GCS and S3 storage)
        secret_key: GOOG1234567890 # Optional (required for private GCS and S3 storage)
        default_target_schema: my_schema # Optional (default None)
        target_schema_prefix: my_prefix # Optional
        max_batch_size: 10000 # Optional (default 10000)
        add_record_metadata: true # Optional
        load_method: merge # Optional (default merge, options: append, merge, overwrite)
        auto_cast_timestamps: true # Optional
        overwrite_if_no_pk: true # Optional (default false)
        partition_fields: {"my_stream": [{"column_name": "created_at", "type": "timestamp", "granularity": ["year", "month"]}]} # Optional
        fallback_on_insert_error: true # Optional - quarantine failed batches instead of failing immediately
        fallback_dir: ${MELTANO_PROJECT_ROOT}/.ducklake/_failed_batches # Optional override
        fallback_include_payload: true # Optional - also write raw Singer payloads
        sanitize_timezones: true # Optional - normalize UTC±HH:MM style strings
        sanitize_dates: true # Optional - rewrite non-ISO date strings
        dates_to_varchar: true # Optional - force timestamp-like columns to VARCHAR
```

### Resilient loading

When `fallback_on_insert_error` is enabled, each failing batch is quarantined to
`<fallback_dir>/<stream>/<UTC timestamp>/` with:

- `batch.parquet` – the sanitized records written out as Parquet.
- `meta.json` – error metadata, schema details, and relevant configuration.
- `records.jsonl` – optional Singer RECORD payloads (enabled by
  `fallback_include_payload`).

`sanitize_timezones` converts timezone-aware Python datetimes and `UTC±HH:MM`
style strings to UTC-naive ISO strings before Arrow ingestion. `sanitize_dates`
normalizes slash-separated or other non-ISO date strings to ISO-8601 before Arrow
ingestion. The `dates_to_varchar*` settings coerce timestamp-like columns to VARCHAR
so DuckDB never has to parse problematic formats. Each coerced column increments the
`target_ducklake_dates_to_varchar_columns_total{stream}` metric, while each
quarantined batch increments
`target_ducklake_fallback_batches_total{stream}`.

### Quarantine & Replay

Quarantined Parquet files can be replayed transactionally with DuckLake once the
bad data has been reviewed:

```sql
-- Setup
INSTALL ducklake; LOAD ducklake;

-- Attach your catalog (and data path) as usual
ATTACH 'ducklake:POSTGRES://…' AS dl (DATA_PATH 'gs://…');

-- Register quarantined file(s):
SELECT ducklake_add_data_files(
  'FACEBOOKADS.creatives',
  ['gs://def-ducklake/.../_failed_batches/creatives/2025-10-04T19-33-58Z/batch.parquet']
);
```

DuckLake registers (rather than copies) the files so ownership transfers to the
catalog and future compaction can clean them up.

### Partitioning

Partitioning is supported by providing a `partition_fields` configuration object. Each stream name maps to an array of column definitions.

Example:

```json
{
    "stream_name": [    
        {
            "column_name": "created_at", # The column name to partition on
            "type": "timestamp", # The partition type (timestamp or identifier)
            "granularity": ["year", "month"] # The granularity of the partition (only for timestamp type)
        }
    ],
    "stream_name_2": [
        {
            "column_name": "status", # The column name to partition on
            "type": "identifier", # The partition type (timestamp or identifier)
        }
    ]
}
```

A full list of supported settings and capabilities for this
target is available by running:

```bash
target-ducklake --about
```

### Ducklake Schema Migration From 0.1 to 0.2
Solution to this issue: https://github.com/duckdb/ducklake/issues/240
The SQL to manually migrate your catalog schema (assuming Postgres) can be found in the `schema_migration.sql` file.

### Configure using environment variables

This Singer target will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Authentication and Authorization

<!--
Developer TODO: If your target requires special access on the destination system, or any special authentication requirements, provide those here.
-->

## Usage

You can easily run `target-ducklake` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Target Directly

```bash
target-ducklake --version
target-ducklake --help
# Test using the "Smoke Test" tap:
tap-smoke-test | target-ducklake --config /path/to/target-ducklake-config.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

Prerequisites:

- Python 3.9+
- [uv](https://docs.astral.sh/uv/)

```bash
uv sync
```

### Create and Run Tests

Create tests within the `tests` subfolder and
then run:

```bash
# Run all unit tests (no database required)
uv run pytest tests/ -v
```

You can also test the `target-ducklake` CLI interface directly using `uv run`:

```bash
uv run target-ducklake --help
```

### Testing with [Meltano](https://meltano.com/)

_**Note:** This target will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

<!--
Developer TODO:
Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any "TODO" items listed in
the file.
-->

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd target-ducklake
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke target-ducklake --version

# OR run a test ELT pipeline with the Smoke Test sample tap:
meltano run tap-smoke-test target-ducklake
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the Meltano Singer SDK to
develop your own Singer taps and targets.

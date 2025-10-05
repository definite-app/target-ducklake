"""ducklake target class."""

from __future__ import annotations

import ast
import logging

from singer_sdk import typing as th
from singer_sdk.target_base import Target

from target_ducklake.sinks import (
    ducklakeSink,
)


class Targetducklake(Target):
    name = "target-ducklake"

    @property
    def config(self):
        """Get config dict with JSON string parsing for complex types."""
        config = dict(super().config)

        # Convert certain config values from strings to their respective types
        # This is required for certain config values that are passed in via env vars into Kubernetes pods
        # Kubernetes pods only support string values for env vars (at least in GCP)
        if "partition_fields" in config and isinstance(config["partition_fields"], str):
            partition_fields_str = config["partition_fields"]
            parsed_partition_fields = ast.literal_eval(partition_fields_str)
            config["partition_fields"] = parsed_partition_fields
            logging.info(
                f"Successfully parsed partition_fields from string to object: {parsed_partition_fields}"
            )

        def _parse_literal_if_string(key: str) -> None:
            if key in config and isinstance(config[key], str):
                try:
                    config[key] = ast.literal_eval(config[key])
                except (ValueError, SyntaxError):
                    logging.warning(
                        "Failed to parse %s from string; keeping original value", key
                    )

        for literal_key in (
            "dates_to_varchar_streams",
            "dates_to_varchar_columns",
            "dates_to_varchar_glob",
        ):
            _parse_literal_if_string(literal_key)

        if "max_batch_size" in config:
            logging.warning(f"max_batch_size: {config.get('max_batch_size')}")
            config["max_batch_size"] = int(config.get("max_batch_size", 10000))
        if "flatten_max_level" in config:
            config["flatten_max_level"] = int(config.get("flatten_max_level", 0))

        bool_keys = (
            "validate_records",
            "overwrite_if_no_pk",
            "auto_cast_timestamps",
            "fallback_on_insert_error",
            "fallback_include_payload",
            "advance_state_on_fallback",
            "sanitize_timezones",
            "sanitize_dates",
            "dates_to_varchar",
        )
        for key in bool_keys:
            if key in config and isinstance(config[key], str):
                config[key] = config[key].lower() == "true"

        if "sanitize_timezones" not in config:
            config["sanitize_timezones"] = config.get("auto_cast_timestamps", False)
        if "sanitize_dates" not in config:
            config["sanitize_dates"] = False
        return config

    config_jsonschema = th.PropertiesList(
        th.Property(
            "catalog_url",
            th.StringType(nullable=False),
            secret=True,  # Flag config as protected.
            title="Catalog URL",
            description="URL connection string to your catalog database",
        ),
        th.Property(
            "catalog_type",
            th.StringType(
                allowed_values=["postgres", "sqlite", "mysql", "duckdb"],
                nullable=False,
            ),
            default="postgres",
            title="Catalog Type",
            description="Type of catalog database: postgres, sqlite, mysql, or duckdb",
        ),
        th.Property(
            "meta_schema",
            th.StringType(nullable=True),
            title="Meta Schema",
            description="Schema name in the catalog database to use for Ducklakemetadata tables",
        ),
        th.Property(
            "data_path",
            th.StringType(nullable=False),
            title="Data Path",
            description="GCS, S3, or local folder path for data storage",
        ),
        th.Property(
            "storage_type",
            th.StringType(
                allowed_values=["GCS", "S3", "local"],
                nullable=False,
            ),
            default="local",
            title="Storage Type",
            description="Type of storage: GCS, S3, or local",
        ),
        th.Property(
            "public_key",
            th.StringType(nullable=True),
            title="Public Key",
            description="Public key for private GCS and S3 storage authentication (optional)",
        ),
        th.Property(
            "secret_key",
            th.StringType(nullable=True),
            secret=True,  # Flag config as protected.
            title="Secret Key",
            description="Secret key for private GCS and S3 storage authentication (optional)",
        ),
        th.Property(
            "region",
            th.StringType(nullable=True),
            title="Region",
            description="AWS region for S3 storage type (required when using S3 with explicit credentials)",
        ),
        th.Property(
            "default_target_schema",
            th.StringType(nullable=True),
            title="Default Target Schema Name",
            description=(
                "Default database schema where data should be written. "
                "If not provided schema will attempt to be derived from the stream name "
                "(e.g. database taps provide schema name in the stream name)."
            ),
        ),
        th.Property(
            "target_schema_prefix",
            th.StringType(nullable=True),
            title="Target Schema Prefix",
            description=(
                "Prefix to add to the target schema name. "
                "If not provided, no prefix will be added."
                "May be useful if target schema name is inferred from the stream name "
                "and you want to add a prefix to the schema name."
            ),
        ),
        th.Property(
            "add_record_metadata",
            th.BooleanType(),
            default=False,
            title="Add Singer Record Metadata",
            description=(
                "When True, automatically adds Singer Data Capture (SDC) metadata columns to target tables: "
            ),
        ),
        th.Property(
            "load_method",
            th.StringType(allowed_values=["append", "merge", "overwrite"]),
            title="Load Method",
            description="Method to use for loading data into the target table: append, merge, or overwrite",
        ),
        th.Property(
            "flatten_max_level",
            th.CustomType(
                {"oneOf": [{"type": "string"}, {"type": "integer", "minimum": 0}]}
            ),
            default=0,
            title="Flattening Max Level",
            description="Maximum depth for flattening nested fields. Set to 0 to disable flattening.",
        ),
        th.Property(
            "temp_file_dir",
            th.StringType(),
            default="temp_files/",
            title="Temporary File Directory",
            description="Directory path for storing temporary parquet files",
        ),
        th.Property(
            "max_batch_size",
            th.CustomType(
                {
                    "oneOf": [
                        {
                            "type": "string",
                            "description": "String representation of max batch size",
                        },
                        {"type": "integer", "minimum": 1},
                    ]
                }
            ),
            default=10000,
            title="Max Batch Size",
            description="Maximum number of records to process in a single batch",
        ),
        th.Property(
            "fallback_on_insert_error",
            th.BooleanType(),
            default=False,
            title="Quarantine Failed Batches",
            description=(
                "When True, batches that fail during insert/merge are written to a fallback"
                " directory for later replay instead of immediately failing."
            ),
        ),
        th.Property(
            "fallback_dir",
            th.StringType(),
            title="Fallback Directory",
            description=(
                "Directory where quarantined batches are written when fallback_on_insert_error"
                " is enabled. Defaults to <data_path>/_failed_batches/."
            ),
        ),
        th.Property(
            "fallback_include_payload",
            th.BooleanType(),
            default=True,
            title="Include Raw Payload in Fallback",
            description=(
                "When True, also write the raw Singer RECORD payloads as records.jsonl in the"
                " fallback directory."
            ),
        ),
        th.Property(
            "advance_state_on_fallback",
            th.BooleanType(),
            default=False,
            title="Advance State After Fallback",
            description=(
                "When True, the target continues processing and advances state after writing a"
                " quarantined batch. When False, the original error is raised to halt the run."
            ),
        ),
        th.Property(
            "sanitize_timezones",
            th.BooleanType(),
            title="Sanitize Timezones",
            description=(
                "Normalize timezone-aware values and UTC±HH:MM strings to UTC-naive timestamps"
                " prior to Arrow ingestion. Defaults to the value of auto_cast_timestamps."
            ),
        ),
        th.Property(
            "sanitize_dates",
            th.BooleanType(),
            default=False,
            title="Sanitize Date Strings",
            description=(
                "Normalize date and datetime-like strings to ISO-8601 so Arrow and DuckLake "
                "ingest them consistently."
            ),
        ),
        th.Property(
            "dates_to_varchar",
            th.BooleanType(),
            default=False,
            title="Coerce Dates to VARCHAR",
            description=(
                "When True, timestamp-like columns are coerced to VARCHAR prior to ingestion"
                " to avoid timezone and format errors."
            ),
        ),
        th.Property(
            "dates_to_varchar_streams",
            th.ArrayType(th.StringType()),
            title="Streams for Dates→VARCHAR",
            description=(
                "List of stream names whose timestamp-like columns should be coerced to"
                " VARCHAR when dates_to_varchar is enabled."
            ),
        ),
        th.Property(
            "dates_to_varchar_columns",
            th.ObjectType(additional_properties=th.ArrayType(th.StringType())),
            title="Specific Columns for Dates→VARCHAR",
            description=(
                "Mapping of stream name to specific column names that should be coerced to"
                " VARCHAR."
            ),
        ),
        th.Property(
            "dates_to_varchar_glob",
            th.ArrayType(th.StringType()),
            title="Column Patterns for Dates→VARCHAR",
            description=(
                "List of glob patterns applied to column names across all streams to mark"
                " columns that should be coerced to VARCHAR."
            ),
        ),
        th.Property(
            "partition_fields",
            th.CustomType(
                {
                    "oneOf": [
                        {
                            "type": "string",
                            "description": "JSON string representation of partition fields object",
                        },
                        {
                            "type": "object",
                            "additionalProperties": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "column_name": {"type": "string"},
                                        "type": {
                                            "type": "string",
                                            "enum": ["timestamp", "identifier"],
                                        },
                                        "granularity": {
                                            "type": "array",
                                            "items": {
                                                "type": "string",
                                                "enum": [
                                                    "year",
                                                    "month",
                                                    "day",
                                                    "hour",
                                                ],
                                            },
                                        },
                                    },
                                    "required": ["column_name", "type"],
                                },
                            },
                        },
                        {"type": "null"},
                    ]
                }
            ),
            nullable=True,
            title="Partition Fields",
            description=(
                "Object mapping stream names to arrays of partition column definitions. "
                "Each stream key maps directly to an array of column definitions. "
                "Can be provided as a JSON string or object."
            ),
        ),
        th.Property(
            "auto_cast_timestamps",
            th.BooleanType(),
            default=False,
            title="Auto Cast Timestamps",
            description=(
                "When True, automatically attempts to cast timestamp-like fields to timestamp types in ducklake."
            ),
        ),
        th.Property(
            "validate_records",
            th.CustomType(
                {
                    "oneOf": [
                        {
                            "type": "string",
                            "description": "String representation of validate records",
                        },
                        {"type": "boolean"},
                    ]
                }
            ),
            default=False,
            title="Validate Records",
            description="Whether to validate the schema of the incoming streams.",
        ),
        th.Property(
            "overwrite_if_no_pk",
            th.CustomType(
                {
                    "oneOf": [
                        {
                            "type": "string",
                            "description": "String representation of overwrite if no primary key, overrides load_method.",
                        },
                        {"type": "boolean"},
                    ]
                }
            ),
            default=False,
            title="Overwrite If No Primary Key",
            description="When True, truncates the target table before inserting records if no primary keys are defined in the stream. Overrides load_method.",
        ),
    ).to_dict()

    default_sink_class = ducklakeSink


if __name__ == "__main__":
    Targetducklake.cli()

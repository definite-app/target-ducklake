"""ducklake target sink class, which handles writing streams."""

from __future__ import annotations

import fnmatch
import json
import os
import re
import shutil
import traceback
from collections.abc import Sequence
from datetime import datetime, time, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow.parquet as pq
from dateutil import parser as date_parser
from singer_sdk import Target

# from singer_sdk.connectors import SQLConnector
from singer_sdk.sinks import BatchSink

from target_ducklake.connector import DuckLakeConnector
from target_ducklake.flatten import (
    CustomJSONEncoder,
    TIMESTAMP_COLUMN_NAMES,
    flatten_record,
    flatten_schema,
)
from target_ducklake.parquet_utils import (
    concat_tables,
    flatten_schema_to_pyarrow_schema,
)

UTC_OFFSET_PATTERN = re.compile(r"^(?P<prefix>.+?)\s*UTC(?P<offset>[+-]\d{2}:\d{2})$")
DATE_CANDIDATE_PATTERN = re.compile(r"\d{1,4}[-/]\d{1,2}[-/]\d{1,4}")
DEFAULT_TIMESTAMP_GLOBS = ("*_at", "*_time", "*timestamp*", "*_date")
FALLBACK_PARQUET_FILENAME = "batch.parquet"
FALLBACK_META_FILENAME = "meta.json"
FALLBACK_RECORDS_FILENAME = "records.jsonl"


class ducklakeSink(BatchSink):
    """ducklake target sink class."""

    def __init__(
        self,
        target: Target,
        stream_name: str,
        schema: dict,
        key_properties: Sequence[str] | None,
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)
        self.temp_file_dir = self.config.get("temp_file_dir", "temp_files/")
        self.files_saved = 0

        self.fallback_on_insert_error = self.config.get(
            "fallback_on_insert_error", False
        )
        self.fallback_include_payload = self.config.get(
            "fallback_include_payload", True
        )
        self.advance_state_on_fallback = self.config.get(
            "advance_state_on_fallback", False
        )
        data_path = self.config.get("data_path", "")
        default_fallback_dir = (
            os.path.join(data_path, "_failed_batches") if data_path else "_failed_batches"
        )
        self.fallback_dir = self.config.get("fallback_dir") or default_fallback_dir

        self.flatten_max_level = self.config.get("flatten_max_level", 0)
        self.auto_cast_timestamps = self.config.get("auto_cast_timestamps", False)
        self.sanitize_timezones = self.config.get(
            "sanitize_timezones", self.auto_cast_timestamps
        )
        self.sanitize_dates = self.config.get("sanitize_dates", False)

        self.dates_to_varchar_enabled = self.config.get("dates_to_varchar", False)
        streams_cfg = self.config.get("dates_to_varchar_streams", []) or []
        self.dates_to_varchar_streams = set(streams_cfg)
        columns_cfg = self.config.get("dates_to_varchar_columns", {}) or {}
        self.dates_to_varchar_columns = {
            stream: set(columns) for stream, columns in columns_cfg.items()
        }
        glob_cfg = self.config.get("dates_to_varchar_glob", []) or []
        self.dates_to_varchar_glob = list(glob_cfg)

        self._varchar_columns: set[str] = set()
        self._columns_warned_for_varchar: set[str] = set()
        self._tz_warning_emitted = False
        self._date_warning_emitted: set[str] = set()
        self._emitted_metrics: list[tuple[str, dict[str, Any]]] = []

        # NOTE: we probably don't need all this logging, but useful for debugging while target is in development
        # Log original schema for debugging
        self.logger.info(f"Original schema for stream '{stream_name}': {self.schema}")

        # Use the connector for database operations
        self.connector = DuckLakeConnector(dict(self.config))

        # Create pyarrow and Ducklake schemas
        self.flatten_schema = flatten_schema(
            self.schema,
            max_level=self.flatten_max_level,
            auto_cast_timestamps=self.auto_cast_timestamps,
        )
        self._apply_dates_to_varchar_overrides()
        # Log flattened schema for debugging
        self.logger.info(
            f"Flattened schema for stream '{stream_name}': {self.flatten_schema}"
        )
        self.pyarrow_schema = flatten_schema_to_pyarrow_schema(self.flatten_schema)
        self.ducklake_schema = self.connector.json_to_ducklake_schema(
            self.flatten_schema
        )
        # Log pyarrow and ducklake schemas for debugging
        self.logger.info(
            f"PyArrow schema for stream '{stream_name}': {self.pyarrow_schema}"
        )
        self.logger.info(
            f"DuckLake schema for stream '{stream_name}': {self.ducklake_schema}"
        )

        # Initialize partition fields (if any)
        partition_fields = self.config.get("partition_fields")
        self.partition_fields = (
            partition_fields.get(self.stream_name) if partition_fields else None
        )

        if not self.config.get("load_method"):
            self.logger.info(f"No load method provided for {self.stream_name}, using default merge")
            self.load_method = "merge"
        else:
            self.logger.info(f"Load method {self.config.get('load_method')} provided for {self.stream_name}")
            self.load_method = self.config.get("load_method")

        # Determine if table should be overwritten
        if not self.key_properties and self.config.get("overwrite_if_no_pk", False):
            self.logger.info(
                f"Load method is overwrite for {self.stream_name}: no key properties and overwrite_if_no_pk is True"
            )
            self.should_overwrite_table = True
        elif self.load_method == "overwrite":
            self.logger.info(f"Load method is overwrite for {self.stream_name}")
            self.should_overwrite_table = True
        else:
            self.logger.info(
                f"Load method is {self.load_method} for {self.stream_name}"
            )
            self.should_overwrite_table = False

    def _apply_dates_to_varchar_overrides(self) -> None:
        """Mutate the flattened schema to coerce timestamp-like columns to string."""

        if not (
            self.dates_to_varchar_enabled
            or self.dates_to_varchar_streams
            or self.dates_to_varchar_columns
            or self.dates_to_varchar_glob
        ):
            return

        coerced_columns: list[str] = []
        for column_name, schema_entry in list(self.flatten_schema.items()):
            if self._should_coerce_column(column_name, schema_entry):
                self._coerce_schema_entry_to_string(column_name, schema_entry)
                coerced_columns.append(column_name)

        if coerced_columns:
            self.logger.info(
                "Coercing columns to VARCHAR for stream %s: %s",
                self.stream_name,
                ", ".join(sorted(coerced_columns)),
            )

    def _should_coerce_column(self, column_name: str, schema_entry: dict) -> bool:
        """Determine if a column should be coerced to VARCHAR."""

        stream_specific = self.dates_to_varchar_columns.get(self.stream_name, set())
        if column_name in stream_specific:
            return True

        for pattern in self.dates_to_varchar_glob:
            if fnmatch.fnmatch(column_name, pattern):
                return True

        if self.stream_name in self.dates_to_varchar_streams and self._is_timestamp_like(
            column_name, schema_entry
        ):
            return True

        if self.dates_to_varchar_enabled and self._is_timestamp_like(
            column_name, schema_entry
        ):
            return True

        return False

    def _is_timestamp_like(self, column_name: str, schema_entry: dict) -> bool:
        """Best-effort detection of timestamp-like columns."""

        fmt = schema_entry.get("format")
        if isinstance(fmt, str) and fmt in {"date-time", "date"}:
            return True

        entry_type = schema_entry.get("type")
        if isinstance(entry_type, list):
            types = {str(t).lower() for t in entry_type}
        elif isinstance(entry_type, str):
            types = {entry_type.lower()}
        else:
            types = set()

        lower_name = column_name.lower()
        if lower_name in TIMESTAMP_COLUMN_NAMES:
            return True

        for pattern in DEFAULT_TIMESTAMP_GLOBS:
            if fnmatch.fnmatch(lower_name, pattern):
                return True

        if types.intersection({"string", "integer", "number"}):
            if lower_name.endswith("_at") or lower_name.endswith("_time"):
                return True

        return False

    def _coerce_schema_entry_to_string(
        self, column_name: str, schema_entry: dict
    ) -> None:
        """Update schema entry to coerce to VARCHAR and record metrics."""

        entry_type = schema_entry.get("type")
        if isinstance(entry_type, list):
            normalized_types = [str(t).lower() for t in entry_type]
            has_null = "null" in normalized_types
        elif isinstance(entry_type, str):
            normalized_types = [entry_type.lower()]
            has_null = entry_type.lower() == "null"
        else:
            normalized_types = []
            has_null = False

        if has_null:
            schema_entry["type"] = ["null", "string"]
        else:
            schema_entry["type"] = "string"

        original_format = schema_entry.get("format")
        schema_entry.pop("format", None)

        self.flatten_schema[column_name] = schema_entry
        if column_name not in self._varchar_columns:
            self._varchar_columns.add(column_name)
            self._increment_metric(
                "target_ducklake_dates_to_varchar_columns_total",
                {"stream": self.stream_name},
            )

        if (
            original_format in {"date", "date-time"}
            and column_name not in self._columns_warned_for_varchar
        ):
            self.logger.warning(
                "Coercing %s.%s declared with format '%s' to VARCHAR due to configuration",
                self.stream_name,
                column_name,
                original_format,
            )
            self._columns_warned_for_varchar.add(column_name)

    def _sanitize_records(self, records: list[dict[str, Any]]) -> None:
        """Normalize timezone and date values in-place for the provided records."""

        if not (self.sanitize_timezones or self.sanitize_dates):
            return

        for record in records:
            for key, value in list(record.items()):
                sanitized = self._sanitize_value(value, key)
                record[key] = sanitized

    def _sanitize_value(self, value: Any, column_name: str) -> Any:
        """Sanitize a single field value for timezone and date anomalies."""

        if isinstance(value, datetime):
            if value.tzinfo is not None:
                sanitized_dt = value.astimezone(timezone.utc).replace(tzinfo=None)
                return sanitized_dt.isoformat()
            if self.sanitize_dates:
                return value.isoformat()
            return value

        if isinstance(value, str):
            sanitized_str = value
            if self.sanitize_timezones:
                normalized = self._normalize_non_iana_timezone(sanitized_str)
                if normalized is not None:
                    if not self._tz_warning_emitted:
                        self.logger.warning(
                            "Non-IANA timezone detected in stream %s (column %s) with sample '%s'; coercing to UTC",
                            self.stream_name,
                            column_name,
                            value,
                        )
                        self._tz_warning_emitted = True
                    sanitized_str = normalized
            if self.sanitize_dates:
                normalized_date = self._normalize_date_string(sanitized_str, column_name)
                if normalized_date is not None:
                    sanitized_str = normalized_date
            return sanitized_str

        return value

    def _normalize_non_iana_timezone(self, value: str) -> str | None:
        """Return a UTC-normalized ISO string for UTC±HH:MM patterns."""

        match = UTC_OFFSET_PATTERN.match(value.strip())
        if not match:
            return None

        prefix = match.group("prefix").strip()
        offset = match.group("offset")
        iso_candidate = f"{prefix}{offset}"
        try:
            parsed = datetime.fromisoformat(iso_candidate)
        except ValueError:
            return None

        if parsed.tzinfo is None:
            return None

        normalized = parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return normalized.isoformat()

    def _normalize_date_string(self, value: str, column_name: str) -> str | None:
        """Normalize common date strings to ISO-8601 for Arrow compatibility."""

        candidate = value.strip()
        if not candidate or not any(char.isdigit() for char in candidate):
            return None

        if not (DATE_CANDIDATE_PATTERN.search(candidate) or "T" in candidate or ":" in candidate):
            return None

        try:
            parsed = date_parser.parse(candidate)
        except (ValueError, OverflowError, TypeError):
            return None

        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)

        normalized = self._format_normalized_datetime(candidate, parsed)
        if normalized == value:
            return None

        if column_name not in self._date_warning_emitted:
            self.logger.warning(
                "Non-ISO date detected in stream %s (column %s) with sample '%s'; normalizing to %s",
                self.stream_name,
                column_name,
                value,
                normalized,
            )
            self._date_warning_emitted.add(column_name)

        return normalized

    def _format_normalized_datetime(self, original: str, parsed: datetime) -> str:
        """Format a parsed datetime into date-only or datetime ISO-8601 string."""

        is_date_only = self._looks_like_date_only(original)
        if is_date_only and parsed.time() == time(0, 0, 0) and parsed.microsecond == 0:
            return parsed.date().isoformat()
        return parsed.isoformat()

    @staticmethod
    def _looks_like_date_only(value: str) -> bool:
        """Heuristically determine if the original string was a date without time."""

        lowered = value.lower()
        if "t" in lowered or ":" in lowered:
            return False
        return any(sep in lowered for sep in ("-", "/", ".")) and len(lowered) <= 12

    def _coerce_records_to_varchar(self, records: list[dict[str, Any]]) -> None:
        """Ensure configured VARCHAR columns contain string values."""

        if not self._varchar_columns:
            return

        for record in records:
            for column in self._varchar_columns:
                if column in record and record[column] is not None:
                    record[column] = self._stringify_value(record[column])

    def _stringify_value(self, value: Any) -> str:
        if isinstance(value, datetime):
            return value.isoformat()
        return str(value)

    def _increment_metric(self, metric_name: str, labels: dict[str, Any]) -> None:
        """Record metrics for observability while tolerating missing clients."""

        self._emitted_metrics.append((metric_name, labels))
        metrics_client = getattr(self, "metrics", None)
        if metrics_client is None:
            return

        try:
            counter = metrics_client.counter(metric_name, labels)
            counter.increment(1)
        except AttributeError:
            try:
                metrics_client.increment(metric_name, 1, labels)
            except AttributeError:
                self.logger.debug(
                    "Metrics client does not support incrementing %s", metric_name
                )

    def _handle_batch_failure(
        self,
        pyarrow_table,
        raw_records: list[dict[str, Any]],
        error: Exception,
    ) -> Path:
        """Persist failed batch artifacts for later replay."""

        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
        batch_dir = Path(self.fallback_dir) / self.stream_name / timestamp
        batch_dir.mkdir(parents=True, exist_ok=True)

        parquet_path = batch_dir / FALLBACK_PARQUET_FILENAME
        if pyarrow_table is not None:
            pq.write_table(pyarrow_table, parquet_path.as_posix())

        meta_path = batch_dir / FALLBACK_META_FILENAME
        meta = {
            "stream": self.stream_name,
            "row_count": 0 if pyarrow_table is None else pyarrow_table.num_rows,
            "schema": self.flatten_schema,
            "error": {"type": type(error).__name__, "message": str(error)},
            "traceback": traceback.format_exception_only(type(error), error),
            "settings": {
                "fallback_on_insert_error": self.fallback_on_insert_error,
                "advance_state_on_fallback": self.advance_state_on_fallback,
                "fallback_include_payload": self.fallback_include_payload,
                "sanitize_timezones": self.sanitize_timezones,
                "sanitize_dates": self.sanitize_dates,
                "dates_to_varchar": self.dates_to_varchar_enabled,
            },
        }
        meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

        if self.fallback_include_payload and raw_records:
            records_path = batch_dir / FALLBACK_RECORDS_FILENAME
            with records_path.open("w", encoding="utf-8") as records_file:
                for record in raw_records:
                    records_file.write(
                        json.dumps(record, cls=CustomJSONEncoder, ensure_ascii=False)
                    )
                    records_file.write("\n")

        self.logger.error(
            "Quarantined failed batch for %s at %s due to %s: %s",
            self.stream_name,
            batch_dir,
            type(error).__name__,
            error,
        )
        self._increment_metric(
            "target_ducklake_fallback_batches_total",
            {"stream": self.stream_name},
        )
        return batch_dir

    @property
    def target_schema(self) -> str:
        """Get the target schema."""
        if self.config.get("default_target_schema"):
            # if default target schema is provided, use it
            self.logger.info(
                f"Using provided default target schema {self.config.get('default_target_schema')}"
            )
            return self.config.get("default_target_schema")  # type: ignore
        # if no default target schema is provided, try to derive it from the stream name
        # only works for database extractors (eg public-users becomes public.users)
        stream_name_dict = stream_name_to_dict(self.stream_name)
        if stream_name_dict.get("schema_name"):
            if self.config.get("target_schema_prefix"):
                target_schema = f"{self.config.get('target_schema_prefix')}_{stream_name_dict.get('schema_name')}"
            else:
                target_schema = stream_name_dict.get("schema_name")
            self.logger.info(
                f"Using derived target schema {target_schema} from stream name {self.stream_name}"
            )
            return target_schema
        raise ValueError(
            f"No schema name found for stream {self.stream_name} and no default target schema provided"
        )

    @property
    def target_table(self) -> str:
        """Get the target table."""
        stream_name_dict = stream_name_to_dict(self.stream_name)
        return stream_name_dict.get("table_name")

    @property
    def max_size(self) -> int:
        """Get max batch size.

        Returns:
            Max number of records to batch before `is_full=True`
        """
        return self.config.get("max_batch_size", 10000)

    def setup(self) -> None:
        # create the target schema if it doesn't exist
        self.connector.prepare_target_schema(self.target_schema)

        # prepare the table
        self.connector.prepare_table(
            target_schema_name=self.target_schema,
            table_name=self.target_table,
            columns=self.ducklake_schema,
            partition_fields=self.partition_fields,
            overwrite_table=self.should_overwrite_table,
        )

    def preprocess_record(self, record: dict, context: dict) -> dict:
        """Process incoming record and convert Decimal objects to float for PyArrow compatibility
        when writing to parquet files.
        """
        # Convert any Decimal objects to float so that we can write to parquet files
        for key, value in record.items():
            if isinstance(value, Decimal):
                record[key] = float(value)
                self.logger.debug(
                    f"Converted Decimal field '{key}' from {value} to {record[key]}"
                )

        return record

    def process_record(self, record: dict, context: dict) -> None:
        """Process the record and flatten if necessary."""
        raw_copy = json.loads(json.dumps(record, cls=CustomJSONEncoder))
        context.setdefault("_raw_records", []).append(raw_copy)
        record_flatten = flatten_record(
            record,
            flatten_schema=self.flatten_schema,
            max_level=self.flatten_max_level,
        )
        super().process_record(record_flatten, context)

    def _remove_temp_table_duplicates(self, pyarrow_df):
        """Remove duplicates within PyArrow table based on key properties."""
        if not self.key_properties or pyarrow_df is None:
            return pyarrow_df

        # Check that all key properties exist in the pyarrow table
        available_columns = set(pyarrow_df.column_names)
        for key_property in self.key_properties:
            if key_property not in available_columns:
                raise ValueError(
                    f"Key property {key_property} not found in pyarrow temp file for {self.target_table}. Available columns: {list(available_columns)}"
                )

        original_row_count = len(pyarrow_df)
        # Convert to polars, drop duplicates, then back to pyarrow
        polars_df = pl.from_arrow(pyarrow_df)
        polars_df = polars_df.unique(subset=self.key_properties)
        pyarrow_df = polars_df.to_arrow()

        new_row_count = len(pyarrow_df)
        duplicates_removed = original_row_count - new_row_count
        if duplicates_removed > 0:
            self.logger.info(
                f"Removed {duplicates_removed} duplicate rows based on key properties: {self.key_properties}"
            )

        return pyarrow_df

    def write_temp_file(self, context: dict):
        """Write the current batch to a temporary parquet file."""
        self.logger.info(
            f"Writing batch for {self.stream_name} with {len(context['records'])} records to parquet file."
        )
        pyarrow_df = None
        records = context.get("records", [])
        self._sanitize_records(records)
        self._coerce_records_to_varchar(records)
        pyarrow_df = concat_tables(
            records,
            pyarrow_df,
            self.pyarrow_schema,  # type: ignore
        )

        # Drop duplicates based on key properties if they exist in temp file
        if self.key_properties and pyarrow_df is not None:
            pyarrow_df = self._remove_temp_table_duplicates(pyarrow_df)

        self.logger.info(
            f"Batch pyarrow table has ({len(pyarrow_df)} rows)"  # type: ignore
        )
        context["records"] = []

        # Ensure temp directory exists
        os.makedirs(self.temp_file_dir, exist_ok=True)

        # Create a more descriptive filename with timestamp
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        file_name = f"{self.stream_name}_{timestamp}_{self.files_saved:05d}.parquet"
        full_temp_file_path = os.path.join(self.temp_file_dir, file_name)

        try:
            self.logger.info(f"Writing batch to temp file: {full_temp_file_path}")
            pq.write_table(pyarrow_df, full_temp_file_path)
            self.files_saved += 1
            row_count = len(pyarrow_df) if pyarrow_df is not None else 0
            self.logger.info(f"Successfully wrote {row_count} rows to {file_name}")
            return full_temp_file_path, pyarrow_df
        except Exception as e:
            self.logger.error(
                f"Failed to write parquet file {full_temp_file_path}: {e}"
            )
            raise

    def process_batch(self, context: dict) -> None:
        """Process a batch of records."""

        # first write each batch to a parquet file
        temp_file_path, pyarrow_df = self.write_temp_file(context)
        self.logger.info(f"Temp file path: {temp_file_path}")
        raw_records = context.pop("_raw_records", [])

        # get the columns from the file and the table
        file_columns = [col["name"] for col in self.ducklake_schema]
        table_columns = self.connector.get_table_columns(
            self.target_schema, self.target_table
        )

        # If no key properties, load method is append or overwrite, or table should be overwritten
        # we simply insert the data. Table is already truncated in setup if load method is overwrite
        try:
            if (
                not self.key_properties
                or self.load_method in ["append", "overwrite"]
                or self.should_overwrite_table
            ):
                self.connector.insert_into_table(
                    temp_file_path,
                    self.target_schema,
                    self.target_table,
                    file_columns,
                    table_columns,
                )

            # If load method is merge, we merge the data
            elif self.load_method == "merge":
                self.connector.merge_into_table(
                    temp_file_path,
                    self.target_schema,
                    self.target_table,
                    file_columns,
                    table_columns,
                    self.key_properties,
                )
        except Exception as exc:
            if self.fallback_on_insert_error:
                self._handle_batch_failure(pyarrow_df, raw_records, exc)
                if self.advance_state_on_fallback:
                    return
            raise
        finally:
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)

    def __del__(self) -> None:
        """Cleanup when sink is destroyed."""
        if hasattr(self, "connector"):
            self.connector.close()
        # delete the temp file
        if hasattr(self, "temp_file_dir"):
            self.logger.info(f"Cleaning up temp file directory {self.temp_file_dir}")
            if os.path.exists(self.temp_file_dir):
                shutil.rmtree(self.temp_file_dir)


def stream_name_to_dict(stream_name, separator="-"):
    catalog_name = None
    schema_name = None
    table_name = stream_name

    # Schema and table name can be derived from stream if it's in <schema_nama>-<table_name> format
    s = stream_name.split(separator)
    if len(s) == 2:
        schema_name = s[0]
        table_name = s[1]
    if len(s) > 2:
        catalog_name = s[0]
        schema_name = s[1]
        table_name = "_".join(s[2:])

    return {
        "catalog_name": catalog_name,
        "schema_name": schema_name,
        "table_name": table_name,
    }

"""ducklake target sink class, which handles writing streams."""

from __future__ import annotations

import os
import shutil
from collections.abc import Sequence
from datetime import datetime, timezone
from decimal import Decimal
import uuid

import polars as pl
import pyarrow.parquet as pq
from singer_sdk import Target

# from singer_sdk.connectors import SQLConnector
from singer_sdk.sinks import BatchSink

from target_ducklake.connector import DuckLakeConnector
from target_ducklake.flatten import flatten_record, flatten_schema
from target_ducklake.parquet_utils import (
    concat_tables,
    flatten_schema_to_pyarrow_schema,
)


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

        self.base_temp_file_dir = self.config.get("temp_file_dir", "temp_files/")
        self.temp_file_dir = os.path.join(
            self.base_temp_file_dir, f"{self.stream_name}_{uuid.uuid4()}"
        )
        self.files_saved = 0
        self.convert_tz_to_utc = self.config.get("convert_tz_to_utc", False)

        # NOTE: we probably don't need all this logging, but useful for debugging while target is in development
        # Log original schema for debugging
        self.logger.info(f"Original schema for stream '{stream_name}': {self.schema}")

        # Use the connector for database operations
        self.connector = DuckLakeConnector(dict(self.config))

        # Create pyarrow and Ducklake schemas
        self.flatten_max_level = self.config.get("flatten_max_level", 0)
        self.auto_cast_timestamps = self.config.get("auto_cast_timestamps", False)
        self.flatten_schema = flatten_schema(
            self.schema,
            max_level=self.flatten_max_level,
            auto_cast_timestamps=self.auto_cast_timestamps,
        )
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
            self.logger.info(
                f"No load method provided for {self.stream_name}, using default merge"
            )
            self.load_method = "merge"
        else:
            self.logger.info(
                f"Load method {self.config.get('load_method')} provided for {self.stream_name}"
            )
            self.load_method = str(self.config.get("load_method"))

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
        self.connector.prepare_target_schema(target_schema_name=self.target_schema)

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

    def write_temp_file(self, context: dict) -> str:
        """Write the current batch to a temporary parquet file."""
        self.logger.info(
            f"Writing batch for {self.stream_name} with {len(context['records'])} records to parquet file."
        )
        pyarrow_df = None
        pyarrow_df = concat_tables(
            records=context.get("records", []),
            pyarrow_table=pyarrow_df,
            pyarrow_schema=self.pyarrow_schema,  # type: ignore
            flattened_schema=self.flatten_schema,
            convert_tz_to_utc=self.convert_tz_to_utc,
        )

        # Drop duplicates based on key properties if they exist in temp file
        if self.key_properties and pyarrow_df is not None:
            pyarrow_df = self._remove_temp_table_duplicates(pyarrow_df)

        self.logger.info(
            f"Batch pyarrow table has ({len(pyarrow_df)} rows)"  # type: ignore
        )
        del context["records"]

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
            return full_temp_file_path
        except Exception as e:
            self.logger.error(
                f"Failed to write parquet file {full_temp_file_path}: {e}"
            )
            raise

    def process_batch(self, context: dict) -> None:
        """Process a batch of records."""

        # first write each batch to a parquet file
        temp_file_path = self.write_temp_file(context)
        self.logger.info(f"Temp file path: {temp_file_path}")

        # get the columns from the file and the table
        file_columns = [col["name"] for col in self.ducklake_schema]
        table_columns = self.connector.get_table_columns(
            self.target_schema, self.target_table
        )

        # If no key properties, load method is append or overwrite, or table should be overwritten
        # we simply insert the data. Table is already truncated in setup if load method is overwrite
        if (
            not self.key_properties
            or self.load_method in ["append", "overwrite"]
            or self.should_overwrite_table
        ):
            self.connector.insert_into_table(
                file_location=temp_file_path,
                target_schema_name=self.target_schema,
                table_name=self.target_table,
                file_columns=file_columns,
                target_table_columns=table_columns,
            )

        # If load method is merge, we merge the data
        elif self.load_method == "merge":
            date_type_pks = [
                col["name"] for col in self.ducklake_schema if col["type"] == "DATETIME"
            ]
            self.connector.merge_into_table(
                file_location=temp_file_path,
                target_schema_name=self.target_schema,
                table_name=self.target_table,
                file_columns=file_columns,
                target_table_columns=table_columns,
                key_properties=list(self.key_properties),
                date_type_keys=list(date_type_pks),
            )

        # delete file after insert
        os.remove(temp_file_path)

    def clean_up(self) -> None:
        """Perform per-stream cleanup."""
        super().clean_up()
        # Remove this stream's dedicated temp directory
        if hasattr(self, "temp_file_dir"):
            self.logger.info(
                f"Cleaning up temp directory for stream {self.stream_name}: {self.temp_file_dir}"
            )
            try:
                if os.path.exists(self.temp_file_dir):
                    shutil.rmtree(self.temp_file_dir)
            except Exception as e:
                self.logger.warning(
                    f"Non-fatal error during temp directory cleanup for {self.stream_name}: {e}"
                )


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
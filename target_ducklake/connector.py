"""DuckLake connector for managing database connections."""

from __future__ import annotations

import logging
import math
from typing import Any, Dict, Optional

import duckdb
from singer_sdk.connectors.sql import JSONSchemaToSQL
from sqlalchemy.types import BIGINT, DECIMAL, INTEGER, JSON

logger = logging.getLogger(__name__)

PARTITION_GRANULARITIES = ["year", "month", "day", "hour"]


class DuckLakeConnectorError(Exception):
    """Custom exception for DuckLake connector errors."""


class JSONSchemaToDuckLake(JSONSchemaToSQL):
    """Convert JSON Schema types to DuckLake compatible types."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Override the default type handler for object and array types
        self.register_type_handler("object", JSON)
        self.register_type_handler("array", JSON)
        # Override the default type handler for integer type
        self.register_type_handler("integer", self._handle_integer_type)
        # Override number handling to use high-precision decimals by default
        self.register_type_handler("number", self._handle_number_type)

    def _handle_integer_type(self, jsonschema: dict) -> INTEGER | BIGINT:
        """Handle integer type."""
        minimum = jsonschema.get("minimum", -math.inf)
        maximum = jsonschema.get("maximum", math.inf)
        if minimum >= -(2**31) and maximum < 2**31:
            return INTEGER()
        return BIGINT()

    def _handle_number_type(self, jsonschema: dict) -> DECIMAL:
        """Handle number type with safe default precision.

        Prefer high-precision DECIMAL to avoid overflow when casting floats from
        parquet into DECIMAL columns. DuckDB supports precision up to 38.
        """
        # If schema provides explicit precision/scale, widen precision to 38.
        precision = jsonschema.get("precision")
        scale = jsonschema.get("scale")

        max_precision = 38
        default_scale = 9

        if precision is not None:
            precision = min(precision, max_precision)
            scale = default_scale if scale is None else min(scale, precision)
            return DECIMAL(precision, scale)

        # Otherwise, use a generous default
        return DECIMAL(max_precision, default_scale)


class DuckLakeConnector:
    """Handles DuckLake database connections and setup."""

    def __init__(self, config: Dict[str, Any]) -> None:
        """Initialize the DuckLake connector with configuration.

        Args:
            config: Configuration dictionary containing connection parameters

        Raises:
            DuckLakeConnectorError: If required configuration is missing
        """
        self.config = config
        self._validate_config()

        self.catalog_url = config.get("catalog_url")
        self.data_path = config.get("data_path")
        self.catalog_type = config.get("catalog_type", "postgres")
        self.storage_type = config.get("storage_type")
        self.public_key = config.get("public_key")
        self.secret_key = config.get("secret_key")
        self.region = config.get("region")

        self._connection: duckdb.DuckDBPyConnection | None = None
        self.catalog_name = "ducklake_catalog"
        self.meta_schema = config.get("meta_schema")

    def _validate_config(self) -> None:
        """Validate required configuration parameters."""
        required_params = ["catalog_url", "data_path"]
        missing_params = [
            param for param in required_params if not self.config.get(param)
        ]

        if missing_params:
            raise DuckLakeConnectorError(
                f"Missing required configuration parameters: {', '.join(missing_params)}"
            )

        # Validate cloud storage configuration
        storage_type = self.config.get("storage_type")
        if storage_type in ["GCS", "S3"]:
            has_public_key = bool(self.config.get("public_key"))
            has_secret_key = bool(self.config.get("secret_key"))

            if has_public_key != has_secret_key:
                # One is set but not the other
                missing_key = "secret_key" if has_public_key else "public_key"
                raise DuckLakeConnectorError(
                    f"Storage type is {storage_type} and {('public_key' if has_public_key else 'secret_key')} "
                    f"is provided, but {missing_key} is missing. Both must be provided together."
                )

            if not has_public_key and not has_secret_key:
                logger.info(
                    f"Storage type is {storage_type} but no authentication credentials provided. "
                    f"This will work for public storage or if using instance/environment credentials "
                    f"(e.g., IAM roles, service accounts)."
                )

            # For S3 with explicit credentials, region is required
            if (
                storage_type == "S3"
                and has_public_key
                and not self.config.get("region")
            ):
                raise DuckLakeConnectorError(
                    "Storage type is S3 with explicit credentials, but region is missing. "
                    "Region is required for S3 authentication."
                )

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """Get or create the DuckDB connection."""
        if self._connection is None:
            self._connection = self._create_connection()
            logger.info("DuckDB and DuckLake catalog connection created.")
        return self._connection

    def _create_connection(self) -> duckdb.DuckDBPyConnection:
        """Create and configure a new DuckDB connection."""
        try:
            logger.info("Creating DuckDB connection")
            conn = duckdb.connect(database=":memory:")

            # Execute startup script to configure DuckLake
            startup_script = self._build_startup_script()
            conn.execute(startup_script)

            return conn
        except Exception as e:
            raise DuckLakeConnectorError(
                f"Failed to create DuckDB connection: {e}"
            ) from e

    def _build_startup_script(self) -> str:
        """Build the DuckDB startup script with extensions and attachments."""
        script_parts = [
            "INSTALL ducklake;",
            "INSTALL postgres;",
            "SET ducklake_max_retry_count=100;",
        ]

        if self.catalog_type == "duckdb":
            logger.info(
                f"Detected DuckDB catalog type: {self.catalog_type}. Local DuckDB catalog will be used."
            )
            attach_statement = (
                f"ATTACH 'ducklake:metadata.ducklake' AS {self.catalog_name};"
            )
        else:
            # Build ATTACH parameters
            attach_params = {"DATA_PATH": f"'{self.data_path}'"}
            if self.meta_schema:
                attach_params["META_SCHEMA"] = f"'{self.meta_schema}'"

            params_str = ", ".join(
                f"{key} {value}" for key, value in attach_params.items()
            )
            attach_statement = f"ATTACH 'ducklake:{self.catalog_type}:{self.catalog_url}' AS {self.catalog_name} ({params_str});"
        script_parts.append(attach_statement)

        # Add secrets for cloud storage if configured
        if self.public_key and self.secret_key:
            if self.storage_type == "GCS":
                script_parts.append(
                    f"CREATE SECRET ("
                    f"TYPE gcs, "
                    f"KEY_ID '{self.public_key}', "
                    f"SECRET '{self.secret_key}'"
                    ");"
                )
            elif self.storage_type == "S3":
                # For S3, region is required when using explicit credentials
                secret_parts = [
                    f"TYPE s3, KEY_ID '{self.public_key}'",
                    f"SECRET '{self.secret_key}'",
                    f"REGION '{self.region}'",  # Always include since we validate it's required
                ]

                script_parts.append("CREATE SECRET (" + ", ".join(secret_parts) + ");")
        elif self.storage_type in ["GCS", "S3"]:
            # Log info when cloud storage is configured but no auth is provided
            logger.info(
                f"Storage type is {self.storage_type} but no authentication credentials provided. "
                f"Will attempt to access storage without explicit authentication. "
                f"This will only work for public buckets or if using instance/environment credentials."
            )

        return "\n".join(script_parts)

    def execute(self, query: str, parameters: Any = None) -> Any:
        """Execute a query on the DuckDB connection.

        Args:
            query: SQL query to execute
            parameters: Optional parameters for the query

        Returns:
            Query result

        Raises:
            DuckLakeConnectorError: If query execution fails
        """
        try:
            logger.debug(f"Executing query: {query}")
            return self.connection.execute(query, parameters)
        except Exception as e:
            raise DuckLakeConnectorError(f"Query {query} failed with error: {e}") from e

    def prepare_target_schema(self, target_schema_name: str) -> None:
        """Prepare the schema for the target table."""
        create_schema_query = (
            f"CREATE SCHEMA IF NOT EXISTS {self.catalog_name}.{target_schema_name};"
        )
        logger.info(
            f"Preparing schema {target_schema_name} in catalog {self.catalog_name} with query {create_schema_query}"
        )
        self.execute(create_schema_query)

    def _check_if_table_exists(self, target_schema_name: str, table_name: str) -> bool:
        """Check if the table exists in the target schema."""
        check_table_query = f"""
        SELECT
            COUNT(*) AS cnt
        FROM information_schema.tables
            WHERE table_schema = '{target_schema_name}'
            AND table_name   = '{table_name}';
        """
        result = self.execute(check_table_query)
        return result.fetchone()[0] > 0

    def get_table_columns(self, target_schema_name: str, table_name: str) -> list[str]:
        """Get the columns of the table."""
        get_columns_query = f"PRAGMA TABLE_INFO('{self.catalog_name}.{target_schema_name}.{table_name}');"
        result = self.execute(get_columns_query).fetchall()
        return [col[1] for col in result]

    def _add_columns(
        self,
        target_schema_name: str,
        table_name: str,
        columns: list[dict[str, str]],
    ) -> None:
        """Add columns to the table one by one."""
        for col in columns:
            add_column_query = f'ALTER TABLE {self.catalog_name}.{target_schema_name}.{table_name} ADD COLUMN "{col["name"]}" {col["type"]};'
            logger.info(
                f"Adding column {col['name']} ({col['type']}) to table {table_name}"
            )
            self.execute(add_column_query)

    def _create_empty_table(
        self,
        target_schema_name: str,
        table_name: str,
        columns: list[dict[str, str]],
    ) -> None:
        """Create an empty table in the target schema."""
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.catalog_name}.{target_schema_name}.{table_name} (
            {", ".join([f'"{col["name"]}" {col["type"]}' for col in columns])}
        );
        """
        logger.info(
            f"Creating table {table_name} in schema {target_schema_name} with query {create_table_query}"
        )
        self.execute(create_table_query)

    def prepare_table(
        self,
        target_schema_name: str,
        table_name: str,
        columns: list[dict[str, str]],
        partition_fields: list[dict[str, str]] | None = None,
        overwrite_table: bool = False,
    ) -> list[str]:
        """Prepare the table for the target schema.
        If the table doesn't exist, create it.
        Add missing columns to existing table if necessary.
        Returns all columns in the table and order in which they appear.
        """
        if not self._check_if_table_exists(target_schema_name, table_name):
            self._create_empty_table(target_schema_name, table_name, columns)
        else:
            logger.info(
                f"Table {table_name} already exists in schema {target_schema_name}. Checking if columns match..."
            )
            # get the columns of the existing table
            existing_columns = self.get_table_columns(target_schema_name, table_name)
            new_columns = [
                col for col in columns if col["name"] not in existing_columns
            ]
            if new_columns:
                logger.info(f"Adding new columns to table {table_name}: {new_columns}")
                self._add_columns(target_schema_name, table_name, new_columns)

        # truncate the table if necessary
        if overwrite_table:
            logger.info(f"Truncating table {table_name} in schema {target_schema_name}")
            self.execute(
                f"TRUNCATE TABLE {self.catalog_name}.{target_schema_name}.{table_name};"
            )

        # Add partition fields if any
        if partition_fields:
            logger.info(
                f"Adding partition fields to table {table_name}: {partition_fields}"
            )
            self._add_partition_fields(target_schema_name, table_name, partition_fields)

        return self.get_table_columns(target_schema_name, table_name)

    def _add_partition_fields(
        self,
        target_schema_name: str,
        table_name: str,
        partition_fields: list[dict[str, str]],
    ) -> None:
        """Add partition fields to the table."""
        partitions_sql = ""
        for partition_field in partition_fields:
            column_type = partition_field["type"]
            if column_type == "identifier":
                partitions_sql += f"{partition_field['column_name']}, "
            elif column_type == "timestamp":
                for granularity in partition_field["granularity"]:
                    if granularity not in PARTITION_GRANULARITIES:
                        raise DuckLakeConnectorError(
                            f"Unsupported granularity: {granularity}"
                        )
                    partitions_sql += (
                        f"{granularity}({partition_field['column_name']}), "
                    )
            else:
                raise DuckLakeConnectorError(
                    f"Unsupported partition type: {column_type}"
                )
        partitions_sql = partitions_sql[:-2]
        add_partition_query = f"ALTER TABLE {self.catalog_name}.{target_schema_name}.{table_name} SET PARTITIONED BY ({partitions_sql});"
        logger.info(
            f"Adding partition fields to table {table_name} with query {add_partition_query}"
        )
        self.execute(add_partition_query)

    def _build_columns_sql(
        self,
        file_columns: list[str],
        target_table_columns: list[str],
    ) -> str:
        """Build SQL column list for SELECT statement used for INSERT.
        Orders columns by which they appear in the target table.
        If a column is not found in the file, it is replaced with NULL.
        """
        columns_sql = ""
        for col in target_table_columns:
            if col not in file_columns:
                logger.warning(f"Column {col} not found in source, using NULL")
                columns_sql += "NULL, "
            else:
                columns_sql += f' "{col}", '
        # Remove trailing comma and space
        return columns_sql[:-2]

    def insert_into_table(
        self,
        file_location: str,
        target_schema_name: str,
        table_name: str,
        file_columns: list[str],
        target_table_columns: list[str],
    ):
        """Insert data from a parquet file into the table.
        Orders columns by which they appear in the target table.
        If a column is not found in the file, it is inserted as NULL.
        """
        columns_sql = self._build_columns_sql(file_columns, target_table_columns)
        insert_sql = f"INSERT INTO {self.catalog_name}.{target_schema_name}.{table_name} SELECT {columns_sql} FROM '{file_location}';"
        logger.info(f"Inserting into table {table_name} with SQL {insert_sql}")
        self.execute(insert_sql)

    def merge_into_table(
        self,
        file_location: str,
        target_schema_name: str,
        table_name: str,
        file_columns: list[str],
        target_table_columns: list[str],
        key_properties: list[str],
        date_type_keys: list[Optional[str]] = [],
    ):
        """Merge data from a parquet file into the table using MERGE INTO statement.

        Uses key_properties to match rows between source and target.
        Matched rows are updated, unmatched rows are inserted.
        """
        table_ref = f"{self.catalog_name}.{target_schema_name}.{table_name}"

        # Build ON condition for key properties
        on_conditions = []
        for key in key_properties:
            if key in date_type_keys:
                logger.info(
                    f"DATETIME primary key detected (column: {key}), casting to VARCHAR for merge comparison"
                )
                on_conditions.append(f'target."{key}"::VARCHAR = source."{key}"::VARCHAR')
            else:
                on_conditions.append(f'target."{key}" = source."{key}"')
        on_clause = " AND ".join(on_conditions)

        # Build UPDATE SET clause for all columns
        update_assignments = []
        for col in target_table_columns:
            if col in file_columns:
                update_assignments.append(f'"{col}" = source."{col}"')
            else:
                update_assignments.append(f'"{col}" = NULL')
        update_clause = ", ".join(update_assignments)

        # Build INSERT clause
        insert_columns = ", ".join([f'"{col}"' for col in target_table_columns])
        insert_values = []
        for col in target_table_columns:
            if col in file_columns:
                insert_values.append(f'source."{col}"')
            else:
                insert_values.append("NULL")
        values_clause = ", ".join(insert_values)

        merge_sql = f"""
        MERGE INTO {table_ref} AS target
        USING '{file_location}' AS source
        ON ({on_clause})
        WHEN MATCHED THEN UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({values_clause});
        """

        logger.info(f"Executing MERGE operation on table {table_name}")
        logger.info(f"Merge SQL: {merge_sql}")

        self.execute(merge_sql)

    def json_to_ducklake_schema(self, schema: dict) -> list[dict[str, str]]:
        """Convert a JSON schema to an array of dictionaries with column name and type.
        Example Output: [{'name': 'id', 'type': 'INTEGER'}, {'name': 'name', 'type': 'STRING'}]
        """
        ducklake_schema = []
        converter = JSONSchemaToDuckLake()
        for prop_name, prop_schema in schema.items():
            ducklake_schema.append(
                {"name": prop_name, "type": str(converter.to_sql_type(prop_schema))}
            )
        return ducklake_schema

    def close(self) -> None:
        """Close the database connection."""
        if self._connection is not None:
            logger.info("Closing DuckDB connection")
            self._connection.close()
            self._connection = None

    def __enter__(self) -> DuckLakeConnector:
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()

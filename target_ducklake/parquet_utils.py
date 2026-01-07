# taken from https://github.com/Automattic/target-parquet/blob/master/target_parquet/utils/parquet.py
import logging
from datetime import datetime, timezone

import pyarrow as pa

FIELD_TYPE_TO_PYARROW = {
    "BOOLEAN": pa.bool_(),
    "STRING": pa.string(),
    "ARRAY": pa.string(),
    "INTEGER": pa.int64(),
    "NUMBER": pa.float64(),
    "OBJECT": pa.string(),
}

logger = logging.getLogger(__name__)

EXTENSION_MAPPING = {
    "snappy": ".snappy",
    "gzip": ".gz",
    "brotli": ".br",
    "zstd": ".zstd",
    "lz4": ".lz4",
}

logger = logging.getLogger(__name__)


def _field_type_to_pyarrow_field(
    field_name: str, input_types: dict, required_fields: list[str]
) -> pa.Field:
    types = input_types.get("type", [])
    # If type is not defined, check if anyOf is defined
    if not types:
        for any_type in input_types.get("anyOf", []):
            if t := any_type.get("type"):
                if isinstance(t, list):
                    types.extend(t)
                else:
                    types.append(t)
    types = [types] if isinstance(types, str) else types
    types_uppercase = [item.upper() for item in types]
    nullable = "NULL" in types_uppercase or field_name not in required_fields
    if "NULL" in types_uppercase:
        types_uppercase.remove("NULL")
    input_type = next(iter(types_uppercase)) if types_uppercase else ""
    pyarrow_type = FIELD_TYPE_TO_PYARROW.get(input_type, pa.string())
    return pa.field(field_name, pyarrow_type, nullable)


def flatten_schema_to_pyarrow_schema(flatten_schema_dictionary: dict) -> pa.Schema:
    """Function that converts a flatten schema to a pyarrow schema in a defined order.

    E.g:
     dictionary = {
         'key_1': {'type': ['null', 'integer']},
         'key_2__key_3': {'type': ['null', 'string']},
         'key_2__key_4__key_5': {'type': ['null', 'integer']},
         'key_2__key_4__key_6': {'type': ['null', 'array']}
     }
    By calling the function with the dictionary above as parameter,
    you will get the following structure:
        pa.schema([
             pa.field('key_1', pa.int64()),
             pa.field('key_2__key_3', pa.string()),
             pa.field('key_2__key_4__key_5', pa.int64()),
             pa.field('key_2__key_4__key_6', pa.string())
        ])
    """
    return pa.schema(
        [
            _field_type_to_pyarrow_field(
                field_name, field_input_types, required_fields=[]
            )
            for field_name, field_input_types in flatten_schema_dictionary.items()
        ]
    )


def _normalize_datetime(value):
    """Normalize datetime objects to UTC timezone for PyArrow compatibility.

    PyArrow doesn't recognize custom timezone strings like 'UTC-07:00'.
    This function converts datetime objects with any timezone to UTC.
    """
    if isinstance(value, datetime) and value.tzinfo is not None:
        # Convert to UTC timezone
        return value.astimezone(timezone.utc)
    return value


def _normalize_data_for_pyarrow(data: dict, datetime_fields: list[str]) -> dict:
    """Normalize data values to be compatible with PyArrow.

    This handles datetime objects with non-standard timezone names.
    Modifies the data dict in-place for efficiency.
    """
    if not datetime_fields:
        return data

    for key in datetime_fields:
        if key in data:
            data[key] = [_normalize_datetime(v) for v in data[key]]
    return data


def create_pyarrow_table(
    list_dict: list[dict], pyarrow_schema: pa.Schema, datetime_fields: list = []
) -> pa.Table:
    """Create a pyarrow Table from a python list of dict."""
    data = {f: [row.get(f) for row in list_dict] for f in pyarrow_schema.names}
    # Normalize datetime values to UTC timezone for PyArrow compatibility
    normalized_data = _normalize_data_for_pyarrow(data, datetime_fields)
    return pa.table(normalized_data).cast(pyarrow_schema)


def concat_tables(
    records: list[dict],
    pyarrow_table: pa.Table,
    pyarrow_schema: pa.Schema,
    flattened_schema: dict,
    convert_tz_to_utc: bool = False,
) -> pa.Table:
    """Create a dataframe from records and concatenate with the existing one."""
    if not records:
        return pyarrow_table
    if convert_tz_to_utc:
        datetime_fields = [
            field
            for field in flattened_schema.keys()
            if "format" in flattened_schema[field]
            and flattened_schema[field]["format"] == "date-time"
        ]
    else:
        datetime_fields = []
    new_table = create_pyarrow_table(records, pyarrow_schema, datetime_fields)
    return pa.concat_tables([pyarrow_table, new_table]) if pyarrow_table else new_table

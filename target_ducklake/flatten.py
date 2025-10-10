"""Adapted from target-duckdb and Meltano SDK
https://github.com/jwills/target-duckdb/blob/36c8ce68a0b2584c4bbb07325482968b1edc0c40/target_duckdb/db_sync.py#L74
https://github.com/meltano/sdk/blob/35b499d6163f91752fd674f2a3e46a7e22a1fb47/singer_sdk/helpers/_flattening.py#L342
"""

import collections
import itertools
import json
from datetime import datetime
from decimal import Decimal

TIMESTAMP_COLUMN_NAMES = {
    "timestamp",
    "created_at",
    "updated_at",
    "deleted_at",
    "modified_at",
    "last_modified",
    "updatedat",
    "createdat",
}


class CustomJSONEncoder(json.JSONEncoder):
    # sometimes Python Decimal objects are returned, we need to convert them to floats when flattening the schema
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)  # Convert Decimal to float
        elif isinstance(obj, datetime):
            return obj.isoformat()  # Convert datetime to ISO format string
        return super().default(obj)


def flatten_key(k, parent_key, sep):
    return sep.join(parent_key + [k])


def _should_auto_cast_to_timestamp(
    column_name: str, schema_entry: dict, auto_cast_timestamps: bool
) -> bool:
    """Check if a column should be auto-cast to timestamp format."""
    if not auto_cast_timestamps:
        return False

    # Skip if format is already specified
    if schema_entry.get("format") is not None:
        return False

    # Check if column name matches timestamp patterns
    if column_name.lower() not in TIMESTAMP_COLUMN_NAMES:
        return False

    # Check if it's a string type (either single string or list containing string)
    entry_type = schema_entry.get("type", [])
    if isinstance(entry_type, str):
        return entry_type == "string"
    if isinstance(entry_type, list):
        return "string" in entry_type

    return False


def _apply_timestamp_format(schema_entry: dict) -> dict:
    """Apply timestamp format to a schema entry."""
    result = dict(schema_entry)  # Create a copy
    result["format"] = "date-time"
    return result


# pylint: disable-msg=too-many-arguments
def flatten_record(
    d, flatten_schema=None, parent_key=[], sep="__", level=0, max_level=0
):
    items = []
    for k, v in d.items():
        new_key = flatten_key(k, parent_key, sep)
        if isinstance(v, collections.abc.MutableMapping) and level < max_level:
            items.extend(
                flatten_record(
                    v,
                    flatten_schema,
                    parent_key + [k],
                    sep=sep,
                    level=level + 1,
                    max_level=max_level,
                ).items()
            )
        else:
            items.append(
                (
                    new_key,
                    json.dumps(
                        v, cls=CustomJSONEncoder
                    )  # use custom encoder to convert Decimal type to float
                    if _should_json_dump_value(k, v, flatten_schema)
                    else v,
                )
            )
    return dict(items)


# pylint: disable=dangerous-default-value,invalid-name
def flatten_schema(
    d, parent_key=[], sep="__", level=0, max_level=0, auto_cast_timestamps=False
):
    """
    This function is a bit of a blackbox. Adapted from DuckDB target and Meltano SDK.
    See note mentioned here in official Meltano SDK repo:
    https://github.com/meltano/sdk/blob/35b499d6163f91752fd674f2a3e46a7e22a1fb47/singer_sdk/helpers/_flattening.py#L342
    """
    items = []

    if "properties" not in d:
        return {}

    for k, v in d["properties"].items():
        new_key = flatten_key(k, parent_key, sep)
        # some schemas from airbyte sources have description field which is not needed and causes issues
        if "description" in v.keys():
            del v["description"]

        if "type" in v.keys():
            if "object" in v["type"] and "properties" in v and level < max_level:
                items.extend(
                    flatten_schema(
                        v,
                        parent_key + [k],
                        sep=sep,
                        level=level + 1,
                        max_level=max_level,
                        auto_cast_timestamps=auto_cast_timestamps,
                    ).items()
                )
            # certain taps (e.g. postgres) sometimes return a list of types for JSON columns
            # we need to handle this specific case, if we don't the schema returns VARCHAR
            # may need to revisit this for other types
            elif "array" in v["type"]:
                items.append((new_key, {"type": ["null", "array"]}))
            elif "object" in v["type"]:
                items.append((new_key, {"type": ["null", "object"]}))
            # Apply timestamp auto-casting if applicable
            elif _should_auto_cast_to_timestamp(new_key, v, auto_cast_timestamps):
                items.append((new_key, _apply_timestamp_format(v)))
            else:
                items.append((new_key, v))
        elif "anyOf" in v.keys():
            llv = v["anyOf"]
            i, entry = 0, llv[0]
            while entry["type"] == "null":
                i += 1
                entry = llv[i]
            # modified to handle type value in schema is string or list of strings
            if isinstance(entry["type"], str):
                entry["type"] = list(set(["null", entry["type"]]))
            elif isinstance(entry["type"], list):
                entry["type"] = list(set(["null"] + entry["type"]))  # type: ignore
            items.append((new_key, entry))
        elif len(v.values()) > 0:
            if list(v.values())[0][0]["type"] == "string":
                list(v.values())[0][0]["type"] = ["null", "string"]
                items.append((new_key, list(v.values())[0][0]))
            elif list(v.values())[0][0]["type"] == "array":
                list(v.values())[0][0]["type"] = ["null", "array"]
                items.append((new_key, list(v.values())[0][0]))
            elif list(v.values())[0][0]["type"] == "object":
                list(v.values())[0][0]["type"] = ["null", "object"]
                items.append((new_key, list(v.values())[0][0]))

    key_func = lambda item: item[0]
    sorted_items = sorted(items, key=key_func)
    for k, g in itertools.groupby(sorted_items, key=key_func):
        if len(list(g)) > 1:
            raise ValueError(f"Duplicate column name produced in schema: {k}")

    return dict(sorted_items)


# pylint: disable=redefined-outer-name
def _should_json_dump_value(key, value, flatten_schema=None):
    if isinstance(value, (dict, list)):
        return True

    if (
        flatten_schema
        and key in flatten_schema
        and "type" in flatten_schema[key]
        and set(flatten_schema[key]["type"]) == {"null", "object", "array"}
    ):
        return True

    return False

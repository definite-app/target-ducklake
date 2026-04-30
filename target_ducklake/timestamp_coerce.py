"""Coerce epoch-numeric values into ISO datetime strings for date-time fields.

Some taps (notably tap-mixpanel `export`) declare a column as
``{"type": ["null", "string"], "format": "date-time"}`` but emit the value
as an integer Unix epoch in milliseconds. DuckLake then fails on INSERT
because it tries to cast the bigint as a seconds-epoch and overflows the
TIMESTAMP range.

This helper detects numeric epoch values (int, float, or all-digit string),
distinguishes seconds vs milliseconds vs microseconds by magnitude, and
returns an ISO 8601 UTC string that DuckLake parses cleanly. ISO strings
and ``datetime`` instances pass through unchanged.
"""

from __future__ import annotations

from datetime import datetime, timezone

# Magnitude thresholds for the unit guess. 1e11 seconds is year 5138, so any
# absolute epoch >= 1e11 is unambiguously sub-second; 1e14 is the microsecond
# boundary. Values below 1e11 are treated as seconds, which covers the full
# realistic past + ~1000 years into the future.
_MS_THRESHOLD = 1e11
_US_THRESHOLD = 1e14


def _is_numeric_string(value: str) -> bool:
    s = value.strip()
    if not s:
        return False
    if s[0] in "+-":
        s = s[1:]
    if not s:
        return False
    # Accept a single decimal point.
    return s.replace(".", "", 1).isdigit()


def coerce_timestamp_value(value):
    """Return an ISO datetime string if value looks like a numeric epoch.

    Pass-through cases (returned unchanged):
        - ``None``
        - ``datetime`` instances
        - ``bool`` (Python True/False are ints; never treat as timestamps)
        - non-numeric strings (assumed to be ISO 8601 already)
        - any other type
    """
    if value is None or isinstance(value, datetime):
        return value
    if isinstance(value, bool):
        return value

    if isinstance(value, (int, float)):
        epoch = float(value)
    elif isinstance(value, str) and _is_numeric_string(value):
        try:
            epoch = float(value)
        except ValueError:
            return value
    else:
        return value

    abs_epoch = abs(epoch)
    if abs_epoch >= _US_THRESHOLD:
        epoch = epoch / 1_000_000.0
    elif abs_epoch >= _MS_THRESHOLD:
        epoch = epoch / 1_000.0

    try:
        return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()
    except (OverflowError, OSError, ValueError):
        return value

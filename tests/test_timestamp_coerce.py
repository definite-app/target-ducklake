"""Tests for the epoch-to-ISO timestamp coercion helper.

Covers the regression triggered by tap-mixpanel's `export` stream emitting
event time as an integer Unix epoch in milliseconds. Without coercion,
DuckLake fails INSERT with:

    Conversion Error: timestamp field value out of range:
        "1749637592477" when casting from source column timestamp
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from target_ducklake.timestamp_coerce import coerce_timestamp_value


class TestCoerceTimestampValue:
    def test_passthrough_none(self):
        assert coerce_timestamp_value(None) is None

    def test_passthrough_datetime(self):
        dt = datetime(2025, 6, 11, 12, 26, 32, tzinfo=timezone.utc)
        assert coerce_timestamp_value(dt) is dt

    def test_passthrough_iso_string(self):
        assert (
            coerce_timestamp_value("2025-06-11T10:26:32Z") == "2025-06-11T10:26:32Z"
        )

    def test_passthrough_arbitrary_string(self):
        assert coerce_timestamp_value("not a number") == "not a number"

    def test_passthrough_bool(self):
        # Python bool is an int subclass; never coerce.
        assert coerce_timestamp_value(True) is True
        assert coerce_timestamp_value(False) is False

    def test_int_milliseconds(self):
        # The actual Saturn / Mixpanel value that broke the sync.
        result = coerce_timestamp_value(1749637592477)
        assert result == "2025-06-11T10:26:32.477000+00:00"

    def test_int_seconds(self):
        # 10-digit seconds-since-epoch.
        result = coerce_timestamp_value(1749637592)
        assert result == "2025-06-11T10:26:32+00:00"

    def test_int_microseconds(self):
        # 16-digit microsecond epoch.
        result = coerce_timestamp_value(1749637592477000)
        assert result == "2025-06-11T10:26:32.477000+00:00"

    def test_float_milliseconds(self):
        result = coerce_timestamp_value(1749637592477.0)
        assert result == "2025-06-11T10:26:32.477000+00:00"

    def test_numeric_string_milliseconds(self):
        result = coerce_timestamp_value("1749637592477")
        assert result == "2025-06-11T10:26:32.477000+00:00"

    def test_numeric_string_with_decimal(self):
        result = coerce_timestamp_value("1749637592.5")
        assert result == "2025-06-11T10:26:32.500000+00:00"

    def test_negative_epoch(self):
        # Pre-1970 timestamp; should still coerce.
        result = coerce_timestamp_value(-31536000)
        assert result == "1969-01-01T00:00:00+00:00"

    def test_unparseable_overflow_returns_value_unchanged(self):
        # Absurdly large value (above us threshold) divided by 1e6 still
        # blows the platform datetime range on some systems; helper must
        # not raise.
        huge = 10**24
        result = coerce_timestamp_value(huge)
        # Either coerced or returned unchanged; must not raise.
        assert result == huge or isinstance(result, str)

    @pytest.mark.parametrize("value", [{}, [], object()])
    def test_passthrough_unsupported_types(self, value):
        assert coerce_timestamp_value(value) is value

"""Tests for the shared date-window convention."""

from datetime import datetime, timezone

import pytest

from src.util.date_windows import window_bounds


class TestWindowBounds:
    """One definition of what [start_date, end_date] covers."""

    def test_end_date_covers_its_whole_day(self) -> None:
        """Regression: a bare end date used to stop at midnight.

        Sentinel-2 and Landsat overpasses are mid-morning to midday, so
        stopping at midnight silently discarded the final day of every window.
        """
        start, end = window_bounds("2023-06-01", "2023-06-15")

        assert start == datetime(2023, 6, 1, tzinfo=timezone.utc)
        assert end == datetime(2023, 6, 16, tzinfo=timezone.utc)

        midday_on_the_last_day = datetime(2023, 6, 15, 12, tzinfo=timezone.utc)
        assert start <= midday_on_the_last_day < end

    def test_naive_input_is_read_as_utc(self) -> None:
        start, _ = window_bounds("2023-06-01T06:30:00", "2023-06-15")

        assert start == datetime(2023, 6, 1, 6, 30, tzinfo=timezone.utc)

    def test_offset_is_converted_not_discarded(self) -> None:
        """An explicit offset has to be honoured.

        Regression: this used to be `.replace(tzinfo=utc)`, which relabelled
        the instant instead of converting it -- a window starting at
        midnight Pacific silently started at midnight UTC, seven hours early.
        """
        start, _ = window_bounds("2023-06-15T00:00:00-07:00", "2023-06-20")

        assert start == datetime(2023, 6, 15, 7, tzinfo=timezone.utc)

    def test_single_day_window_is_a_full_day(self) -> None:
        start, end = window_bounds("2023-06-01", "2023-06-01")

        assert (end - start).total_seconds() == 24 * 60 * 60

    def test_backwards_window_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="not after"):
            window_bounds("2023-06-15", "2023-06-01")

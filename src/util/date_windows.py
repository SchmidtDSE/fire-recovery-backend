"""
The one definition of what a ``[start_date, end_date]`` window means.

Both the STAC provider-coverage check and the time slicing of stacked scenes
have to agree on this, or a provider gets accepted for a window it cannot
actually fill and the failure surfaces much later, deep inside numpy. They
used to encode the rule separately -- one in datetime, one in numpy -- kept in
step by a comment. This module is that agreement.
"""

from datetime import datetime, timedelta, timezone
from typing import Tuple


def window_bounds(start_date: str, end_date: str) -> Tuple[datetime, datetime]:
    """
    The half-open UTC interval ``[start, end)`` a date window covers.

    ``end_date`` is inclusive of its whole day. A bare date parses to midnight,
    so treating it as the stop instant drops every scene captured later that
    same day -- and Sentinel-2 and Landsat overpasses are mid-morning to
    midday, which silently discarded the whole final day of every window.

    Args:
        start_date: ISO date or datetime. Naive values are read as UTC.
        end_date: ISO date or datetime, inclusive of the whole day it names.

    Returns:
        Start instant and exclusive end instant, both timezone-aware UTC.
    """
    start = _parse_utc(start_date)
    end = _parse_utc(end_date) + timedelta(days=1)

    if end <= start:
        raise ValueError(
            f"Window end {end_date} is not after window start {start_date}"
        )

    return start, end


def _parse_utc(value: str) -> datetime:
    """Parse an ISO date or datetime to UTC, reading naive values as UTC."""
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)

# -*- coding: utf-8 -*-
"""Test epoch time."""

from datetime import datetime, timezone
from typing import Any, Dict

from dsdk.service import get_utc_now_times


def test_utc_now_times():
    """Test utc now time."""
    kwargs: Dict[str, Any] = {}
    _, _, utc_now, epoch_ms = get_utc_now_times(kwargs, "epoch_ms")

    actual = datetime.utcfromtimestamp(epoch_ms / 1000).replace(
        tzinfo=timezone.utc
    )
    assert utc_now == actual

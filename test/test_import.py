# -*- coding: utf-8 -*-
"""Test import."""


def test_import():
    """Test import."""
    import dsdk  # pylint: disable=import-outside-toplevel

    assert dsdk.Asset is not None
    assert dsdk.Interval is not None
    assert dsdk.Service is not None

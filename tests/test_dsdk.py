# -*- coding: utf-8 -*-
"""Test dsdk."""

from unittest.mock import Mock

import configargparse

from dsdk import BaseBatchJob
from dsdk import Block


def test_batch(monkeypatch):
    """Test batch."""

    class _TestBlock(Block):  # pylint: disable=too-few-public-methods
        name = "test"

        def run(self):
            return 42

    monkeypatch.setattr(configargparse, "ArgParser", Mock)

    batch = BaseBatchJob([_TestBlock()])
    batch.run()
    assert len(batch.evidence) == 1
    assert batch.evidence["test"] == 42

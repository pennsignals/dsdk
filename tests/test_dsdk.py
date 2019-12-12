import configargparse
from unittest.mock import Mock

from dsdk import BaseBatchJob
from dsdk import Block


def test_batch(monkeypatch):
    class TestBlock(Block):
        name = "test"

        def run(self):
            return 42

    monkeypatch.setattr(configargparse, "ArgParser", Mock)

    batch = BaseBatchJob(pipeline=[TestBlock()])
    batch.run()
    assert len(batch.evidence) == 1
    assert batch.evidence["test"] == 42

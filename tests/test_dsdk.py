# -*- coding: utf-8 -*-
"""Test dsdk."""

from unittest.mock import Mock

import configargparse

from dsdk import BaseBatchJob, Block
from dsdk.utils import retry


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


def test_retry_other_exception():
    """Test retry other exception."""

    exceptions_in = [
        RuntimeError("what?"),
        NotImplementedError("how?"),
        RuntimeError("no!"),
    ]
    actual = []
    expected = [1.0, 1.5, 2.25]

    def sleep(wait: float):
        actual.append(wait)

    @retry(
        (NotImplementedError, RuntimeError),
        retries=4,
        delay=1.0,
        backoff=1.5,
        sleep=sleep,
    )
    def explode():
        raise exceptions_in.pop()

    try:
        explode()
        raise AssertionError("IndexError expected")
    except IndexError:
        assert actual == expected


def test_retry_exhausted():
    """Test retry."""

    exceptions_in = [
        RuntimeError("what?"),
        NotImplementedError("how?"),
        RuntimeError("no!"),
        NotImplementedError("when?"),
    ]
    actual = []
    expected = [1.0, 1.5]

    def sleep(wait: float):
        actual.append(wait)

    @retry(
        (NotImplementedError, RuntimeError),
        retries=2,
        delay=1.0,
        backoff=1.5,
        sleep=sleep,
    )
    def explode():
        raise exceptions_in.pop()

    try:
        explode()
        raise AssertionError("NotImplementedError expected")
    except NotImplementedError as exception:
        assert actual == expected
        assert str(exception) == "when?"

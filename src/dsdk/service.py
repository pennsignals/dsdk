# -*- coding: utf-8 -*-
"""Service."""

from __future__ import annotations

from collections import OrderedDict
from contextlib import contextmanager
from datetime import datetime, timezone
from logging import (
    INFO,
    Logger,
    LoggerAdapter,
    NullHandler,
    basicConfig,
    getLogger,
)
from sys import argv as sys_argv
from typing import Any, Dict, Generator, Optional, Sequence, Tuple, cast

from configargparse import ArgParser as ArgumentParser
from configargparse import Namespace

# TODO Add import calling function from parent application
EXTRA = {"callingfunc": "callingfunc"}
logger = getLogger(__name__)
FORMAT = '%(asctime)-15s - %(name)s - %(levelname)s - {"callingfunc": \
    %(callingfunc)s, "module": %(module)s, "function": %(funcName)s, \
        %(message)s}'
basicConfig(format=FORMAT)
logger.setLevel(INFO)
# Add extra kwargs to message format
logger.addHandler(NullHandler())
logger = cast(Logger, LoggerAdapter(logger, EXTRA))


class Interval:  # pylint: disable=too-few-public-methods
    """Interval."""

    def __init__(self, on: datetime, end: Optional[datetime] = None):
        """__init__."""
        self.on = on
        self.end = end

    def as_doc(self) -> Dict[str, Any]:
        """As doc."""
        return {"end": self.end, "on": self.on}


class Model:  # pylint: disable=too-few-public-methods
    """Model."""

    def __init__(self, name: str, version: str) -> None:
        """__init__."""
        self.name = name
        self.version = version

    def as_doc(self) -> Dict[str, Any]:
        """As doc."""
        return {"name": self.name, "on": self.version}


class Batch:  # pylint: disable=too-few-public-methods
    """Batch."""

    def __init__(self, key: Any, record: Interval) -> None:
        """__init__."""
        self.key = key
        self.record = record
        self.evidence = Evidence()

    @property
    def start_time(self):
        """Return start time."""
        return self.record.on

    @property
    def start_date(self):
        """Return start date."""
        on = self.record.on
        return datetime(on.year, on.month, on.day, tzinfo=timezone.utc)

    @property
    def end_date(self):
        """Return end date."""
        end = self.record.end
        if not end:
            return end
        return datetime(end.year, end.month, end.day, tzinfo=timezone.utc)

    def as_insert_doc(self, model: Optional[Model]) -> Dict[str, Any]:
        """As insert doc."""
        doc: Optional[Dict[str, Any]] = None
        if model is not None:
            doc = model.as_doc()
        return {"model": doc, "record": self.record.as_doc()}

    def as_update_doc(self) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """As update doc."""
        return ({"_id": self.key}, {"$set": {"record": self.record.as_doc()}})


class Evidence(OrderedDict):
    """Evidence."""

    def __setitem__(self, key, value):
        """__setitem__."""
        if key in self:
            raise KeyError("{} has already been set".format(key))
        super().__setitem__(key, value)


class Service:
    """Service."""

    def __init__(
        self,
        argv: Optional[Sequence[str]] = None,
        parser: Optional[ArgumentParser] = None,
        pipeline: Optional[Sequence[Task]] = None,
    ) -> None:
        """__init__."""
        self.args: Optional[Namespace] = None
        self.parser = parser

        # inferred type of self.pipeline must not be optional...
        self.pipeline = cast(Sequence[Task], pipeline)
        if parser:
            self.inject_arguments(parser)
            if not argv:
                argv = sys_argv[1:]
            self.args = parser.parse_args(argv)

        # ... because self.pipeline is not optional
        assert self.pipeline is not None

    def __call__(self) -> Batch:
        """Run."""
        self.check()
        with self.open_batch() as batch:
            for task in self.pipeline:
                task(batch, self)
            logger.info(
                '"pipeline:" %s',
                ", ".join(
                    map(lambda s: str(s).split(" ")[0][1:], self.pipeline)
                ),
            )
            return batch

    def check(self) -> None:
        """Check."""
        # TODO add smoke test for each database mixin.

    def inject_arguments(  # pylint: disable=no-self-use,protected-access
        self, parser: ArgumentParser
    ) -> None:
        """Inject arguments."""
        parser._default_config_files = [
            "/local/config.yaml",
            "/secrets/config.yaml",
        ]
        parser._ignore_unknown_config_file_keys = True
        parser.add(
            "-c",
            "--config",
            is_config_file=True,
            help="config file path",
            env_var="CONFIG",  # make ENV match default metavar
        )

    @contextmanager
    def open_batch(  # pylint: disable=no-self-use,unused-argument
        self, key: Any = None, model: Optional[Model] = None
    ) -> Generator[Batch, None, None]:
        """Open batch."""
        record = Interval(on=datetime.now(timezone.utc), end=None)
        yield Batch(key, record)
        record.end = datetime.now(timezone.utc)
        logger.info('"key": "%s"', key)

    def store_evidence(  # pylint: disable=no-self-use,unused-argument
        self, batch: Batch, *args, **kwargs
    ) -> None:
        """Store evidence."""
        while args:
            key, df, *args = args  # type: ignore
            batch.evidence[key] = df
        logger.info('"key": "%s", "count": "%s"}', key, len(batch.evidence))


class Task:  # pylint: disable=too-few-public-methods
    """Task."""

    def __call__(self, batch: Batch, service: Service) -> None:
        """__call__."""
        raise NotImplementedError()

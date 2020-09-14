# -*- coding: utf-8 -*-
"""Service."""

from __future__ import annotations

from collections import OrderedDict
from contextlib import contextmanager
from datetime import date, datetime, tzinfo
from json import dumps
from logging import getLogger
from sys import argv as sys_argv
from typing import Any, Dict, Generator, Optional, Sequence, Tuple, cast

from configargparse import ArgParser as ArgumentParser
from configargparse import Namespace
from dateutil import tz

from .dependency import (
    epoch_ms_from_utc_datetime,
    inject_float,
    inject_timezone,
    now_utc_datetime,
    utc_datetime_from_epoch_ms,
)
from .utils import configure_logger

logger = getLogger(__name__)


class Interval:  # pylint: disable=too-few-public-methods
    """Interval."""

    def __init__(self, on: datetime, end: Optional[datetime] = None):
        """__init__."""
        self.on = on
        self.end = end

    def as_doc(self) -> Dict[str, Any]:
        """As doc."""
        return {"end": self.end, "on": self.on}


class Batch:  # pylint: disable=too-few-public-methods
    """Batch."""

    def __init__(
        self,
        key: Any,
        execute: Interval,
        as_of_utc_datetime: datetime,
        timezone: tzinfo,
    ) -> None:
        """__init__."""
        self.key = key
        self.execute = execute
        self.as_of_utc_datetime = as_of_utc_datetime
        self.evidence = Evidence()
        self.timezone = timezone

    @property
    def as_of_local_datetime(self) -> datetime:
        """Return as_of local datetime."""
        return self.as_of_utc_datetime.astimezone(self.timezone)

    @property
    def as_of_local_date(self) -> date:
        """Return as of local date."""
        return self.as_of_local_datetime.date()

    def as_insert_doc(self, model) -> Dict[str, Any]:
        """As insert doc."""
        doc: Optional[Dict[str, Any]] = None
        if model is not None:
            doc = model.as_doc()
        return {
            "_id": self.key,
            "as_of_utc_datetime": self.as_of_utc_datetime,
            "execute": self.execute.as_doc(),
            "model": doc,
            "timezone": self.timezone,
        }

    def as_update_doc(self) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """As update doc."""
        return (
            {"_id": self.key},
            {
                "$set": {
                    "execute": self.execute.as_doc(),
                }
            },
        )


class Evidence(OrderedDict):
    """Evidence."""

    def __setitem__(self, key, value):
        """__setitem__."""
        if key in self:
            raise KeyError("{} has already been set".format(key))
        super().__setitem__(key, value)


class Service:
    """Service."""

    ON = dumps({"key": "main.on"})
    END = dumps({"key": "main.end"})

    @classmethod
    def main(cls):
        """Main."""
        configure_logger("dsdk")
        logger.info(cls.ON)
        service = cls(parser=ArgumentParser())
        _ = service()
        logger.info(cls.END)

    def __init__(  # pylint: disable=too-many-arguments
        self,
        argv: Optional[Sequence[str]] = None,
        parser: Optional[ArgumentParser] = None,
        pipeline: Optional[Sequence[Task]] = None,
        epoch_ms: Optional[float] = None,
        timezone: Optional[tzinfo] = None,
    ) -> None:
        """__init__."""
        self.args: Optional[Namespace] = None
        self.parser = parser

        # inferred type of self.pipeline must not be optional...
        self.pipeline = cast(Sequence[Task], pipeline)
        self.now_utc_datetime = now_utc_datetime()
        self.epoch_ms = cast(float, epoch_ms)
        self.timezone = cast(tzinfo, timezone)
        if parser:
            with self.inject_arguments(parser):
                if not argv:
                    argv = sys_argv[1:]
                self.args = parser.parse_args(argv)

        if self.epoch_ms is None:
            self.epoch_ms = epoch_ms_from_utc_datetime(self.now_utc_datetime)
        if self.timezone is None:
            self.timezone = tz.tzlocal()

        # ... because self.pipeline is not optional
        assert self.pipeline is not None

    TASK_ON = dumps({"key": "task.on", "task": "%s"})
    TASK_END = dumps({"key": "task.end", "task": "%s"})
    PIPELINE_ON = dumps({"key": "pipeline.on", "pipeline": "%s"})
    PIPELINE_END = dumps({"key": "pipeline.end", "pipeline": "%s"})

    def __call__(self) -> Batch:
        """Run."""
        with self.open_batch() as batch:
            logger.info(self.PIPELINE_ON, self.__class__.__name__)
            for task in self.pipeline:
                logger.info(self.TASK_ON, task.__class__.__name__)
                task(batch, self)
                logger.info(self.TASK_END, task.__class__.__name__)
            logger.info(self.PIPELINE_END, self.__class__.__name__)
            return batch

    @contextmanager
    def inject_arguments(  # pylint: disable=no-self-use,protected-access
        self, parser: ArgumentParser
    ) -> Generator[None, None, None]:
        """Inject arguments."""
        kwargs: Dict[str, Any] = {}
        parser._default_config_files = [
            "/local/config.yaml",
            "/local/config.yml",
            "/local/.yml",
            "/secrets/config.yaml",
            "/secrets/config.yml",
            "/secrets/.yml",
        ]
        parser._ignore_unknown_config_file_keys = True
        parser.add(
            "-c",
            "--config",
            is_config_file=True,
            help="config file path",
            env_var="CONFIG",  # make ENV match default metavar
        )
        parser.add(
            "-e",
            "--epoch-ms",
            help="epoch ms",
            env_var="EPOCH_MS",
            type=inject_float("epoch_ms", kwargs),
        )
        parser.add(
            "-t",
            "--timezone",
            help="timezone",
            env_var="TIMEZONE",
            type=inject_timezone("timezone", kwargs),
        )

        yield

        self.epoch_ms = cast(float, kwargs.get("epoch_ms"))
        self.timezone = cast(tzinfo, kwargs.get("timezone"))

    def dependency(self, key, cls, kwargs):
        """Dependency."""
        dependency = getattr(self, key)
        if dependency is not None:
            return
        logger.debug(
            "Injecting dependency: %s, %s, %s",
            key,
            cls.__name__,
            kwargs.keys(),
        )
        dependency = cls(**kwargs)
        setattr(self, key, dependency)

    BATCH_OPEN = dumps({"key": "batch.open", "on": "%s", "as_of": "%s"})
    BATCH_CLOSE = dumps({"key": "batch.close", "end": "%s"})

    @contextmanager
    def open_batch(  # pylint: disable=no-self-use,unused-argument
        self, key: Any = None, model=None
    ) -> Generator[Batch, None, None]:
        """Open batch."""
        execute = Interval(on=self.now_utc_datetime, end=None)
        as_of_utc_datetime = utc_datetime_from_epoch_ms(self.epoch_ms)
        logger.info(
            self.BATCH_OPEN, execute.on, as_of_utc_datetime, self.timezone
        )
        yield Batch(key, execute, as_of_utc_datetime, self.timezone)
        execute.end = now_utc_datetime()
        logger.info(self.BATCH_CLOSE, execute.end)

    def store_evidence(  # pylint: disable=no-self-use,unused-argument
        self, batch: Batch, *args, **kwargs
    ) -> None:
        """Store evidence."""
        # TODO It isn't really evidence if it isn't written to the data store.
        while args:
            key, df, *args = args  # type: ignore
            batch.evidence[key] = df


class Task:  # pylint: disable=too-few-public-methods
    """Task."""

    def __call__(self, batch: Batch, service: Service) -> None:
        """__call__."""
        raise NotImplementedError()

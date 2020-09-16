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
from pandas import DataFrame
from pkg_resources import DistributionNotFound, get_distribution

from .dependency import (
    Interval,
    epoch_ms_from_utc_datetime,
    get_tzinfo,
    inject_float,
    inject_str,
    now_utc_datetime,
    utc_datetime_from_epoch_ms,
)
from .utils import configure_logger

try:
    __version__ = get_distribution("dsdk").version
except DistributionNotFound:
    # package is not installed
    pass

logger = getLogger(__name__)


class Delegate:
    """Delegate."""

    def __init__(self, parent: Any):
        """__init__."""
        self.parent = parent

    @property
    def as_of_local_datetime(self) -> datetime:
        """Return as of local datetime."""
        return self.parent.as_of_local_datetime

    @property
    def as_of_utc_datetime(self) -> datetime:
        """Return as of utc datetime."""
        return self.parent.as_of_utc_datetime

    @property
    def as_of_local_date(self) -> date:
        """Return as of local date."""
        return self.parent.as_of_local_date

    @property
    def evidence(self) -> Evidence:
        """Return evidence."""
        return self.parent.evidence

    @property
    def predictions(self) -> DataFrame:
        """Return predictions."""
        return self.parent.predictions

    @predictions.setter
    def predictions(self, value: DataFrame) -> None:
        """Predictions setter."""
        self.parent.predictions = value

    @property
    def epoch_ms(self) -> float:
        """Return epoch_ms."""
        return self.parent.epoch_ms

    @property
    def time_zone(self) -> str:
        """Return time zone."""
        return self.parent.time_zone

    @property
    def tz_info(self) -> tzinfo:
        """Return tzinfo."""
        return self.parent.tz_info

    def as_insert_doc(self) -> Dict[str, Any]:
        """As insert doc."""
        return self.parent.as_insert_doc()

    def as_insert_sql(self) -> Dict[str, Any]:
        """As insert sql."""
        return self.parent.as_insert_sql()

    def as_update_doc(self) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """As update doc."""
        return self.parent.as_update_doc()


class Batch:
    """Batch."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        duration: Interval,
        as_of_utc_datetime: datetime,
        epoch_ms: float,
        time_zone: str,
        model: Optional[Any] = None,  # hack
    ) -> None:
        """__init__."""
        self.duration = duration
        self.as_of_utc_datetime = as_of_utc_datetime
        self.epoch_ms = epoch_ms
        self.evidence = Evidence()
        self.time_zone = time_zone
        self.model = model  # hack
        self.predictions: Optional[DataFrame] = None

    @property
    def as_of_local_datetime(self) -> datetime:
        """Return as of local datetime."""
        return self.as_of_utc_datetime.astimezone(self.tz_info)

    @property
    def as_of_local_date(self) -> date:
        """Return as of local date."""
        return self.as_of_local_datetime.date()

    @property
    def tz_info(self) -> tzinfo:
        """Return tz_info."""
        return get_tzinfo(self.time_zone)

    @property
    def parent(self) -> Any:
        """Return parent."""
        raise ValueError()

    def as_insert_doc(self) -> Dict[str, Any]:
        """As insert doc."""
        return {
            "as_of": self.as_of_utc_datetime,
            "epoch_ms": self.epoch_ms,
            "time_zone": self.time_zone,
        }

    def as_insert_sql(self) -> Dict[str, Any]:
        """As insert sql."""
        # duration comes from the database clock.
        return {
            "as_of": self.as_of_utc_datetime,
            "epoch_ms": self.epoch_ms,
            "time_zone": self.time_zone,
        }

    def as_update_doc(self) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """As update doc."""
        return {}, {"duration": self.duration.as_doc()}


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
        time_zone: Optional[str] = None,
    ) -> None:
        """__init__."""
        self.args: Optional[Namespace] = None
        self.parser = parser

        # inferred type of self.pipeline must not be optional...
        self.pipeline = cast(Sequence[Task], pipeline)
        self.now_utc_datetime = now_utc_datetime()
        self.epoch_ms = cast(float, epoch_ms)
        self.time_zone = cast(str, time_zone)
        if parser:
            with self.inject_arguments(parser):
                if not argv:
                    argv = sys_argv[1:]
                self.args = parser.parse_args(argv)

        if self.epoch_ms is None:
            self.epoch_ms = epoch_ms_from_utc_datetime(self.now_utc_datetime)
        if self.time_zone is None:
            self.time_zone = time_zone = "America/New_York"

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

    @property
    def tz_info(self) -> tzinfo:
        """Return tz_info."""
        return get_tzinfo(self.time_zone)

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
            "--time-zone",
            help="time_zone",
            env_var="TIME_ZONE",
            type=inject_str("time_zone", kwargs),
        )

        yield

        self.epoch_ms = cast(float, kwargs.get("epoch_ms"))
        self.time_zone = cast(str, kwargs.get("time_zone"))

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

    BATCH_OPEN = dumps(
        {"key": "batch.open", "on": "%s", "as_of": "%s", "time_zone": "%s"}
    )
    BATCH_CLOSE = dumps({"key": "batch.close", "end": "%s"})

    @contextmanager
    def open_batch(self) -> Generator[Any, None, None]:
        """Open batch."""
        duration = Interval(on=self.now_utc_datetime, end=None)
        as_of_utc_datetime = utc_datetime_from_epoch_ms(self.epoch_ms)
        logger.info(
            self.BATCH_OPEN,
            duration.on,
            as_of_utc_datetime,
            self.time_zone,
        )
        yield Batch(
            duration, as_of_utc_datetime, self.epoch_ms, self.time_zone
        )
        duration.end = now_utc_datetime()
        logger.info(self.BATCH_CLOSE, duration.end)

    def store_evidence(  # pylint: disable=no-self-use,unused-argument
        self, batch: Any, *args, **kwargs
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

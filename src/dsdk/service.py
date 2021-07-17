# -*- coding: utf-8 -*-
"""Service."""

from __future__ import annotations

import pickle
from argparse import ArgumentParser
from collections import OrderedDict
from contextlib import contextmanager
from datetime import date, datetime, tzinfo
from json import dumps
from logging import getLogger
from os import environ as os_env
from sys import argv as sys_argv
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    List,
    Mapping,
    Optional,
    Sequence,
)

from pandas import DataFrame
from pkg_resources import DistributionNotFound, get_distribution
from yaml import SafeDumper, SafeLoader, add_constructor, add_representer
from yaml import safe_load as yaml_loads

from .asset import Asset
from .env import Env
from .interval import Interval
from .utils import configure_logger, get_tzinfo, now_utc_datetime

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
    def duration(self) -> Interval:
        """Return duration."""
        return self.parent.duration

    @duration.setter
    def duration(self, value: Interval):
        """Duration setter."""
        self.parent.duration = value

    @property
    def as_of_local_datetime(self) -> datetime:
        """Return as of local datetime."""
        return self.parent.as_of_local_datetime

    @property
    def as_of(self) -> datetime:
        """Return as of utc datetime."""
        return self.parent.as_of

    @as_of.setter
    def as_of(self, value: datetime):
        """Set as_of."""
        self.parent.as_of = value

    @property
    def as_of_local_date(self) -> date:
        """Return as of local date."""
        return self.parent.as_of_local_date

    @property
    def evidence(self) -> Evidence:
        """Return evidence."""
        return self.parent.evidence

    @property
    def id(self) -> int:
        """Return id."""
        return self.parent.id

    @id.setter
    def id(self, value: int):
        """Set id."""
        self.parent.id = value

    @property
    def predictions(self) -> DataFrame:
        """Return predictions."""
        return self.parent.predictions

    @predictions.setter
    def predictions(self, value: DataFrame) -> None:
        """Predictions setter."""
        self.parent.predictions = value

    @property
    def time_zone(self) -> str:
        """Return time zone."""
        return self.parent.time_zone

    @time_zone.setter
    def time_zone(self, value: str):
        """Time zone setter."""
        self.parent.time_zone = value

    @property
    def tz_info(self) -> tzinfo:
        """Return tzinfo."""
        return self.parent.tz_info

    def as_insert_sql(self) -> Dict[str, Any]:
        """As insert sql."""
        return self.parent.as_insert_sql()


class Batch:
    """Batch."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        as_of: Optional[datetime],
        duration: Optional[Interval],
        time_zone: Optional[str],
        microservice_version: str,
    ) -> None:
        """__init__."""
        self.id: Optional[int] = None
        self.as_of = as_of
        self.duration = duration
        self.evidence = Evidence()
        self.time_zone = time_zone
        self.microservice_version = microservice_version
        self.predictions: Optional[DataFrame] = None

    @property
    def as_of_local_datetime(self) -> datetime:
        """Return as of local datetime."""
        assert self.as_of is not None
        assert self.tz_info is not None
        return self.as_of.astimezone(self.tz_info)

    @property
    def as_of_local_date(self) -> date:
        """Return as of local date."""
        return self.as_of_local_datetime.date()

    @property
    def tz_info(self) -> tzinfo:
        """Return tz_info."""
        assert self.time_zone is not None
        return get_tzinfo(self.time_zone)

    @property
    def parent(self) -> Any:
        """Return parent."""
        raise ValueError()

    def as_insert_sql(self) -> Dict[str, Any]:
        """As insert sql."""
        # duration comes from the database clock.
        return {
            "as_of": self.as_of,
            "microservice_version": self.microservice_version,
            "time_zone": self.time_zone,
        }


class Evidence(OrderedDict):
    """Evidence."""

    def __setitem__(self, key, value):
        """__setitem__."""
        if key in self:
            raise KeyError("{} has already been set".format(key))
        super().__setitem__(key, value)


class Service:  # pylint: disable=too-many-instance-attributes
    """Service."""

    ON = dumps({"key": "%s.on"})
    END = dumps({"key": "%s.end"})
    BATCH_OPEN = dumps({"key": "batch.open", "as_of": "%s", "time_zone": "%s"})
    BATCH_CLOSE = dumps({"key": "batch.close"})
    TASK_ON = dumps({"key": "task.on", "task": "%s"})
    TASK_END = dumps({"key": "task.end", "task": "%s"})
    PIPELINE_ON = dumps({"key": "pipeline.on", "pipeline": "%s"})
    PIPELINE_END = dumps({"key": "pipeline.end", "pipeline": "%s"})
    COUNT = dumps(
        {"key": "validate.count", "scores": "%s", "test": "%s", "status": "%s"}
    )
    MATCH = dumps({"key": "validate.match", "status": "%s"})
    YAML = ""

    VERSION = __version__

    @classmethod
    def as_yaml_type(cls) -> None:
        """As yaml type."""
        add_constructor(cls.YAML, cls._yaml_init, Loader=SafeLoader)
        add_representer(cls, cls._yaml_repr, Dumper=SafeDumper)

    @classmethod
    @contextmanager
    def context(
        cls,
        key: str,
        argv: Optional[List[str]] = None,
        env: Optional[Mapping[str, str]] = None,
    ):
        """Context."""
        configure_logger("dsdk")
        logger.info(cls.ON, key)
        yield cls.parse(argv=argv, env=env)
        logger.info(cls.END, key)

    @classmethod
    def create_gold(cls):
        """Create gold."""
        with cls.context("create_gold") as service:
            service.on_create_gold()

    @classmethod
    def main(cls):
        """Main."""
        with cls.context("main") as service:
            service()

    @classmethod
    def parse(
        cls,
        *,
        argv: Optional[List[str]] = None,
        env: Optional[Mapping[str, str]] = None,
    ) -> Service:
        """Parse."""
        if argv is None:
            argv = sys_argv[1:]
        assert argv is not None
        if env is None:
            env = os_env
        assert env is not None

        parser = ArgumentParser()
        parser.add_argument(
            "-c",
            "--config",
            dest="config_file",
            type=str,
            help="configuration yaml file",
            default=env.get("CONFIG", None),
        )
        parser.add_argument(
            "-e",
            "--env",
            dest="env_file",
            type=str,
            help="env file",
            default=env.get("env", None),
        )
        args = parser.parse_args(argv)
        if args.config_file is None:
            parser.error(
                " ".join(
                    (
                        "the following arguments are required:",
                        "-c/--config or CONFIG from env variable",
                    )
                )
            )
        return cls.load(
            config_file=args.config_file, env=env, env_file=args.env_file,
        )

    @classmethod
    def load(
        cls,
        *,
        config_file: str,
        env: Optional[Mapping[str, str]] = None,
        env_file: Optional[str] = None,
    ):
        """Load."""
        if env is None:
            env = os_env
        assert env is not None

        if env_file:
            with open(env_file) as fin:
                envs = fin.read()
            logger.debug("Environment variables are only from %s", env_file)
        else:
            logger.debug("Environment variables are being used.")

        with open(config_file) as fin:
            configs = fin.read()
        return cls.loads(configs=configs, env=env, envs=envs)

    @classmethod
    def loads(
        cls,
        *,
        configs: str,
        env: Mapping[str, str],
        envs: Optional[str] = None,
    ) -> Service:
        """Loads from strings."""
        if envs is not None:
            env = Env.loads(envs)

        Env.as_yaml_type(env=env)

        cls.yaml_types()
        return yaml_loads(configs)

    @classmethod
    def validate_gold(cls):
        """Validate gold."""
        with cls.context("validate_gold") as service:
            service.on_validate_gold()

    @classmethod
    def yaml_types(cls) -> None:
        """Yaml types."""
        Asset.as_yaml_type()
        Interval.as_yaml_type()

    @classmethod
    def _yaml_init(cls, loader, node):
        """Yaml init."""
        return cls(**loader.construct_mapping(node, deep=True))

    @classmethod
    def _yaml_repr(cls, dumper, self):
        """Yaml repr."""
        return dumper.represent_mapping(cls.YAML, self.as_yaml())

    def __init__(  # pylint: disable=too-many-arguments
        self,
        pipeline: Sequence[Task] = None,
        as_of: Optional[datetime] = None,
        gold: Optional[str] = None,
        time_zone: Optional[datetime] = None,
        batch_cls: Callable = Batch,
    ) -> None:
        """__init__."""
        self.gold = gold
        self.pipeline = pipeline
        self.duration: Optional[Interval] = None
        self.as_of = as_of
        self.time_zone = time_zone
        self.batch_cls = batch_cls

    def __call__(self) -> Batch:
        """Run."""
        with self.open_batch() as batch:

            # if one of the mixins didn't set these properties...
            if batch.as_of is None:
                batch.as_of = now_utc_datetime()
            if batch.time_zone is None:
                batch.time_zone = "America/New_York"
            if batch.duration is None:
                batch.duration = Interval(on=batch.as_of, end=None,)

            logger.info(self.PIPELINE_ON, self.__class__.__name__)
            for task in self.pipeline:
                logger.info(self.TASK_ON, task.__class__.__name__)
                task(batch, self)
                logger.info(self.TASK_END, task.__class__.__name__)
            logger.info(self.PIPELINE_END, self.__class__.__name__)

            # if one of the mixins did not set this property...
            if batch.duration.end is None:
                batch.duration.end = now_utc_datetime()

            return batch

    @property
    def tz_info(self) -> tzinfo:
        """Return tz_info."""
        assert self.time_zone is not None
        return get_tzinfo(self.time_zone)

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

    def as_yaml(self) -> Dict[str, Any]:
        """As yaml."""
        return {
            "as_of": self.as_of,
            "duration": self.duration,
            "gold": self.gold,
            "time_zone": self.time_zone,
        }

    def on_create_gold(self) -> Batch:
        """On create gold."""
        path = self.gold
        assert path is not None

        run = self()

        scores = self.scores(run.id)
        n_scores = scores.shape[0]
        logger.info("Write %s scores to %s", n_scores, path)
        with open(path, "wb") as fout:
            pickle.dump(
                {
                    "as_of": run.as_of,
                    "microservice_version": self.VERSION,
                    # pylint: disable=no-member
                    "model_version": self.model.version,  # type: ignore
                    "scores": scores,
                    "time_zone": run.time_zone,
                },
                fout,
            )
        return run

    def on_validate_gold(self) -> Batch:
        """On validate gold."""
        path = self.gold
        assert path is not None

        with open(path, "rb") as fin:
            gold = pickle.load(fin)

        # just set as_of and time_zone from gold
        # do not trust that config parameters match
        self.as_of = gold["as_of"]
        self.time_zone = gold["time_zone"]
        scores = gold["scores"]
        n_scores = scores.shape[0]

        run = self()

        test = self.scores(run.id)
        n_test = test.shape[0]

        n_tests = 0
        n_passes = 0

        n_tests += 1
        try:
            assert n_scores == n_test
            logger.info(self.COUNT, n_scores, n_test, "pass")
            n_passes += 1
        except AssertionError:
            logger.error(self.COUNT, n_scores, n_test, "FAIL")

        n_tests += 1
        try:
            assert (scores == test).all()
            logger.info(self.MATCH, "pass")
            n_passes += 1
        except AssertionError:
            logger.error(self.MATCH, "FAIL")

        assert n_tests == n_passes
        return run

    @contextmanager
    def open_batch(self) -> Generator[Any, None, None]:
        """Open batch."""
        logger.info(
            self.BATCH_OPEN, self.as_of, self.time_zone,
        )
        yield self.batch_cls(
            as_of=self.as_of,
            duration=self.duration,
            microservice_version=self.VERSION,
            time_zone=self.time_zone,
        )
        logger.info(self.BATCH_CLOSE)

    def scores(self, run_id):
        """Get scores."""
        raise NotImplementedError()


class Task:  # pylint: disable=too-few-public-methods
    """Task."""

    def __call__(self, batch: Batch, service: Service) -> None:
        """__call__."""
        raise NotImplementedError()

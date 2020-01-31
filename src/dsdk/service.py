# -*- coding: utf-8 -*-
"""Service."""

from __future__ import annotations

from datetime import datetime
from logging import NullHandler, getLogger
from sys import argv as default_argv
from typing import Any, Dict

from configargparse import ArgParser as ArgumentParser
from configargparse import Namespace

from .utils import WriteOnceDict

logger = getLogger(__name__)
logger.addHandler(NullHandler())


class Service:
    """Service."""

    @classmethod
    def add_arguments(cls, parser: ArgumentParser) -> None:
        """Add arguments."""
        parser.add(
            "-c",
            "--config",
            is_config_file=True,
            help="config file path",
            env_var="CONFIG_PATH",
        )

    @classmethod
    def new_parser(cls) -> ArgumentParser:
        """New parser."""
        parser = ArgumentParser(
            default_config_files=[
                "/local/config.yaml",
                "/secrets/config.yaml",
            ],
            ignore_unknown_config_file_keys=True,
        )
        cls.add_arguments(parser)
        return parser

    def __init__(self, pipeline=None, argv=None) -> None:
        """__init__."""
        if not argv:
            argv = list(default_argv[1:])
        if not pipeline:
            pipeline = []
        parser = self.new_parser()
        self.args = args = parser.parse_args(argv)
        self.info: Dict[str, Any] = {}
        self.pipeline = pipeline
        self.setup(args)

    def check(self) -> None:
        """Check."""
        pass  # pylint: disable=unnecessary-pass

    def new_batch(self) -> Batch:
        """New batch."""
        return Batch(0, self.info)

    def run(self) -> Batch:
        """Run."""
        self.check()
        batch = self.new_batch()
        for task in self.pipeline:
            logger.info(task.name)
            batch.evidence[task.name] = task.run(batch, self)
        return batch

    def setup(self, args: Namespace) -> None:
        """Setup."""
        pass  # pylint: disable=unnecessary-pass


class Batch:  # pylint: disable=too-few-public-methods
    """Batch."""

    def __init__(self, batch_id: Any, info: Dict[str, Any]) -> None:
        """__init__."""
        self.batch_id = batch_id
        self.info = info
        self.evidence = WriteOnceDict()
        # datetime.now(timezone.utc)
        self.start_time = start_time = datetime.utcnow()
        self.start_date = datetime(
            start_time.year,
            start_time.month,
            start_time.day,
            # tzinfo=timezone.utc,
        )


class Task:  # pylint: disable=too-few-public-methods
    """Task."""

    def __init__(self, name=None) -> None:
        """__init__."""
        if not name:
            name = self.__class__.__name__
        self.name = name

    def run(self, batch: Batch, service: Service):
        """Run."""
        raise NotImplementedError()

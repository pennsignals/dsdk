# -*- coding: utf-8 -*-
"""Data Science Deployment Kit."""

from datetime import datetime
from urllib.parse import unquote

from .utils import (
    WriteOnceDict,
    get_base_config,
    get_model,
    get_mongo_connection,
    get_mssql_connection,
)


class BaseBatchJob:
    """Base class for all batch jobs."""

    def __init__(self, pipeline=None):
        """__init__."""
        self.pipeline = pipeline
        self.get_config()
        self.config = self._configparser.parse_args()
        self.extra_batch_info = {}
        self.setup()
        self.evidence = WriteOnceDict()
        # datetime.now(timezone.utc)
        self.start_time = start_time = datetime.utcnow()
        self.start_date = datetime(
            start_time.year,
            start_time.month,
            start_time.day,
            # tzinfo=timezone.utc,
        )

    def check(self):
        """Check."""
        for task in self.pipeline:
            task.check(self)

    def run(self):
        """Run."""
        for task in self.pipeline:
            print(type(task).__name__)  # TODO: logging
            self.evidence[task.name] = task.run(self)

    def get_config(self):
        """Get config."""
        self._configparser = get_base_config()

    def setup(self):
        """Setup."""
        pass  # pylint: disable=unnecessary-pass


class MongoMixin(BaseBatchJob):
    """Mongo Mixin."""

    def get_config(self):
        """Get config."""
        super().get_config()
        self._configparser.add(
            "--mongouri",
            required=True,
            help="Mongo URI used to connect to MongoDB",
            env_var="MONGO_URI",
        )

    def setup(self):
        """Setup."""
        super().setup()
        self.mongo = get_mongo_connection(
            unquote(self.config.mongouri)
        ).get_default_database()


class MssqlMixin(BaseBatchJob):
    """Mssql Mixin."""

    def get_config(self):
        """Get config."""
        super().get_config()
        self._configparser.add(
            "--mssqluri",
            required=True,
            help="MS SQL URI used to connect to MS SQL Server",
            env_var="MSSQL_URI",
        )

    def setup(self):
        """Setup."""
        super().setup()
        self.mssql = get_mssql_connection(unquote(self.config.mssqluri))


class ModelMixin(BaseBatchJob):
    """Model Mixin."""

    def get_config(self):
        """Get config."""
        super().get_config()
        self._configparser.add(
            "--model",
            required=True,
            help="Path to pickled sklearn model",
            env_var="MODEL_PATH",
        )

    def setup(self):
        """Setup."""
        super().setup()
        self.model = get_model(self.config.model)
        self.extra_batch_info.update(
            {"model": self.model["name"], "version": self.model["version"]}
        )


class Task:
    """Task."""

    def __init__(self):
        """__init__."""
        if not hasattr(self, "name"):
            raise AttributeError("'name' is undefined")

    def check(self, batch):
        """Check."""
        pass  # pylint: disable=unnecessary-pass

    def run(self, batch):
        """Run."""
        raise NotImplementedError()

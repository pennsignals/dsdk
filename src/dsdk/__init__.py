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
        self.get_config()
        self.config = self._configparser.parse_args()
        self.extra_batch_info = {}
        self.setup()
        self.set_pipeline(pipeline)
        self.evidence = WriteOnceDict()
        self.start_time = datetime.utcnow()
        self.start_date = datetime(
            self.start_time.year, self.start_time.month, self.start_time.day
        )

    def run(self):
        """Run."""
        for block in self.pipeline:
            print(type(block).__name__)  # TODO: logging
            self.evidence[block.name] = block.run()

    def set_pipeline(self, pipeline):
        """Set pipeline."""
        if pipeline is None:
            pipeline = []
        self.pipeline = pipeline
        for block in self.pipeline:
            block.batch = self

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
        super(MongoMixin, self).get_config()
        self._configparser.add(
            "--mongouri",
            required=True,
            help="Mongo URI used to connect to MongoDB",
            env_var="MONGO_URI",
        )

    def setup(self):
        """Setup."""
        super(MongoMixin, self).setup()
        self.mongo = get_mongo_connection(
            unquote(self.config.mongouri)
        ).get_default_database()


class MssqlMixin(BaseBatchJob):
    """Mssql Mixin."""

    def get_config(self):
        """Get config."""
        super(MssqlMixin, self).get_config()
        self._configparser.add(
            "--mssqluri",
            required=True,
            help="MS SQL URI used to connect to MS SQL Server",
            env_var="MSSQL_URI",
        )

    def setup(self):
        """Setup."""
        super(MssqlMixin, self).setup()
        self.mssql = get_mssql_connection(unquote(self.config.mssqluri))


class ModelMixin(BaseBatchJob):
    """Model Mixin."""

    def get_config(self):
        """Get config."""
        super(ModelMixin, self).get_config()
        self._configparser.add(
            "--model",
            required=True,
            help="Path to pickled sklearn model",
            env_var="MODEL_PATH",
        )

    def setup(self):
        """Setup."""
        super(ModelMixin, self).setup()
        self.model = get_model(self.config.model)
        self.extra_batch_info.update(
            {"model": self.model["name"], "version": self.model["version"]}
        )


class Block:  # pylint: disable=too-few-public-methods
    """Block."""

    def __init__(self):
        """__init__."""
        if not hasattr(self, "name"):
            raise AttributeError("'name' is undefined")

    def run(self):
        """Run."""
        raise NotImplementedError

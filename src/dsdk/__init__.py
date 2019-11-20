__version__ = "0.1.0"
from datetime import datetime

from .utils import WriteOnceDict
from .utils import get_base_config
from .utils import get_model
from .utils import get_mongo_connection
from .utils import get_mssql_connection


class BaseBatchJob:
    """Base class for all batch jobs."""

    def __init__(self, blocks=None):
        self.get_config()
        self.config = self._configparser.parse_args()
        self.setup()
        self.set_blocks(blocks)
        self.evidence = WriteOnceDict()
        self.start_time = datetime.now()
        self.start_date = datetime(
            self.start_time.year, self.start_time.month, self.start_time.day
        )
        for block in self.blocks:
            self.evidence[block.name] = block.run()

    def set_blocks(self, blocks):
        if blocks is None:
            blocks = []
        self.blocks = blocks
        for block in self.blocks:
            block.batch = self

    def get_config(self):
        self._configparser = get_base_config()

    def setup(self):
        pass


class MongoMixin(BaseBatchJob):
    def get_config(self):
        super(MongoMixin, self).get_config()
        self._configparser.add(
            "--mongouri",
            required=True,
            help="Mongo URI used to connect to MongoDB",
            env_var="MONGO_URI",
        )

    def setup(self):
        super(MongoMixin, self).setup()
        self.mongo = get_mongo_connection(self.config.mongouri).get_default_database()


class MssqlMixin(BaseBatchJob):
    def get_config(self):
        super(MssqlMixin, self).get_config()
        self._configparser.add(
            "--mssqluri",
            required=True,
            help="MS SQL URI used to connect to MS SQL Server",
            env_var="MSSQL_URI",
        )

    def setup(self):
        super(MssqlMixin, self).setup()
        self.mssql = get_mssql_connection(self.config.mssqluri)


class ModelMixin(BaseBatchJob):
    def get_config(self):
        super(ModelMixin, self).get_config()
        self._configparser.add(
            "--model", required=True, help="Path to pickled sklearn model", env_var="MODEL_PATH"
        )

    def setup(self):
        super(ModelMixin, self).setup()
        self.model = get_model(self.config.model)


class Block:
    def run(self, evidence):
        raise NotImplementedError

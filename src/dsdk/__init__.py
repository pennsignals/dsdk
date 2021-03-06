# -*- coding: utf-8 -*-
"""Data Science Deployment Kit."""

from .dependency import Interval, namespace_directory, now_utc_datetime
from .model import Mixin as ModelMixin
from .model import Model
from .mongo import EvidenceMixin as MongoEvidenceMixin
from .mongo import Mixin as MongoMixin
from .mongo import Persistor as MongoPersistor
from .mssql import CheckTablePrivileges as CheckMssqlTablePrivileges
from .mssql import Mixin as MssqlMixin
from .mssql import Persistor as MssqlPersistor
from .postgres import CheckTablePrivileges as CheckPostgresTablePrivileges
from .postgres import Mixin as PostgresMixin
from .postgres import Persistor as PostgresPersistor
from .postgres import PredictionMixin as PostgresPredictionMixin
from .service import Batch, Delegate, Service, Task
from .utils import (
    chunks,
    configure_logger,
    dump_json_file,
    dump_pickle_file,
    load_json_file,
    load_pickle_file,
    retry,
)

__all__ = (
    "Batch",
    "Delegate",
    "Interval",
    "Model",
    "ModelMixin",
    "MongoMixin",
    "MongoPersistor",
    "MongoEvidenceMixin",
    "MssqlMixin",
    "MssqlPersistor",
    "CheckMssqlTablePrivileges",
    "CheckPostgresTablePrivileges",
    "PostgresPredictionMixin",
    "PostgresMixin",
    "PostgresPersistor",
    "Service",
    "Task",
    "chunks",
    "configure_logger",
    "dump_json_file",
    "dump_pickle_file",
    "load_json_file",
    "load_pickle_file",
    "namespace_directory",
    "now_utc_datetime",
    "retry",
)

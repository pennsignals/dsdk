# -*- coding: utf-8 -*-
"""Data Science Deployment Kit."""

from .dependency import namespace_directory
from .model import Mixin as ModelMixin
from .model import Model
from .mongo import EvidenceMixin as MongoEvidenceMixin
from .mongo import EvidencePersistor as MongoEvidencePersistor
from .mongo import Mixin as MongoMixin
from .mongo import Persistor as MongoPersistor
from .mssql import AlchemyMixin as MssqlAlchemyMixin
from .mssql import AlchemyPersistor as MssqlAlchemyPersistor
from .mssql import CheckTablePrivileges as CheckMssqlTablePrivileges
from .mssql import Mixin as MssqlMixin
from .mssql import Persistor as MssqlPersistor
from .postgres import CheckTablePrivileges as CheckPostgresTablePrivileges
from .postgres import Mixin as PostgresMixin
from .postgres import Persistor as PostgresPersistor
from .postgres import PredictionMixin as PostgresPredictionMixin
from .service import Batch, Service, Task
from .utils import (
    chunks,
    configure_logger,
    df_from_query,
    df_from_query_by_ids,
    dump_json_file,
    dump_pickle_file,
    load_json_file,
    load_pickle_file,
    retry,
)

__all__ = (
    "Batch",
    "Model",
    "ModelMixin",
    "MongoMixin",
    "MongoPersistor",
    "MongoEvidenceMixin",
    "MongoEvidencePersistor",
    "MssqlAlchemyMixin",
    "MssqlAlchemyPersistor",
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
    "df_from_query_by_ids",
    "df_from_query",
    "dump_json_file",
    "dump_pickle_file",
    "load_json_file",
    "load_pickle_file",
    "namespace_directory",
    "retry",
)

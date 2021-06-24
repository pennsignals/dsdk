# -*- coding: utf-8 -*-
"""Test dsdk."""

from argparse import Namespace
from typing import Any, Dict

from configargparse import ArgParser as ArgumentParser
from pandas import DataFrame
from pytest import mark

from dsdk import (
    Batch,
    Model,
    ModelMixin,
    MssqlMixin,
    MssqlPersistor,
    PostgresMixin,
    PostgresPersistor,
    Service,
    Task,
    dump_pickle_file,
    namespace_directory,
    retry,
)


class _Extract(Task):  # pylint: disable=too-few-public-methods
    def __call__(self, batch: Batch, service: Service) -> None:
        pass


class _Transform(Task):  # pylint: disable=too-few-public-methods
    def __call__(self, batch: Batch, service: Service) -> None:
        pass


class _Predict(Task):  # pylint: disable=too-few-public-methods
    def __call__(self, batch: Batch, service: Service) -> None:
        pass


def test_batch_evidence():
    """Test batch evidence."""

    df = DataFrame()

    class _MockTask(Task):  # pylint: disable=too-few-public-methods
        def __call__(self, batch: Batch, service: Service) -> None:
            batch.evidence["test"] = df

    service = Service(pipeline=(_MockTask(),))
    batch = service()
    assert len(batch.evidence) == 1
    assert batch.evidence["test"] is df


def mixin_kwargs():
    """Return mixin kwargs."""

    # TODO
    # do not use filesystem for init of sql namespaces
    model = Model("test", "0.0.1-rc.1")
    mssql = MssqlPersistor(
        username="username",
        password="password",
        host="host",
        port=1433,
        database="database",
        sql=namespace_directory("./sql/mssql"),
        tables=("foo", "bar", "baz"),
    )
    postgres = PostgresPersistor(
        username="username",
        password="password",
        host="host",
        port=5432,
        database="database",
        sql=namespace_directory("./sql/postgres"),
        tables=("foo", "bar", "baz"),
    )
    return {
        "model": model,
        "mssql": mssql,
        "postgres": postgres,
    }


def mixin_parser_kwargs():
    """Return mixin parser kwargs."""
    model = "./model.pkl"
    dump_pickle_file(Model(name="test", version="0.0.1"), model)

    mssql = Namespace()
    mssql.database = "test"
    mssql.host = "host"
    mssql.password = "password"
    mssql.port = 1433
    mssql.sql = "./sql/mssql"
    mssql.tables = ("foo", "bar", "baz")
    mssql.username = "username"

    postgres = Namespace()
    postgres.database = "test"
    postgres.host = "host"
    postgres.password = "password"
    postgres.port = 5432
    postgres.sql = "./sql/postgres"
    postgres.tables = ("foo", "bar", "baz")
    postgres.username = "username"

    argv = [
        "--model",
        model,
        "--mssql-database",
        mssql.database,
        "--mssql-host",
        mssql.host,
        "--mssql-password",
        mssql.password,
        "--mssql-port",
        str(mssql.port),
        "--mssql-sql",
        mssql.sql,
        "--mssql-tables",
        ",".join(mssql.tables),
        "--mssql-username",
        mssql.username,
        "--postgres-database",
        postgres.database,
        "--postgres-host",
        postgres.host,
        "--postgres-password",
        postgres.password,
        "--postgres-port",
        str(postgres.port),
        "--postgres-sql",
        postgres.sql,
        "--postgres-tables",
        ",".join(postgres.tables),
        "--postgres-username",
        postgres.username,
    ]
    parser = ArgumentParser(argv)
    return {"argv": argv, "parser": parser}


@mark.parametrize("kwargs", (mixin_kwargs(), mixin_parser_kwargs()))
def test_mixin_service(kwargs: Dict[str, Any]):
    """Test postgres, mssql mixin."""

    class _Service(MssqlMixin, PostgresMixin, ModelMixin, Service):
        def __init__(self, **kwargs):
            pipeline = (_Extract, _Transform, _Predict)
            super().__init__(pipeline=pipeline, **kwargs)

    service = _Service(**kwargs)
    assert service.postgres.__class__ is PostgresPersistor
    assert service.mssql.__class__ is MssqlPersistor
    assert service.model.__class__ is Model


def test_retry_other_exception():
    """Test retry other exception."""

    exceptions_in = [
        RuntimeError("what?"),
        NotImplementedError("how?"),
        RuntimeError("no!"),
    ]
    actual = []
    expected = [1.0, 1.5, 2.25]

    def sleep(wait: float):
        actual.append(wait)

    @retry(
        (NotImplementedError, RuntimeError),
        retries=4,
        delay=1.0,
        backoff=1.5,
        sleep=sleep,
    )
    def explode():
        raise exceptions_in.pop()

    try:
        explode()
        raise AssertionError("IndexError expected")
    except IndexError:
        assert actual == expected


def test_retry_exhausted():
    """Test retry."""

    exceptions_in = [
        RuntimeError("what?"),
        NotImplementedError("how?"),
        RuntimeError("no!"),
        NotImplementedError("when?"),
    ]
    actual = []
    expected = [1.0, 1.5]

    def sleep(wait: float):
        actual.append(wait)

    @retry(
        (NotImplementedError, RuntimeError),
        retries=2,
        delay=1.0,
        backoff=1.5,
        sleep=sleep,
    )
    def explode():
        raise exceptions_in.pop()

    try:
        explode()
        raise AssertionError("NotImplementedError expected")
    except NotImplementedError as exception:
        assert actual == expected
        assert str(exception) == "when?"

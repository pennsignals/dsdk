# -*- coding: utf-8 -*-
"""Test dsdk."""

from sys import version_info
from typing import Any, Dict

from configargparse import ArgParser as ArgumentParser
from pandas import DataFrame
from pytest import mark

from dsdk import (
    Batch,
    Model,
    ModelMixin,
    MongoEvidenceMixin,
    MssqlAlchemyMixin,
    Service,
    Task,
    dump_json_file,
    dump_pickle_file,
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
            service.store_evidence(batch, "test", df)

    service = Service(pipeline=(_MockTask(),))
    batch = service()
    assert len(batch.evidence) == 1
    assert batch.evidence["test"] is df


def mongo_mixin_parser_kwargs() -> Dict[str, Any]:
    """Mongo mixin with parser."""
    config_path = "./config.json"
    config: Dict[str, Any] = {}
    dump_json_file(config, config_path)

    model = Model(name="test", version="0.0.1")
    model_path = "./model.pkl"
    dump_pickle_file(model, model_path)

    mongo_uri = "mongodb://mongo/database?authsource=admin"

    argv = [
        "--config",
        config_path,
        "--model",
        model_path,
        "--mongo-uri",
        mongo_uri,
    ]
    parser = ArgumentParser()
    return {"argv": argv, "parser": parser}


def mongo_mixin_kwargs() -> Dict[str, Any]:
    """Return mongo mixin kwargs."""
    model = Model(name="test", version="0.0.1")
    mongo_uri = "mongodb://mongo/database?authsource=admin"

    return {"model": model, "mongo_uri": mongo_uri}


@mark.parametrize(
    "kwargs", [mongo_mixin_parser_kwargs(), mongo_mixin_kwargs()]
)
def test_mongo_mixin(kwargs: Dict[str, Any]) -> None:
    """Test mongo mixin."""

    class _App(MongoEvidenceMixin, ModelMixin, Service):
        def __init__(self, **kwargs):
            pipeline = (_Extract, _Transform, _Predict)
            super().__init__(pipeline=pipeline, **kwargs)

    _ = _App(**kwargs)


def mssql_mixin_parser_kwargs():
    """Return mssql mixin parser kwargs."""
    config_path = "./config.json"
    config: Dict[str, Any] = {
        "mssql-sql": "./sql/mssql",
        "mssql-tables": "foo,bar,baz",
    }
    dump_json_file(config, config_path)

    model = Model(name="test", version="0.0.1")
    model_path = "./model.pkl"
    dump_pickle_file(model, model_path)

    mssql_sql = "./sql/mssql"
    mssql_tables = "foo,bar,baz"
    mssql_uri = "mssql+pymssql://mssql?test"

    argv = [
        "--config",
        config_path,
        "--model",
        model_path,
        "--mssql-sql",
        mssql_sql,
        "--mssql-tables",
        mssql_tables,
        "--mssql-uri",
        mssql_uri,
    ]
    parser = ArgumentParser()
    return {"argv": argv, "parser": parser}


def mssql_mixin_kwargs():
    """Return mssql mixin kwargs."""
    model = Model(name="test", version="0.0.1")
    mssql_sql = "./sql/mssql"
    mssql_tables = "foo,bar,baz"
    mssql_uri = "mssql+pymssql://mssql?test"

    return {
        "model": model,
        "mssql_sql": mssql_sql,
        "mssql_tables": mssql_tables,
        "mssql_uri": mssql_uri,
    }


@mark.skipif(
    version_info >= (3, 8), reason="pymssql not supported >= python 3.8"
)
@mark.parametrize(
    "kwargs", [mssql_mixin_parser_kwargs(), mssql_mixin_kwargs()]
)
def test_mssql_mixin(kwargs: Dict[str, Any]) -> None:
    """Test mssql mixin."""

    class _App(MssqlAlchemyMixin, ModelMixin, Service):
        def __init__(self, **kwargs):
            pipeline = (_Extract, _Transform, _Predict)
            super().__init__(pipeline=pipeline, **kwargs)

    _ = _App(**kwargs)


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

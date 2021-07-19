# -*- coding: utf-8 -*-
"""Test dsdk."""

from io import StringIO
from typing import Any, Callable, Dict, Tuple

from pandas import DataFrame
from pytest import mark

from dsdk import (
    Asset,
    Batch,
    Model,
    ModelMixin,
    Mssql,
    MssqlMixin,
    Postgres,
    PostgresMixin,
    Service,
    Task,
    dump_pickle_file,
    retry,
    yaml_dumps,
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


class MyService(ModelMixin, MssqlMixin, PostgresMixin, Service):
    """MyService."""

    YAML = "!myservice"

    @classmethod
    def yaml_types(cls):
        """Yaml types."""
        cls.as_yaml_type()
        super().yaml_types()

    def __init__(self, **kwargs):
        """__init__."""
        pipeline = (_Extract, _Transform, _Predict)
        super().__init__(pipeline=pipeline, **kwargs)


def build_from_parameters(cls) -> Tuple[Callable, Dict[str, Any]]:
    """Build from parameters."""
    model = Model(name="test", path="./test/model.pkl", version="0.0.1-rc.1")
    mssql = Mssql(
        username="username",
        password="password",
        host="host",
        port=1433,
        database="database",
        sql=Asset.build(path="./assets/mssql", ext=".sql"),
        tables=("foo", "bar", "baz"),
    )
    postgres = Postgres(
        username="username",
        password="password",
        host="host",
        port=5432,
        database="database",
        sql=Asset.build(path="./assets/postgres", ext=".sql"),
        tables=("foo", "bar", "baz"),
    )
    return (
        cls,
        {"model": model, "mssql": mssql, "postgres": postgres},
    )


def build_from_yaml(cls) -> Tuple[Callable, Dict[str, Any]]:
    """Build from yaml."""
    pickle_file = "./test/model.pkl"
    dump_pickle_file(
        Model(name="test", path=pickle_file, version="0.0.1"), pickle_file
    )

    configs = StringIO(
        """
!myservice
mssql: !mssql
  database: test
  host: 0.0.0.0
  password: ${MSSQL_PASSWORD}
  port: 1433
  sql: ./assets/mssql
  tables:
  - foo
  - bar
  - baz
  username: mssql
model: !model ./test/model.pkl
postgres: !postgres
  database: test
  host: 0.0.0.0
  password: ${POSTGRES_PASSWORD}
  port: 5432
  sql: ./asset/postgres
  tables:
  - foo
  - bar
  - baz
  username: postgres
"""
    )
    env = {"POSTGRES_PASSWORD": "oops!", "MSSQL_PASSWORD": "oops!"}
    envs = StringIO(
        """
MSSQL_PASSWORD=password
POSTGRES_PASSWORD=password
"""
    )
    return (
        cls.loads,
        {"configs": configs, "env": env, "envs": envs},
    )


@mark.parametrize(
    "cls,kwargs",
    (build_from_yaml(MyService), build_from_parameters(MyService)),
)
def test_service(
    cls,  # pylint: disable=redefined-outer-name
    kwargs: Dict[str, Any],
):
    """Test parameters, config, and env."""
    service = cls(**kwargs)
    assert service.__class__ is MyService
    assert service.model.__class__ is Model
    assert service.postgres.__class__ is Postgres
    assert service.mssql.__class__ is Mssql
    assert service.postgres.password == "password"
    assert service.mssql.password == "password"
    buf = StringIO()
    buf.write(yaml_dumps(service))
    expected = """

    """
    assert buf.getvalue() == expected


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


if __name__ == "__main__":
    cls, kwargs = build_from_yaml(MyService)
    test_service(cls, kwargs)

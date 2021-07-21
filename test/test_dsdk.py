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
        username="mssql",
        password="password",
        host="0.0.0.0",
        port=1433,
        database="test",
        sql=Asset.build(path="./assets/mssql", ext=".sql"),
        tables=("a", "b", "c"),
    )
    postgres = Postgres(
        username="postgres",
        password="password",
        host="0.0.0.0",
        port=5432,
        database="test",
        sql=Asset.build(path="./assets/postgres", ext=".sql"),
        tables=("ichi", "ni", "san", "shi", "go"),
    )
    return (
        cls,
        {"model": model, "mssql": mssql, "postgres": postgres},
    )


CONFIGS = """
!myservice
mssql: !mssql
  database: test
  host: 0.0.0.0
  password: ${MSSQL_PASSWORD}
  port: 1433
  sql: !asset
    ext: .sql
    path: ./assets/mssql
  tables:
  - a
  - b
  - c
  username: mssql
model: !model ./test/model.pkl
postgres: !postgres
  database: test
  host: 0.0.0.0
  password: ${POSTGRES_PASSWORD}
  port: 5432
  sql: !asset
    ext: .sql
    path: ./assets/postgres
  tables:
  - ichi
  - ni
  - san
  - shi
  - go
  username: postgres
""".strip()

ENVS = """
MSSQL_PASSWORD=password
POSTGRES_PASSWORD=password
""".strip()

EXPECTED = """
!myservice
as_of: null
duration: null
gold: null
model: !model ./test/model.pkl
mssql: !mssql
  database: test
  host: 0.0.0.0
  password: password
  port: 1433
  sql: !asset
    ext: .sql
    path: ./assets/mssql
  tables:
  - a
  - b
  - c
  username: mssql
postgres: !postgres
  database: test
  host: 0.0.0.0
  password: password
  port: 5432
  sql: !asset
    ext: .sql
    path: ./assets/postgres
  tables:
  - ichi
  - ni
  - san
  - shi
  - go
  username: postgres
time_zone: null
""".strip()


def build_from_yaml(cls) -> Tuple[Callable, Dict[str, Any]]:
    """Build from yaml."""
    pickle_file = "./test/model.pkl"
    dump_pickle_file(
        Model(name="test", path=pickle_file, version="0.0.1"), pickle_file
    )

    configs = StringIO(CONFIGS)
    env = {"POSTGRES_PASSWORD": "oops!", "MSSQL_PASSWORD": "oops!"}
    envs = StringIO(ENVS)
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
    expected = EXPECTED
    service = cls(**kwargs)
    assert service.__class__ is MyService
    assert service.model.__class__ is Model
    assert service.postgres.__class__ is Postgres
    assert service.mssql.__class__ is Mssql
    assert service.postgres.password == "password"
    assert service.mssql.password == "password"
    buf = StringIO()
    buf.write(yaml_dumps(service))
    actual = buf.getvalue().strip()
    assert actual == expected


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
    test_service(*build_from_yaml(MyService))

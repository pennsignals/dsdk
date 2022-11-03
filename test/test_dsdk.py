"""Test dsdk."""

from __future__ import annotations

from io import StringIO
from logging import getLogger
from typing import Any, Callable

from cfgenvy import yaml_dumps
from pandas import DataFrame
from pytest import mark

from dsdk import (
    Asset,
    Batch,
    Flowsheet,
    FlowsheetMixin,
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
)

logger = getLogger(__name__)


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


class MockService(  # pylint: disable=too-many-ancestors
    FlowsheetMixin, ModelMixin, MssqlMixin, PostgresMixin, Service
):
    """MockService."""

    YAML = "!test"

    @classmethod
    def yaml_types(cls):
        """As yaml type."""
        logger.debug("MockService.yaml_types()")
        Model.as_yaml_type()
        super().yaml_types()

    def __init__(self, **kwargs) -> None:
        """__init__."""
        pipeline = (_Extract, _Transform, _Predict)
        super().__init__(pipeline=pipeline, **kwargs)


CONFIGS = """
!test
flowsheets: !flowsheets
  client_id: 00000000-0000-0000-0000-000000000000
  cookie: ASP.NET_SessionId=000000000000000000000000
  kinds:
    score:
      flowsheet_id: '0000000000'
      flowsheet_template_id: '0000000000'
  password: ${EPIC_PASSWORD}
  url: https://interconnectbgprod.uphs.upenn.edu/interconnect-prd-web/
  username: epic
model: !model ./test/0.0.1.pkl
mssql: !mssql
  database: test
  host: 0.0.0.0
  password: ${MSSQL_PASSWORD}
  sql: !asset
    ext: .sql
    path: ./assets/mssql
  username: mssql
postgres: !postgres
  database: test
  host: 0.0.0.0
  password: ${POSTGRES_PASSWORD}
  schema: example
  sql: !asset
    ext: .sql
    path: ./assets/postgres
  username: postgres
""".strip()

ENVS = """
EPIC_PASSWORD=password
MSSQL_PASSWORD=password
POSTGRES_PASSWORD=password
""".strip()

EXPECTED = """
!test
as_of: null
duration: null
flowsheets: !flowsheets
  client_id: 00000000-0000-0000-0000-000000000000
  contact_id_type: CSN
  cookie: ASP.NET_SessionId=000000000000000000000000
  kinds:
    score:
      flowsheet_id: '0000000000'
      flowsheet_id_type: external
      flowsheet_template_id: '0000000000'
      flowsheet_template_id_type: external
  password: password
  patient_id_type: UID
  url: https://interconnectbgprod.uphs.upenn.edu/interconnect-prd-web/
  user_id: PENNSIGNALS
  user_id_type: external
  username: epic
gold: null
model: !model ./test/0.0.1.pkl
mssql: !mssql
  database: test
  host: 0.0.0.0
  password: password
  port: 1433
  schema: dbo
  sql: !asset
    ext: .sql
    path: ./assets/mssql
  username: mssql
poll_interval: 60
postgres: !postgres
  database: test
  host: 0.0.0.0
  password: password
  port: 5432
  schema: example
  sql: !asset
    ext: .sql
    path: ./assets/postgres
  username: postgres
time_zone: null
""".strip()


def build(
    cls,
    expected: str = EXPECTED,
) -> tuple[Callable, dict[str, Any], str]:
    """Build from parameters."""
    cls.yaml_types()
    flowsheets = Flowsheet(
        client_id="00000000-0000-0000-0000-000000000000",
        cookie="ASP.NET_SessionId=000000000000000000000000",
        kinds={
            "score": {
                "flowsheet_id": "0000000000",
                "flowsheet_template_id": "0000000000",
            },
        },
        password="password",
        url="https://interconnectbgprod.uphs.upenn.edu/interconnect-prd-web/",
        username="epic",
    )
    model = Model(name="test", path="./test/0.0.1.pkl", version="0.0.1")
    mssql = Mssql(
        database="test",
        host="0.0.0.0",
        password="password",
        sql=Asset.build(path="./assets/mssql", ext=".sql"),
        username="mssql",
    )
    postgres = Postgres(
        database="test",
        host="0.0.0.0",
        password="password",
        schema="example",
        sql=Asset.build(path="./assets/postgres", ext=".sql"),
        username="postgres",
    )
    return (
        cls,
        {
            "flowsheets": flowsheets,
            "model": model,
            "mssql": mssql,
            "postgres": postgres,
        },
        expected,
    )


def deserialize(
    cls,
    configs: str = CONFIGS,
    envs: str = ENVS,
    expected: str = EXPECTED,
) -> tuple[Callable, dict[str, Any], str]:
    """Build from yaml."""
    pickle_file = "./test/0.0.1.pkl"
    dump_pickle_file({"name": "test", "version": "0.0.1"}, pickle_file)

    env = {
        "EPIC_PASSWORD": "oops!",
        "MSSQL_PASSWORD": "oops!",
        "POSTGRES_PASSWORD": "oops!",
    }
    return (
        cls.loads,
        {
            "configs": StringIO(configs),
            "env": env,
            "envs": StringIO(envs),
        },
        expected,
    )


@mark.parametrize(
    "cls,kwargs,expected",
    (
        build(MockService),
        deserialize(MockService),
    ),
)
def test_service(
    cls: Callable,  # pylint: disable=redefined-outer-name
    kwargs: dict[str, Any],
    expected: str,
):
    """Test parameters, config, and env."""
    service = cls(**kwargs)
    assert service.__class__ is MockService
    assert service.model.__class__ is Model
    assert service.postgres.__class__ is Postgres
    assert service.mssql.__class__ is Mssql
    assert service.postgres.password == "password"
    assert service.mssql.password == "password"
    actual = yaml_dumps(service).strip()
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


def test_asset():
    """Test asset traversal."""
    asset = Asset(
        ext=".sql",
        path="./predict/sql/mssql",
        run=Asset(
            ext=".sql",
            path="./predict/sql/mssql/run",
            select="select * from runs",
        ),
        cohort="select * from patients",
    )
    actual = tuple(each for each in asset())
    expected = (
        ("run.select", "select * from runs"),
        ("cohort", "select * from patients"),
    )
    assert actual == expected

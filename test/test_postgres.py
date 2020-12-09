# -*- coding: utf-8 -*-
"""Test postgres."""

from contextlib import contextmanager
from os import environ
from typing import Any, Generator

from pandas import DataFrame

from dsdk import Batch, PostgresPersistor, configure_logger
from dsdk.dependency import namespace_directory
from dsdk.model import Batch as ModelBatch

logger = configure_logger(__name__)


class Persistor(PostgresPersistor):
    """Persistor."""

    def __init__(
        self,
        env=None,
        **kwargs,
    ):
        """__init__."""
        if env is None:
            env = environ
        self.attempts = 0
        super().__init__(
            username=kwargs.get(
                "username", env.get("POSTGRES_USERNAME", "postgres")
            ),
            password=kwargs.get(
                "password", env.get("POSTGRES_PASSWORD", "postgres")
            ),
            host=kwargs.get("host", env.get("POSTGRES_HOST", "postgres")),
            port=kwargs.get("port", int(env.get("POSTGRES_PORT", "5432"))),
            database=kwargs.get(
                "database", env.get("POSTGRES_DATABASE", "test")
            ),
            sql=namespace_directory(
                kwargs.get("sql", env.get("POSTGRES_SQL", "./sql/postgres"))
            ),
            tables=kwargs.get(
                "tables",
                env.get(
                    "POSTGRES_TABLES",
                    ",".join(
                        (
                            "dsdk.models",
                            "dsdk.microservices",
                            "dsdk.runs",
                            "dsdk.predictions",
                        )
                    ),
                ).split(","),
            ),
        )

    @contextmanager
    def connect(self) -> Generator[Any, None, None]:
        """Connect."""
        self.attempts += 1
        with super().connect() as con:
            yield con


def test_connect():
    """Test connect."""
    persistor = Persistor()
    with persistor.connect() as con:
        logger.info(con.info)


def test_check_ok():
    """Test check OK."""
    persistor = Persistor()
    with persistor.rollback() as cur:
        persistor.check(cur)


def test_check_not_ok():
    """Test check not OK."""
    persistor = Persistor(tables=("test.dne",))
    try:
        with persistor.rollback() as cur:
            persistor.check(cur)
    except RuntimeError:
        return
    raise AssertionError("Schema check passed even though table dne.")


def test_cursor():
    """Test cursor."""
    persistor = Persistor()
    with persistor.rollback() as cur:
        cur.execute("""select 1 as n""")
        for row in cur.fetchall():
            assert row["n"] == 1


def test_open_run(
    data=(
        (0, 0.75, True, False, False),
        (1, 0.25, True, False, False),
        (2, 0.75, False, True, False),
        (3, 0.25, False, True, False),
        (4, 0.75, False, False, True),
        (5, 0.25, False, False, True),
    ),
    in_columns=(
        "subject_id",
        "greenish",
        "is_animal",
        "is_vegetable",
        "is_mineral",
    ),
    check="""
select
    run_id,
    subject_id,
    score,
    greenish,
    is_animal,
    is_vegetable,
    is_mineral
from
    predictions
    natural join features id
where
    run_id = %(run_id)s""",
):
    """Test open_run."""
    batch = Batch(
        as_of=None,
        duration=None,
        microservice_version="1.0.0",
        time_zone=None,
    )
    persistor = Persistor()
    model_batch = ModelBatch(model_version="1.0.0", parent=batch)
    with persistor.open_run(parent=model_batch) as run:
        df = DataFrame(data=data, columns=in_columns)
        df.set_index("subject_id")
        df["score"] = ~df["is_mineral"] * (
            (df["is_animal"] * df["greenish"])
            + (df["is_vegetable"] * (1.0 - df["greenish"]))
        )
        run.predictions = df

    with persistor.rollback() as cur:
        cur.execute(persistor.sql.schema)
        cur.execute(check, {"run_id": run.id})
        out_columns = tuple(d[0] for d in cur.description)
        df = DataFrame(cur.fetchall(), columns=out_columns)
        df.set_index("subject_id")

    # reorder columns to match run.predictions
    df = df[run.predictions.columns]
    # logger.error(df.head(10))
    # logger.error(run.predictions.head(10))
    assert df.equals(run.predictions)


def test_retry_connect():
    """Test retry_connect."""


def test_store_evidence():
    """Test store evidence."""


def test_store_df():
    """Test store_df."""

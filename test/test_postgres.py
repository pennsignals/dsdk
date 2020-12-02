# -*- coding: utf-8 -*-
"""Test postgres."""

from os import environ

from dsdk import PostgresPersistor, configure_logger
from dsdk.dependency import namespace_directory

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
        super().__init__(
            username=kwargs.get(
                "username", env.get("POSTGRES_USERNAME", "postgres")
            ),
            password=kwargs.get(
                "password", env.get("POSTGRES_PASSWORD", "postgres")
            ),
            host=kwargs.get("host", env.get("POSTGRES_HOST", "postgres.test")),
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
                            "test.models",
                            "test.microservices",
                            "test.runs",
                            "test.predictions",
                        )
                    ),
                ).split(","),
            ),
        )


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
    persistor = Persistor(tables=("dne",))
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


def test_open_run():
    """Test open_run."""


def test_retry_connect():
    """Test retry_connect."""
    _ = Persistor(host="dne")


def test_store_evidence():
    """Test store evidence."""


def test_store_df():
    """Test store_df."""

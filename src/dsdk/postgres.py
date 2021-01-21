# -*- coding: utf-8 -*-
"""Postgres."""

from __future__ import annotations

from abc import ABC
from contextlib import contextmanager
from json import dumps
from logging import getLogger
from typing import TYPE_CHECKING, Any, Dict, Generator, Type, cast

from configargparse import ArgParser as ArgumentParser
from numpy import integer
from pandas import DataFrame, NaT, Series, isna

from .dependency import Interval, StubException
from .persistor import Persistor as BasePersistor
from .service import Delegate, Service, Task
from .utils import retry

logger = getLogger(__name__)


try:
    # Not everyone will be using postgres
    from psycopg2 import (
        DatabaseError,
        InterfaceError,
        OperationalError,
        connect,
    )
    from psycopg2.extras import (
        RealDictCursor,
        execute_batch,
    )
    from psycopg2.extensions import (
        register_adapter,
        ISQLQuote,
        Float,
        AsIs,
        Int,
    )

    def na_adapter(as_type):
        """Na adapter."""

        class _Adapter(ISQLQuote):  # pylint: disable=too-few-public-methods
            def getquoted(self):
                """Getquoted escaped against sql injection."""
                if isna(self._wrapped):
                    return b"NULL"
                return as_type(self._wrapped).getquoted()

        return _Adapter

    register_adapter(float, na_adapter(Float))
    register_adapter(integer, na_adapter(Int))
    register_adapter(type(NaT), na_adapter(AsIs))

    try:
        # pylint: disable=ungrouped-imports
        from pandas._libs.missing import (
            NAType,
        )

        # new version of pandas with a proper
        # NA type for int.
        register_adapter(NAType, na_adapter(AsIs))
    except ImportError:
        # old version of pandas that casts int
        # columns to float when np.nan is used.
        pass

except ImportError as import_error:
    logger.warning(import_error)

    DatabaseError = InterfaceError = OperationalError = StubException

    def connect(*args, **kwargs):
        """Connect stub."""
        raise NotImplementedError()


if TYPE_CHECKING:
    BaseMixin = Service
else:
    BaseMixin = ABC


class Messages:  # pylint: disable=too-few-public-methods
    """Messages."""

    KEY = "postgres"

    CLOSE = dumps({"key": f"{KEY}.close"})
    COMMIT = dumps({"key": f"{KEY}.commit"})
    END = dumps({"key": f"{KEY}.end"})
    ERROR = dumps({"key": f"{KEY}.table.error", "table": "%s"})
    ERRORS = dumps({"key": f"{KEY}.tables.error", "tables": "%s"})
    EXTANT = dumps({"key": f"{KEY}.sql.extant", "value": "%s"})
    ON = dumps({"key": f"{KEY}.on"})
    OPEN = dumps({"key": f"{KEY}.open"})
    ROLLBACK = dumps({"key": f"{KEY}.rollback"})
    DATA_TYPE_ERROR = dumps(
        {
            "index": "%s",
            "key": "f{KEY}.store_evidence",
            "message": "".join(
                (
                    "your dataframe evidence data types",
                    "may not match",
                    "your postgres schema data types,",
                    "most likely your ingest sql needs to",
                    "cast the upstream data to the correct type",
                )
            ),
            "value": "%s",
        }
    )


class Persistor(Messages, BasePersistor):
    """Persistor."""

    def check(self, cur, exceptions=(DatabaseError, InterfaceError)):
        """Check."""
        super().check(cur, exceptions)

    @contextmanager
    def connect(self) -> Generator[Any, None, None]:
        """Connect."""
        # Replace return type with ContextManager[Any] when mypy is fixed.
        # The `with ... as con:` formulation does not close the connection:
        # https://www.psycopg.org/docs/usage.html#with-statement
        con = self.retry_connect()
        logger.info(self.OPEN)
        try:
            yield con
        finally:
            con.close()
            logger.info(self.CLOSE)

    @contextmanager
    def cursor(self, con) -> Generator[Any, None, None]:
        """Yield a cursor that provides dicts."""
        # Replace return type with ContextManager[Any] when mypy is fixed.
        with con.cursor(cursor_factory=RealDictCursor) as cur:
            yield cur

    @contextmanager
    def open_run(self, parent: Any) -> Generator[Run, None, None]:
        """Open batch."""
        # Replace return type with ContextManager[Run] when mypy is fixed.
        sql = self.sql
        columns = parent.as_insert_sql()
        with self.commit() as cur:
            cur.execute(sql.schema)
            cur.execute(sql.runs.open, columns)
            for row in cur:
                run = Run(
                    row["id"],
                    row["microservice_id"],
                    row["model_id"],
                    parent,
                )
                parent.as_of = row["as_of"]
                duration = row["duration"]
                parent.duration = Interval(
                    on=duration.lower, end=duration.upper
                )
                parent.time_zone = row["time_zone"]
                break

        yield run

        with self.commit() as cur:
            cur.execute(sql.schema)
            predictions = run.predictions
            if predictions is not None:
                # pylint: disable=unsupported-assignment-operation
                predictions["run_id"] = run.id
                execute_batch(
                    cur,
                    sql.predictions.insert,
                    run.predictions.to_dict("records"),
                )
            cur.execute(sql.runs.close, {"id": run.id})
            for row in cur:
                duration = row["duration"]
                run.duration = Interval(on=duration.lower, end=duration.upper)
                break

    @retry((OperationalError,))
    def retry_connect(self):
        """Retry connect."""
        return connect(
            user=self.username,
            password=self.password,
            host=self.host,
            port=self.port,
            dbname=self.database,
        )

    def scores(self, run_id) -> Series:
        """Return scores series."""
        sql = self.sql
        with self.rollback() as cur:
            cur.execute(sql.schema)
            return self.df_from_query(
                cur,
                sql.predictions.gold,
                {"run_id": run_id},
            ).score.values  # pylint: disable=no-member

    def store_evidence(self, run: Any, *args, **kwargs) -> None:
        """Store evidence."""
        sql = self.sql
        schema = sql.schema
        run_id = run.id
        evidence = run.evidence
        exclude = set(kwargs.get("exclude", ()))
        while args:
            key, df, *args = args  # type: ignore
            evidence[key] = df
            # setattr(batch.evidence, name, data)
            if df.empty:
                continue
            try:
                insert = getattr(sql, key).insert
            except AttributeError as e:
                raise FileNotFoundError(
                    f"Missing sql/postgres/{key}/insert.sql"
                ) from e
            self._store_df(
                schema,
                insert,
                run_id,
                df[list(set(df.columns) - exclude)],
            )

    def _store_df(
        self,
        schema: str,
        insert: str,
        run_id: int,
        df: DataFrame,
    ):
        df["run_id"] = run_id
        out = df.to_dict("records")
        try:
            with self.commit() as cur:
                cur.execute(schema)
                execute_batch(
                    cur,
                    insert,
                    out,
                )
        except DatabaseError as e:
            enumeration = enumerate(out)
            while True:
                with self.rollback() as cur:
                    # enumeration is a generator
                    # it will pick up where it left off
                    for i, row in enumeration:
                        try:
                            cur.execute(insert, row)
                        except DatabaseError:
                            # assumes the client encoding is the default utf-8!
                            value = dumps(cur.mogrify(insert, row).decode())
                            logger.error(self.DATA_TYPE_ERROR, i, value)
                            break
                    else:
                        break
            raise e


class Mixin(BaseMixin):
    """Mixin."""

    def __init__(
        self,
        *,
        postgres=None,
        postgres_cls: Type = Persistor,
        **kwargs,
    ):
        """__init__."""
        self.postgres = cast(Persistor, postgres)
        self.postgres_cls = postgres_cls
        super().__init__(**kwargs)

    @contextmanager
    def inject_arguments(
        self, parser: ArgumentParser
    ) -> Generator[None, None, None]:
        """Inject arguments."""
        # Replace return type with ContextManager[None] when mypy is fixed.
        with self.postgres_cls.configure(self, parser):
            with super().inject_arguments(parser):
                yield

    def scores(self, run_id) -> Series:
        """Get scores."""
        return self.postgres.scores(run_id)


class Run(Delegate):
    """Run."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        id_: int,
        microservice_id: str,
        model_id: str,
        parent: Any,
    ):
        """__init__."""
        super().__init__(parent)
        self.id = id_
        self.microservice_id = microservice_id
        self.model_id = model_id

    def as_insert_doc(self) -> Dict[str, Any]:
        """As insert doc."""
        return {
            "run_id": self.id,
            "microservice_id": self.microservice_id,
            "model_id": self.model_id,
            **self.parent.as_insert_doc(),
        }


class PredictionMixin(Mixin):  # pylint: disable=too-few-public-methods.
    """Prediction Mixin."""

    @contextmanager
    def open_batch(self) -> Generator[Run, None, None]:
        """Open batch."""
        # Replace return type with ContextManager[Run] when mypy is fixed.
        with super().open_batch() as parent:
            with self.postgres.open_run(parent) as run:
                yield run


class CheckTablePrivileges(Task):  # pylint: disable=too-few-public-methods
    """CheckTablePrivileges."""

    def __call__(self, batch, service):
        """__call__."""
        postgres = service.postgres
        with postgres.rollback() as cur:
            postgres.check(cur)

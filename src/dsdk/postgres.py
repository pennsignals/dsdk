"""Postgres."""

from __future__ import annotations

from abc import ABC
from collections import deque
from contextlib import contextmanager
from json import dumps
from logging import getLogger
from typing import TYPE_CHECKING, Any, Generator

from numpy import integer
from pandas import DataFrame, NaT, Series, isna

from .interval import Interval
from .persistor import Persistor as BasePersistor
from .service import Delegate, Service
from .utils import StubError, retry

logger = getLogger(__name__)


try:
    # Not everyone will be using postgres
    from psycopg2 import (
        DatabaseError,
        InterfaceError,
        OperationalError,
        connect,
    )
    from psycopg2.extensions import (
        ISOLATION_LEVEL_AUTOCOMMIT,
        AsIs,
        Float,
        Int,
        ISQLQuote,
        register_adapter,
    )
    from psycopg2.extras import execute_batch

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
        from pandas._libs.missing import NAType

        # new version of pandas with a proper
        # NA type for int.
        register_adapter(NAType, na_adapter(AsIs))
    except ImportError:
        # old version of pandas that casts int
        # columns to float when np.nan is used.
        pass

except ImportError as import_error:
    logger.warning(import_error)

    DatabaseError = InterfaceError = OperationalError = StubError

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
    LISTEN = dumps({"key": f"{KEY}.listen", "value": "%s"})
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

    YAML = "!postgres"

    @classmethod
    def mogrify(
        cls,
        cur,
        query: str,
        parameters: Any,
    ) -> bytes:
        """Safely mogrify parameters into query or fragment."""
        return cur.mogrify(query, parameters)

    def __init__(
        self,
        *,
        port: int = 5432,
        schema: str = "public",
        **kwargs,
    ):
        """__init__."""
        super().__init__(port=port, schema=schema, **kwargs)

    def dry_run(
        self,
        parameters: dict[str, Any],
        exceptions=(DatabaseError, InterfaceError),
    ):
        """Dry run."""
        super().dry_run(parameters, exceptions)

    @contextmanager
    def commit(self) -> Generator[Any, None, None]:
        """Commit."""
        with super().commit() as cur:
            cur.execute(f"set search_path={self.schema};")
            yield cur

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
    def listen(self, *listens: str) -> Generator[Any, None, None]:
        """Listen."""
        con = self.retry_connect()
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        logger.info(self.OPEN)
        try:
            # replace list with a deque to allow
            # users to pop the last notify
            con.notifies = deque(con.notifies)
            with con.cursor() as cur:
                cur.execute(f"set search_path={self.schema};")
                for each in listens:
                    logger.debug(self.LISTEN, each)
                    cur.execute(each)
            yield con
        finally:
            con.close()
            logger.info(self.CLOSE)

    @contextmanager
    def open_run(self, parent: Any) -> Generator[Run, None, None]:
        """Open batch."""
        # Replace return type with ContextManager[Run] when mypy is fixed.
        sql = self.sql
        columns = parent.as_insert_sql()
        with self.commit() as cur:
            cur.execute(sql.runs.open, columns)
            for row in cur:
                (
                    id_,
                    microservice_id,
                    model_id,
                    duration,
                    as_of,
                    time_zone,
                    *_,
                ) = row
                run = Run(
                    id_,
                    microservice_id,
                    model_id,
                    parent,
                )
                parent.as_of = as_of
                parent.duration = Interval(
                    on=duration.lower, end=duration.upper
                )
                parent.time_zone = time_zone
                break

        yield run

        with self.commit() as cur:
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
                _, _, _, duration, _, _, *_ = row
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

    @contextmanager
    def rollback(self) -> Generator[Any, None, None]:
        """Rollback."""
        with super().rollback() as cur:
            cur.execute(f"set search_path={self.schema};")
            yield cur

    def scores(self, run_id) -> Series:
        """Return scores series."""
        sql = self.sql
        with self.rollback() as cur:
            return self.df_from_query(
                cur,
                sql.predictions.gold,
                parameters={"run_id": run_id},
            ).score.values  # pylint: disable=no-member

    def store_evidence(self, run: Any, *args, **kwargs) -> None:
        """Store evidence."""
        sql = self.sql
        run_id = run.id
        evidence = run.evidence
        exclude = set(kwargs.get("exclude", ()))
        while args:
            key, df, *args = args  # type: ignore[assignment]
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
                insert,
                run_id,
                df[list(set(df.columns) - exclude)],
            )

    def _store_df(
        self,
        insert: str,
        run_id: int,
        df: DataFrame,
    ):
        df["run_id"] = run_id
        out = df.to_dict("records")
        try:
            with self.commit() as cur:
                execute_batch(
                    cur,
                    insert,
                    out,
                )
        except DatabaseError as e:
            # figure out all rows which failed,
            #   rolling back any successful insertions
            # enumeration is a generator
            enumeration = enumerate(out)
            while True:
                with self.rollback() as cur:
                    for i, row in enumeration:
                        try:
                            cur.execute(insert, row)
                        except DatabaseError:
                            # assumes the client encoding is the default utf-8!
                            value = dumps(cur.mogrify(insert, row).decode())
                            logger.error(self.DATA_TYPE_ERROR, i, value)
                            break  # DatabaseError: break for loop
                    else:
                        # GeneratorExit: enumeration is exhausted
                        #   break while loop
                        break
                    # DatabaseError: rollback and continue while loop
                    #   enumeration will pick up where it left off
            raise e


class Mixin(BaseMixin):
    """Mixin."""

    @classmethod
    def yaml_types(cls) -> None:
        """Yaml types."""
        logger.debug("dsdk.postgres.Mixin.yaml_types()")
        Persistor.as_yaml_type()
        super().yaml_types()

    def __init__(self, *, postgres: Persistor, **kwargs):
        """__init__."""
        self.postgres = postgres
        super().__init__(**kwargs)

    def as_yaml(self) -> dict[str, Any]:
        """As yaml."""
        return {
            "postgres": self.postgres,
            **super().as_yaml(),
        }

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


class PredictionMixin(Mixin):  # pylint: disable=too-few-public-methods.
    """Prediction Mixin."""

    @contextmanager
    def open_batch(self) -> Generator[Run, None, None]:
        """Open batch."""
        # Replace return type with ContextManager[Run] when mypy is fixed.
        with super().open_batch() as parent:
            with self.postgres.open_run(parent) as run:
                yield run

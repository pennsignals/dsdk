# -*- coding: utf-8 -*-
"""Test mixin."""
# Goals:
#    - test mixin
#    - self-use with inject_args
#    - avoid additional method like set_config
#    - make parser optional
#    - demo dependency injection

from __future__ import annotations

from abc import ABC
from argparse import ArgumentParser
from contextlib import contextmanager
from json import dump as json_dump
from json import load as json_load
from pickle import dump as pickle_dump
from pickle import load as pickle_load
from sys import argv as sys_argv
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generator,
    Optional,
    Sequence,
    cast,
)


def dump_pickle_file(obj, path: str) -> None:
    """Dump pickle to file."""
    with open(path, "wb") as fout:
        pickle_dump(obj, fout)


def load_pickle_file(path: str) -> object:
    """Load pickle from file."""
    with open(path, "rb") as fin:
        return pickle_load(fin)


def dump_json_file(obj, path: str) -> None:
    """Dump json to file."""
    with open(path, "w", encoding="utf-8") as fout:
        json_dump(obj, fout)


def load_json_file(path: str) -> object:
    """Load json from file."""
    with open(path, "r", encoding="utf-8") as fin:
        return json_load(fin)


class Service:  # pylint: disable=too-few-public-methods
    """Service."""

    def __init__(
        self,
        *,
        argv: Optional[Sequence[str]] = None,
        cfg: Optional[Dict[str, Any]] = None,
        i: int = 0,
        parser: Optional[ArgumentParser] = None,
    ) -> None:
        """__init__."""
        self.service_i = i
        self.cfg = cast(Dict[str, Any], cfg)
        self.parser = parser

        # parsing arguments must be optional
        if parser:
            with self.inject_args(parser):
                if not argv:
                    argv = sys_argv[1:]
                parser.parse_args(argv)

        assert self.cfg is not None

    @contextmanager
    def inject_args(
        self, parser: ArgumentParser
    ) -> Generator[None, None, None]:
        """Inject args."""

        # In this example, the injected parameters here are simple:
        #    because no post parser.parse configuration is needed,
        #    but this is not necesarily the case.
        # Using only one configuration file is not desirable if
        #    if it is more difficult to validate.
        # Concider using separate files for tabular configuration.
        # This is particularly true for the mixins.

        def _inject_cfg(path: str) -> Dict[str, Any]:
            cfg = cast(Dict[str, Any], load_json_file(path))
            self.cfg = cfg
            return cfg

        parser.add_argument("--cfg", type=_inject_cfg)

        # before parser.parse call
        yield
        # after parser.parse call

        # load_json_file or a more complex constructor with multiple parameters
        #    could be here instead of in the single parameter parse callback
        #    _inject_cfg.


if TYPE_CHECKING:
    Mixin = Service
else:
    Mixin = ABC


class ModelMixin(Mixin):  # pylint: disable=too-few-public-methods
    """Model Mixin."""

    def __init__(
        self, *, i: int = 0, model: Optional[Dict[str, Any]] = None, **kwargs
    ) -> None:
        """__init__."""
        self.model_i = i
        self.model = cast(Dict[str, Any], model)
        super().__init__(i=i + 1, **kwargs)

        # self.model is not optional
        assert self.model is not None

    @contextmanager
    def inject_args(
        self, parser: ArgumentParser
    ) -> Generator[None, None, None]:
        """Inject args."""

        def _inject_model(path: str) -> Dict[str, Any]:
            model = cast(Dict[str, Any], load_pickle_file(path))
            self.model = model
            return model

        parser.add_argument("--model", type=_inject_model)
        # before parser.parse call
        with super().inject_args(parser):
            yield
        # after parser.parse call


class Postgres:  # pylint: disable=too-few-public-methods
    """Postgres."""

    def __init__(
        self, username: str, password: str, host: str, database: str,
    ) -> None:
        """__init__."""
        self.username = username
        self.password = password
        self.host = host
        self.database = database


class PostgresMixin(Mixin):  # pylint: disable=too-few-public-methods
    """Postgres Mixin."""

    def __init__(
        self, *, i: int = 0, postgres: Optional[Postgres] = None, **kwargs,
    ) -> None:
        """__init__."""
        self.postgres_i = i
        self.postgres = cast(Postgres, postgres)
        super().__init__(i=i + 1, **kwargs)

        # self.model is not optional
        assert self.postgres is not None

    @contextmanager
    def inject_args(
        self, parser: ArgumentParser
    ) -> Generator[None, None, None]:
        """Inject args."""
        kwargs: Dict[str, Any] = {}

        def _inject_str(key: str) -> Callable[[str], str]:
            def _inject(value: str) -> str:
                kwargs[key] = value
                return value

            return _inject

        parser.add_argument("--username", type=_inject_str("username"))
        parser.add_argument("--password", type=_inject_str("password"))
        parser.add_argument("--host", type=_inject_str("host"))
        parser.add_argument("--database", type=_inject_str("database"))

        # before parser.parse call
        with super().inject_args(parser):
            yield
        # after parser.parse call

        self.postgres = Postgres(**kwargs)


# Service must be last in inheritence
#    to ensure mixin methods are all called.
class App(
    PostgresMixin, ModelMixin, Service
):  # pylint: disable=too-few-public-methods
    """App."""

    def __init__(self, *, i: int = 0, **kwargs):
        """__init__."""
        self.app_i = i
        super().__init__(i=i + 1, **kwargs)

        # Assert correct order of initialization
        assert self.app_i == 0
        assert self.postgres_i == 1
        assert self.model_i == 2
        assert self.service_i == 3


def test_mixin_with_parser():
    """Test mixin with parser."""
    model_path = "./model.pkl"
    cfg_path = "./cfg.json"
    obj = {}
    dump_pickle_file(obj, model_path)
    dump_json_file(obj, cfg_path)

    argv = [
        "--cfg",
        cfg_path,
        "--model",
        model_path,
        "--username",
        "username",
        "--password",
        "password",
        "--host",
        "host",
        "--database",
        "database",
    ]

    parser = ArgumentParser()
    App(parser=parser, argv=argv)


def test_mixin_without_parser():
    """Test mixin without parser."""
    model: Dict[str, Any] = {}
    cfg: Dict[str, Any] = {}
    username = "username"
    password = "password"
    host = "host"
    database = "database"
    postgres = Postgres(
        username=username, password=password, host=host, database=database,
    )

    App(
        cfg=cfg, model=model, postgres=postgres,
    )


def test_mixins_are_abstract():
    """Test mixins are abstract."""
    # pylint: disable=abstract-class-instantiated
    username = "username"
    password = "password"
    host = "host"
    database = "database"

    try:
        PostgresMixin(
            username=username, password=password, host=host, database=database,
        )
        raise AssertionError("Ensure PostgresMixin is abstract.")
    except TypeError:
        pass

    model: Dict[str, Any] = {}
    try:
        ModelMixin(model=model)
        raise AssertionError("Ensure ModelMixin is abstract.")
    except TypeError:
        pass


def main():
    """Main."""
    test_mixin_with_parser()
    test_mixin_without_parser()
    test_mixins_are_abstract()


if __name__ == "__main__":
    main()

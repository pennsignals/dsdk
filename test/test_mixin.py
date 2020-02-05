# -*- coding: utf-8 -*-
"""Test mixin."""
# Goals:
#    - test mixin
#    - self-use with inject_args
#    - avoid additional method like set_config
#    - make parser optional
#    - demo dependency injection

from __future__ import annotations

from abc import ABC, abstractmethod
from argparse import ArgumentParser
from json import dump as json_dump
from json import load as json_load
from pickle import dump as pickle_dump
from pickle import load as pickle_load
from sys import argv as sys_argv
from typing import TYPE_CHECKING, Any, Dict, Optional, Sequence, cast


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
    with open(path, "w") as fout:
        json_dump(obj, fout)


def load_json_file(path: str) -> object:
    """Load json from file."""
    with open(path, "r") as fin:
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
        self.cfg = cfg
        self.parser = parser
        if parser:  # parsing arguments must be optional
            self.inject_args(parser)
            if not argv:
                argv = sys_argv[1:]
            parser.parse_args(argv)
        self.service_i = i
        assert self.cfg is not None

    def inject_args(self, parser: ArgumentParser) -> None:
        """Inject args."""

        def _inject_cfg(path: str) -> Dict[str, Any]:
            cfg = cast(Dict[str, Any], load_json_file(path))
            self.cfg = cfg
            return cfg

        parser.add_argument("--cfg", type=_inject_cfg)


if TYPE_CHECKING:
    Mixin = Service
else:
    Mixin = ABC


class ModelMixin(Mixin):  # pylint: disable=too-few-public-methods
    """Model Mixin."""

    @abstractmethod
    def __init__(
        self, *, i: int = 0, model: Optional[Dict[str, Any]] = None, **kwargs
    ) -> None:
        """__init__."""
        self.model = model
        super().__init__(i=i + 1, **kwargs)
        self.model_i = i
        assert self.model is not None

    def inject_args(self, parser: ArgumentParser) -> None:
        """Inject args."""
        super().inject_args(parser)

        def _inject_model(path: str) -> Dict[str, Any]:
            model = cast(Dict[str, Any], load_pickle_file(path))
            self.model = model
            return model

        parser.add_argument("--model", type=_inject_model)


class MongoMixin(Mixin):  # pylint: disable=too-few-public-methods
    """Mongo Mixin."""

    @abstractmethod
    def __init__(
        self, *, i: int = 0, mongo_uri: Optional[str] = None, **kwargs
    ):
        """__init__."""
        self.mongo_uri = mongo_uri
        super().__init__(i=i + 1, **kwargs)
        self.mongo_i = i
        assert self.mongo_uri is not None

    def inject_args(self, parser: ArgumentParser) -> None:
        """Inject args."""
        super().inject_args(parser)

        def _inject_mongo_uri(mongo_uri: str) -> str:
            self.mongo_uri = mongo_uri
            return mongo_uri

        parser.add_argument("--mongo-uri", type=_inject_mongo_uri)


# Service must be last in inheritence
class App(
    MongoMixin, ModelMixin, Service
):  # pylint: disable=too-few-public-methods
    """App."""

    def __init__(self, *, i: int = 0, **kwargs):
        """__init__."""
        super().__init__(i=i + 1, **kwargs)
        self.app_i = i
        assert self.app_i == 0
        assert self.mongo_i == 1
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
        "--mongo-uri",
        "mongodb://mongo/test",
        "--model",
        model_path,
    ]

    parser = ArgumentParser()
    App(parser=parser, argv=argv)


def test_mixin_without_parser():
    """Test mixin without parser."""
    model: Dict[str, Any] = {}
    cfg: Dict[str, Any] = {}
    mongo_uri = ""

    App(cfg=cfg, model=model, mongo_uri=mongo_uri)


def test_mixins_are_abstract():
    """Test mixins are abstract."""
    # pylint: disable=abstract-class-instantiated
    mongo_uri = ""

    try:
        MongoMixin(mongo_uri=mongo_uri)
        raise AssertionError("Ensure MongoMixin is abstract.")
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

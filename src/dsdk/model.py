# -*- coding: utf-8 -*-
"""Model support."""

from __future__ import annotations

from abc import ABC
from contextlib import contextmanager
from logging import getLogger
from typing import TYPE_CHECKING, Any, Dict, Generator, Type, cast

from configargparse import ArgParser as ArgumentParser

from .dependency import inject_str
from .service import Delegate, Service
from .utils import load_pickle_file

logger = getLogger(__name__)


if TYPE_CHECKING:
    BaseMixin = Service
else:
    BaseMixin = ABC


class Model:  # pylint: disable=too-few-public-methods
    """Model."""

    KEY = "model"

    @classmethod
    def load(cls, path: str) -> Model:
        """Load."""
        pkl = load_pickle_file(path)
        if pkl.__class__ is dict:
            assert pkl.__class__ is dict
            return cls(**pkl)  # type: ignore
        assert isinstance(pkl, Model)
        return pkl

    @classmethod
    @contextmanager
    def configure(
        cls, service: Service, parser
    ) -> Generator[None, None, None]:
        """Dependencies."""
        kwargs: Dict[str, Any] = {}

        parser.add(
            f"--{cls.KEY}",
            env_var=f"{cls.KEY.upper()}",
            help="Path to pickled model.",
            required=True,
            type=inject_str("path", kwargs),
        )
        yield

        service.dependency(cls.KEY, cls.load, kwargs)

    def __init__(self, name: str, version: str) -> None:
        """__init__."""
        self.name = name
        self.version = version


class Batch(Delegate):
    """Batch."""

    def __init__(self, model_version: str, parent: Any):
        """__init__."""
        super().__init__(parent)
        self.model_version = model_version

    def as_insert_doc(self) -> Dict[str, Any]:
        """As insert doc."""
        return {
            "model_version": self.model_version,
            **self.parent.as_insert_doc(),
        }

    def as_insert_sql(self) -> Dict[str, Any]:
        """As insert sql."""
        return {
            "model_version": self.model_version,
            **self.parent.as_insert_sql(),
        }


class Mixin(BaseMixin):
    """Mixin."""

    def __init__(self, *, model=None, model_cls: Type = Model, **kwargs):
        """__init__."""
        self.model = cast(Model, model)
        self.model_cls = model_cls
        super().__init__(**kwargs)

    @contextmanager
    def inject_arguments(
        self, parser: ArgumentParser
    ) -> Generator[None, None, None]:
        """Inject arguments."""
        with self.model_cls.configure(self, parser):
            with super().inject_arguments(parser):
                yield

    @contextmanager
    def open_batch(self) -> Generator[Any, None, None]:
        """Open batch."""
        with super().open_batch() as parent:
            batch = Batch(self.model.version, parent)
            yield batch

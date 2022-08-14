"""Model support."""

from __future__ import annotations

from abc import ABC
from contextlib import contextmanager
from json import dumps
from logging import getLogger
from typing import TYPE_CHECKING, Any, Generator

from cfgenvy import yaml_type

from .service import Delegate, Service
from .utils import load_pickle_file

logger = getLogger(__name__)


if TYPE_CHECKING:
    BaseMixin = Service
else:
    BaseMixin = ABC


class Model:
    """Model."""

    YAML = "!model"
    INIT = dumps(
        {
            "key": "model",
            "name": "%s",
            "path": "%s",
            "version": "%s",
        }
    )

    @classmethod
    def as_yaml_type(cls, tag: str | None = None) -> None:
        """As yaml type."""
        logger.debug("%s.as_yaml_type(tag=%s)", cls.__name__, tag)
        yaml_type(
            cls,
            tag or cls.YAML,
            init=cls._yaml_init,
            repr=cls._yaml_repr,
        )

    @classmethod
    def _yaml_init(cls, loader, node):
        """Yaml init."""
        path = loader.construct_scalar(node)
        d = load_pickle_file(path)
        assert d.__class__ is dict
        pkl = cls(path=path, **d)
        assert isinstance(pkl, Model)
        logger.info(cls.INIT, pkl.name, pkl.path, pkl.version)
        return pkl

    @classmethod
    def _yaml_repr(cls, dumper, self, *, tag: str):
        """Yaml_repr."""
        return dumper.represent_scalar(tag, self.as_yaml())

    def __init__(
        self,
        *,
        name: str,
        path: str,
        version: str,
    ) -> None:
        """__init__."""
        self.name = name
        self.path = path
        self.version = version

    def as_yaml(self) -> str:
        """As yaml."""
        return self.path


class Batch(Delegate):
    """Batch."""

    def __init__(self, model_version: str, parent: Any):
        """__init__."""
        super().__init__(parent)
        self.model_version = model_version

    def as_insert_sql(self) -> dict[str, Any]:
        """As insert sql."""
        return {
            "model_version": self.model_version,
            **self.parent.as_insert_sql(),
        }


class Mixin(BaseMixin):
    """Mixin."""

    def __init__(self, *, model: Model, **kwargs):
        """__init__."""
        self.model = model
        super().__init__(**kwargs)

    def as_yaml(self) -> dict[str, Any]:
        """As yaml."""
        return {
            "model": self.model,
            **super().as_yaml(),
        }

    @contextmanager
    def open_batch(self) -> Generator[Any, None, None]:
        """Open batch."""
        with super().open_batch() as parent:
            batch = Batch(self.model.version, parent)
            yield batch

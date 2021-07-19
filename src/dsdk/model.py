# -*- coding: utf-8 -*-
"""Model support."""

from __future__ import annotations

from abc import ABC
from contextlib import contextmanager
from logging import getLogger
from typing import TYPE_CHECKING, Any, Dict, Generator, Optional

from .service import Delegate, Service
from .utils import YamlDumper, YamlLoader, load_pickle_file

logger = getLogger(__name__)


if TYPE_CHECKING:
    BaseMixin = Service
else:
    BaseMixin = ABC


class Model:  # pylint: disable=too-few-public-methods
    """Model."""

    YAML = "!model"

    @classmethod
    def as_yaml_type(cls) -> None:
        """As yaml type."""
        YamlLoader.add_constructor(cls.YAML, cls._yaml_init)
        YamlDumper.add_representer(cls, cls._yaml_repr)

    @classmethod
    def _yaml_init(cls, loader, node):
        """Yaml init."""
        path = loader.construct_scalar(node)
        pkl = load_pickle_file(path)
        if pkl.__class__ is dict:
            pkl = cls(path=path, **pkl)
        else:
            pkl.path = path
        assert isinstance(pkl, Model)
        return pkl

    @classmethod
    def _yaml_repr(cls, dumper, self):
        """Yaml_repr."""
        return dumper.represent_scalar(cls.YAML, self.as_yaml())

    def __init__(
        self,
        *,
        name: str,
        version: str,
        path: Optional[str] = None,
    ) -> None:
        """__init__."""
        self.name = name
        self.path = path
        self.version = version

    def as_yaml(self) -> Dict[str, Any]:
        """As yaml."""
        return {"path": self.path}


class Batch(Delegate):
    """Batch."""

    def __init__(self, model_version: str, parent: Any):
        """__init__."""
        super().__init__(parent)
        self.model_version = model_version

    def as_insert_sql(self) -> Dict[str, Any]:
        """As insert sql."""
        return {
            "model_version": self.model_version,
            **self.parent.as_insert_sql(),
        }


class Mixin(BaseMixin):
    """Mixin."""

    @classmethod
    def yaml_types(cls) -> None:
        """As yaml types."""
        Model.as_yaml_type()
        super().yaml_types()

    def __init__(self, *, model: Model, **kwargs):
        """__init__."""
        self.model = model
        super().__init__(**kwargs)

    def as_yaml(self) -> Dict[str, Any]:
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

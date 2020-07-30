# -*- coding: utf-8 -*-
"""Model support."""

from __future__ import annotations

from abc import ABC
from contextlib import contextmanager
from logging import getLogger
from typing import TYPE_CHECKING, Any, Dict, Generator, cast

from configargparse import ArgParser as ArgumentParser

from .service import Service
from .utils import load_pickle_file

logger = getLogger(__name__)


if TYPE_CHECKING:
    BaseMixin = Service
else:
    BaseMixin = ABC


class Model:  # pylint: disable=too-few-public-methods
    """Model."""

    def __init__(self, name: str, version: str) -> None:
        """__init__."""
        self.name = name
        self.version = version

    def as_doc(self) -> Dict[str, Any]:
        """As doc."""
        return {"name": self.name, "on": self.version}


class Mixin(BaseMixin):
    """Mixin."""

    def __init__(self, *, model=None, **kwargs):
        """__init__."""
        self.model = cast(Model, model)
        super().__init__(**kwargs)

    @contextmanager
    def inject_arguments(
        self, parser: ArgumentParser
    ) -> Generator[None, None, None]:
        """Inject arguments."""

        model = cast(Model, None)

        def _inject_model(path: str) -> Model:
            nonlocal model
            model = cast(Model, load_pickle_file(path))
            return model

        parser.add(
            "--model",
            required=True,
            help="Path to pickled model",
            env_var="MODEL_PATH",
            type=_inject_model,
        )

        with super().inject_arguments(parser):
            yield

        if self.model is None:
            self.model = model

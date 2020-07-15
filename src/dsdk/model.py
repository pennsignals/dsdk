# -*- coding: utf-8 -*-
"""Model support."""

from __future__ import annotations

from abc import ABC
from logging import getLogger
from typing import TYPE_CHECKING, Optional, cast

from configargparse import ArgParser as ArgumentParser

from .service import Model, Service
from .utils import load_pickle_file

logger = getLogger(__name__)


if TYPE_CHECKING:
    BaseMixin = Service
else:
    BaseMixin = ABC


class Mixin(BaseMixin):
    """Mixin."""

    def __init__(self, *, model: Optional[Model] = None, **kwargs):
        """__init__."""
        # inferred type of self.model must not be optional...
        self.model = cast(Model, model)
        super().__init__(**kwargs)

        # ... because self.model is not optional
        assert self.model is not None

    def inject_arguments(self, parser: ArgumentParser) -> None:
        """Inject arguments."""
        super().inject_arguments(parser)

        def _inject_model(path: str) -> Model:
            model = cast(Model, load_pickle_file(path))
            self.model = model
            return model

        parser.add(
            "--model",
            required=True,
            help="Path to pickled model",
            env_var="MODEL_PATH",
            type=_inject_model,
        )

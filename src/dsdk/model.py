# -*- coding: utf-8 -*-
"""Model support."""

from __future__ import annotations

from logging import NullHandler, getLogger
from pickle import load
from typing import Dict

from configargparse import ArgParser as ArgumentParser
from configargparse import Namespace

from .service import Service

logger = getLogger(__name__)
logger.addHandler(NullHandler())


def get_model(model_path: str) -> Dict:
    """Get model from path."""
    with open(model_path, "rb") as fin:
        return load(fin)


class Mixin(Service):
    """Mixin."""

    @classmethod
    def add_arguments(cls, parser: ArgumentParser) -> None:
        """Add arguments."""
        super().add_arguments(parser)
        parser.add(
            "--model",
            required=True,
            help="Path to pickled sklearn model",
            env_var="MODEL_PATH",
        )

    def setup(self, args: Namespace) -> None:
        """Setup."""
        super().setup(args)
        self.model = model = get_model(args.model)
        self.info.update({"model": model["name"], "version": model["version"]})

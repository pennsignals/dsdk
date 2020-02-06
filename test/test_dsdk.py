# -*- coding: utf-8 -*-
"""Test dsdk."""

from configargparse import ArgParser as ArgumentParser
from pandas import DataFrame

from dsdk import (
    Batch,
    Model,
    ModelMixin,
    MongoEvidenceMixin,
    MssqlMixin,
    Service,
    Task,
    dump_json_file,
    dump_pickle_file,
    retry,
)


def test_batch_evidence():
    """Test batch evidence."""

    df = DataFrame()

    class _MockTask(Task):  # pylint: disable=too-few-public-methods
        def __call__(self, batch: Batch, service: Service) -> None:
            service.store_evidence(batch, "test", df)

    service = Service(pipeline=(_MockTask(name="test"),))
    batch = service()
    assert len(batch.evidence) == 1
    assert batch.evidence["test"] is df


def test_mixin_with_parser():
    """Test mixin with parser."""
    config_path = "./config.json"
    config = {}
    dump_json_file(config, config_path)

    model = Model(name="test", version="0.0.1")
    model_path = "./model.pkl"
    dump_pickle_file(model, model_path)

    mongo_uri = "mongodb://mongo?test"
    mssql_uri = "mssql+pymssql://mssql?test"

    class _Extract(Task):  # pylint: disable=too-few-public-methods
        def __call__(self, batch: Batch, service: Service) -> None:
            pass

    class _Transform(Task):  # pylint: disable=too-few-public-methods
        def __call__(self, batch: Batch, service: Service) -> None:
            pass

    class _Predict(Task):  # pylint: disable=too-few-public-methods
        def __call__(self, batch: Batch, service: Service) -> None:
            pass

    class _App(MongoEvidenceMixin, MssqlMixin, ModelMixin, Service):
        def __init__(self, **kwargs):
            pipeline = (_Extract, _Transform, _Predict)
            super().__init__(pipeline=pipeline, **kwargs)

    argv = [
        "--config",
        config_path,
        "--model",
        model_path,
        "--mssql-uri",
        mssql_uri,
        "--mongo-uri",
        mongo_uri,
    ]
    parser = ArgumentParser()
    _ = _App(parser=parser, argv=argv)


def test_mixin_without_parser():
    """Test mixin without parser."""
    model = Model(name="test", version="0.0.1")
    mongo_uri = "mongodb://mongo?test"
    mssql_uri = "mssql+pymssql://mssql?test"

    class _Extract(Task):  # pylint: disable=too-few-public-methods
        def __call__(self, batch: Batch, service: Service) -> None:
            pass

    class _Transform(Task):  # pylint: disable=too-few-public-methods
        def __call__(self, batch: Batch, service: Service) -> None:
            pass

    class _Predict(Task):  # pylint: disable=too-few-public-methods
        def __call__(self, batch: Batch, service: Service) -> None:
            pass

    class _App(MongoEvidenceMixin, MssqlMixin, ModelMixin, Service):
        def __init__(self, **kwargs):
            pipeline = (_Extract, _Transform, _Predict)
            super().__init__(pipeline=pipeline, **kwargs)

    _ = _App(model=model, mongo_uri=mongo_uri, mssql_uri=mssql_uri)


def test_retry_other_exception():
    """Test retry other exception."""

    exceptions_in = [
        RuntimeError("what?"),
        NotImplementedError("how?"),
        RuntimeError("no!"),
    ]
    actual = []
    expected = [1.0, 1.5, 2.25]

    def sleep(wait: float):
        actual.append(wait)

    @retry(
        (NotImplementedError, RuntimeError),
        retries=4,
        delay=1.0,
        backoff=1.5,
        sleep=sleep,
    )
    def explode():
        raise exceptions_in.pop()

    try:
        explode()
        raise AssertionError("IndexError expected")
    except IndexError:
        assert actual == expected


def test_retry_exhausted():
    """Test retry."""

    exceptions_in = [
        RuntimeError("what?"),
        NotImplementedError("how?"),
        RuntimeError("no!"),
        NotImplementedError("when?"),
    ]
    actual = []
    expected = [1.0, 1.5]

    def sleep(wait: float):
        actual.append(wait)

    @retry(
        (NotImplementedError, RuntimeError),
        retries=2,
        delay=1.0,
        backoff=1.5,
        sleep=sleep,
    )
    def explode():
        raise exceptions_in.pop()

    try:
        explode()
        raise AssertionError("NotImplementedError expected")
    except NotImplementedError as exception:
        assert actual == expected
        assert str(exception) == "when?"

"""Test model."""

from dsdk.utils import as_utc_non_naive_datetime


def test_open_batch(stub_model_service):
    """Test open batch."""
    with stub_model_service.open_batch() as batch:
        actual = batch.as_insert_sql()
        del actual["microservice_version"]
    as_of = as_utc_non_naive_datetime("2019-09-18 17:19:23.873398+00")
    time_zone = "America/New_York"
    model_version = "0.0.1"
    expected = {
        "as_of": as_of,
        "model_version": model_version,
        "time_zone": time_zone,
    }
    assert actual == expected

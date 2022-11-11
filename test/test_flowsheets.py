"""Test flowsheets."""

from inspect import unwrap

from pandas import DataFrame
from vcr import VCR

vcr = VCR(
    filter_headers=(
        ("authorization", "EPIC_AUTHORIZATION"),
        ("epic-client-id", "EPIC_CLIENT_ID"),
        ("cookie", "EPIC_COOKIE"),
    ),
)


@vcr.use_cassette("./test/flowsheets.valid.yaml")
def test_valid(stub_flowsheets_service):
    """Test valid flowsheet."""
    service = stub_flowsheets_service

    postgres = service.postgres
    postgres.return_value = DataFrame(
        [
            # inpatient admission date is 2019-02-06 at PAH
            {
                "as_of": service.as_of,
                "csn": 218202909,
                "empi": "8330651951",
                "id": 0,
                "kind": "score",
                "run_id": 0,
                "score": 0.5,
            }
        ]
    )
    expected = ""
    for result in service.publish():
        assert result.status is True
        assert result.status_code == 200
        actual = str(result)
        assert expected == actual
        break
    else:
        raise AssertionError("Atleast one result expected")


@vcr.use_cassette("./test/flowsheets.invalid.csn.yaml")
def test_invalid_csn(stub_flowsheets_service):
    """Test invalid csn."""
    service = stub_flowsheets_service

    postgres = service.postgres
    postgres.return_value = DataFrame(
        [
            {
                "as_of": service.as_of,
                "csn": 999999999,
                "empi": "8330651951",
                "id": 0,
                "kind": "score",
                "run_id": 0,
                "score": 0.5,
            }
        ]
    )
    expected = (
        "An error occurred while executing the command: "
        "EPT_DAT_RETRIEVAL_ERROR."
    )

    for result in service.publish():
        assert result.description == expected
        assert result.status is False
        assert result.status_code == 400
        assert result.name == "HTTPError"


@vcr.use_cassette("./test/flowsheets.invalid.empi.yaml")
def test_invalid_empi(stub_flowsheets_service):
    """Test invalid empi."""
    service = stub_flowsheets_service

    postgres = service.postgres
    postgres.return_value = DataFrame(
        [
            {
                "as_of": service.as_of,
                "csn": 218202909,
                "empi": "9999999999",
                "id": 0,
                "kind": "score",
                "run_id": 0,
                "score": 0.5,
            }
        ]
    )
    expected = (
        "An error occurred while executing the command: "
        "EPT_ID_RETRIEVAL_ERROR details: "
        "2:InvalidRecord:RecordNotFound:9999999999;UID."
    )

    for result in service.publish():
        assert result.description == expected
        assert result.status is False
        assert result.status_code == 400
        assert result.name == "HTTPError"


@vcr.use_cassette("./test/flowsheets.data.not.saved.yaml")
def test_data_not_saved(stub_flowsheets_service):
    """Test data not saved."""
    service = stub_flowsheets_service
    flowsheets = service.flowsheets

    inner = unwrap(flowsheets.on_rest)

    def outer(*args, **kwargs):
        return inner(flowsheets, *args, **kwargs)

    service.flowsheets.on_rest = outer

    postgres = service.postgres
    postgres.return_value = DataFrame(
        [
            {
                "as_of": service.as_of,
                "csn": 133713371,
                "empi": "1337133713",
                "id": 0,
                "kind": "score",
                "run_id": 0,
                "score": 0.5,
            }
        ]
    )

    for result in service.publish():
        assert result.description == "DATA_NOT_SAVED"
        assert result.status is False
        assert result.status_code == 400
        assert result.name == "SaveError"

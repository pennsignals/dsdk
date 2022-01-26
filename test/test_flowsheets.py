# -*- coding: utf-8 -*-
"""Test flowsheets."""

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
def test_valid(mock_flowsheets_service):
    """Test valid flowsheet."""
    service = mock_flowsheets_service

    postgres = service.postgres
    postgres.df_from_query.return_value = DataFrame(
        [
            # inpatient admission date is 2019-02-06 at PAH
            {
                "as_of": service.as_of,
                "csn": 218202909,
                "empi": "8330651951",
                "id": 0,
                "run_id": 0,
                "score": 0.5,
            }
        ]
    )
    for result in service.publish():
        assert result.status is True
        assert result.status_code == 200


@vcr.use_cassette("./test/flowsheets.invalid.csn.yaml")
def test_invalid_csn(mock_flowsheets_service):
    """Test invalid csn."""
    service = mock_flowsheets_service

    postgres = service.postgres
    postgres.df_from_query.return_value = DataFrame(
        [
            {
                "as_of": service.as_of,
                "csn": 999999999,
                "empi": "8330651951",
                "id": 0,
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
def test_invalid_empi(mock_flowsheets_service):
    """Test invalid empi."""
    service = mock_flowsheets_service

    postgres = service.postgres
    postgres.df_from_query.return_value = DataFrame(
        [
            {
                "as_of": service.as_of,
                "csn": 218202909,
                "empi": "9999999999",
                "id": 0,
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

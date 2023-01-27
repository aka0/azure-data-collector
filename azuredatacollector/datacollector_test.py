"""Tests"""
from datetime import datetime

import pytest
from datacollector import DataCollectorClient


@pytest.fixture
def test_client():
    """Test Client"""
    return DataCollectorClient("customer_id", "c2hhcmVkX2tleQ==")


def test_build_authorization_header(test_client) -> None:

    headers = test_client._DataCollectorClient__build_authorization_headers(
        content_length=1, log_type="TestTable", x_ms_date=datetime(2000, 1, 1, 1, 1, 1)
    )

    assert headers.get("content-type") == "application/json"
    assert headers.get("Log-Type") == "TestTable"
    assert (
        headers.get("Authorization")
        == "SharedKey customer_id:p9rltWgAbXzQtX8iV4P1E95fvA3n7imvysp16fPUXKE="
    )
    assert headers.get("x-ms-date") == "Sat, 01 Jan 2000 01:01:01 GMT"


@pytest.mark.parametrize(
    "test_data, test_max_bytes, expected",
    [
        (
            [{"col": "data"}, {"col": "data"}, {"col": "data"}],
            33,
            [[{"col": "data"}], [{"col": "data"}], [{"col": "data"}]],
        ),
        ([{"col": "data"}], 0, [[{"col": "data"}]]),
        ([{"col": "data"}], 3000, [[{"col": "data"}]]),
    ],
)
def test_batch(test_client, test_data, test_max_bytes, expected) -> None:
    assert (
        test_client._DataCollectorClient__batch(
            data=test_data, max_bytes_per_request=test_max_bytes
        )
        == expected
    )

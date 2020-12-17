import pytest  # type: ignore
from datetime import date, datetime, timezone, timedelta

from snuba_sdk.expressions import _stringify_scalar, ScalarType

tests = [
    pytest.param(None, "NULL"),
    pytest.param(True, "TRUE"),
    pytest.param(False, "FALSE"),
    pytest.param(1, "1"),
    pytest.param(1000000000000000000, "1000000000000000000"),
    pytest.param(-1000000000000000000, "-1000000000000000000"),
    pytest.param(1.0, "1.0"),
    pytest.param(3.14159, "3.14159"),
    pytest.param(0.0, "0.0"),
    pytest.param(date(2020, 12, 25), "toDateTime('2020-12-25')"),
    pytest.param(
        datetime(2020, 12, 25, 1, 12, 35, 81321),
        "toDateTime('2020-12-25T01:12:35.081321')",
    ),
    pytest.param(
        datetime(2020, 12, 25, 1, 12, 35, 81321, timezone.utc),
        "toDateTime('2020-12-25T01:12:35.081321')",
    ),
    pytest.param(
        datetime(2020, 12, 25, 1, 12, 35, 81321, timezone(timedelta(hours=5))),
        "toDateTime('2020-12-24T20:12:35.081321')",
    ),
    pytest.param("abc", "'abc'"),
    pytest.param(b"abc", "'abc'"),
    pytest.param("a'b''c'", "'a\\'b\\'\\'c\\''"),
    pytest.param("a\\''b''c'", "'a\\'\\'b\\'\\'c\\''"),
    pytest.param("a\nb\nc", "'a\\nb\\nc'"),
]


@pytest.mark.parametrize("scalar, expected", tests)
def test_scalars(scalar: ScalarType, expected: str) -> None:
    assert _stringify_scalar(scalar) == expected, scalar

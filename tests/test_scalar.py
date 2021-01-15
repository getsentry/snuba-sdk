import pytest
import re
from datetime import date, datetime, timezone, timedelta

from snuba_sdk.expressions import InvalidArray, ScalarType
from snuba_sdk.visitors import _stringify_scalar

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
    pytest.param([1, 2, 3], "array(1, 2, 3)"),
    pytest.param(
        [[1, 2, None], [None, 5, 6]], "array(array(1, 2, NULL), array(NULL, 5, 6))"
    ),
    pytest.param(("a", "b", "c"), "tuple('a', 'b', 'c')"),
    pytest.param(
        (("a", 1, True), (None, "b", None)),
        "tuple(tuple('a', 1, TRUE), tuple(NULL, 'b', NULL))",
    ),
]


@pytest.mark.parametrize("scalar, expected", tests)
def test_scalars(scalar: ScalarType, expected: str) -> None:
    assert _stringify_scalar(scalar) == expected, scalar


def test_invalid_scalars() -> None:
    with pytest.raises(
        InvalidArray,
        match=re.escape(
            "invalid array ['a', 1]: arrays must have the same data type or None, perhaps use a tuple instead"
        ),
    ):
        _stringify_scalar(["a", 1])

    with pytest.raises(
        InvalidArray,
        match=re.escape(
            "invalid array ['a', 1, 2...]: arrays must have the same data type or None, perhaps use a tuple instead"
        ),
    ):
        _stringify_scalar(["a", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13])

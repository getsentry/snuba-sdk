import pytest
from typing import Any, Optional

from snuba_sdk.expressions import Granularity, InvalidExpression, Limit, Offset

limit_tests = [
    pytest.param(0, None),
    pytest.param(10, None),
    pytest.param(-1, InvalidExpression("limit '-1' must be at least 0")),
    pytest.param("5", InvalidExpression("limit '5' must be an integer")),
    pytest.param(1.5, InvalidExpression("limit '1.5' must be an integer")),
    pytest.param(10.0, InvalidExpression("limit '10.0' must be an integer")),
    pytest.param(1000000, InvalidExpression("limit '1000000' is capped at 10,000")),
]


@pytest.mark.parametrize("value, exception", limit_tests)
def test_limit(value: Any, exception: Optional[Exception]) -> None:
    if exception is not None:
        with pytest.raises(type(exception), match=str(exception)):
            Limit(value)
    else:
        assert Limit(value).limit == value


offset_tests = [
    pytest.param(0, None),
    pytest.param(10, None),
    pytest.param(-1, InvalidExpression("offset '-1' must be at least 0")),
    pytest.param("5", InvalidExpression("offset '5' must be an integer")),
    pytest.param(1.5, InvalidExpression("offset '1.5' must be an integer")),
    pytest.param(10.0, InvalidExpression("offset '10.0' must be an integer")),
    pytest.param(1000000, InvalidExpression("offset '1000000' is capped at 10,000")),
]


@pytest.mark.parametrize("value, exception", offset_tests)
def test_offset(value: Any, exception: Optional[Exception]) -> None:
    if exception is not None:
        with pytest.raises(type(exception), match=str(exception)):
            Offset(value)
    else:
        assert Offset(value).offset == value


granularity_tests = [
    pytest.param(10, None),
    pytest.param(0, InvalidExpression("granularity '0' must be at least 1")),
    pytest.param("5", InvalidExpression("granularity '5' must be an integer")),
    pytest.param(1.5, InvalidExpression("granularity '1.5' must be an integer")),
    pytest.param(10.0, InvalidExpression("granularity '10.0' must be an integer")),
]


@pytest.mark.parametrize("value, exception", granularity_tests)
def test_granularity(value: Any, exception: Optional[Exception]) -> None:
    if exception is not None:
        with pytest.raises(type(exception), match=str(exception)):
            Granularity(value)
    else:
        assert Granularity(value).granularity == value

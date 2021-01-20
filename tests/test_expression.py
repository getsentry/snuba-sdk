import pytest
import re
from typing import Any, Optional

from snuba_sdk.expressions import (
    Column,
    Direction,
    Function,
    Granularity,
    InvalidExpression,
    Limit,
    LimitBy,
    Offset,
    OrderBy,
    Totals,
)

limit_tests = [
    pytest.param(1, None),
    pytest.param(10, None),
    pytest.param(-1, InvalidExpression("limit '-1' must be at least 1")),
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


orderby_tests = [
    pytest.param(Column("foo"), Direction.ASC, None),
    pytest.param(Function("bar", [Column("foo")]), Direction.ASC, None),
    pytest.param(
        0,
        Direction.DESC,
        InvalidExpression("OrderBy expression must be a Column or Function"),
    ),
    pytest.param(
        Column("foo"), "ASC", InvalidExpression("OrderBy direction must be a Direction")
    ),
]


@pytest.mark.parametrize("exp, direction, exception", orderby_tests)
def test_orderby(exp: Any, direction: Any, exception: Optional[Exception]) -> None:
    if exception is not None:
        with pytest.raises(type(exception), match=str(exception)):
            OrderBy(exp, direction)
    else:
        assert OrderBy(exp, direction)


limitby_tests = [
    pytest.param(Column("foo"), 1, None),
    pytest.param("bar", 1, InvalidExpression("LimitBy can only be used on a Column")),
    pytest.param(
        Column("foo"),
        "1",
        InvalidExpression("LimitBy count must be a positive integer (max 10,000)"),
    ),
    pytest.param(
        Column("foo"),
        -1,
        InvalidExpression("LimitBy count must be a positive integer (max 10,000)"),
    ),
    pytest.param(
        Column("foo"),
        15000,
        InvalidExpression("LimitBy count must be a positive integer (max 10,000)"),
    ),
]


@pytest.mark.parametrize("column, count, exception", limitby_tests)
def test_limitby(column: Any, count: Any, exception: Optional[Exception]) -> None:
    if exception is not None:
        with pytest.raises(type(exception), match=re.escape(str(exception))):
            LimitBy(column, count)
    else:
        assert LimitBy(column, count).count == count


totals_tests = [
    pytest.param(True, None),
    pytest.param(False, None),
    pytest.param(0, InvalidExpression("totals must be a boolean")),
]


@pytest.mark.parametrize("value, exception", totals_tests)
def test_totals(value: Any, exception: Optional[Exception]) -> None:
    if exception is not None:
        with pytest.raises(type(exception), match=str(exception)):
            Totals(value)
    else:
        assert Totals(value).totals == value

import re
from typing import Any, Optional

import pytest

from snuba_sdk.column import Column
from snuba_sdk.expressions import (
    Granularity,
    InvalidExpressionError,
    Limit,
    Offset,
    Totals,
)
from snuba_sdk.function import Function
from snuba_sdk.orderby import Direction, LimitBy, OrderBy

limit_tests = [
    pytest.param(1, None),
    pytest.param(10, None),
    pytest.param(-1, InvalidExpressionError("limit '-1' must be at least 1")),
    pytest.param("5", InvalidExpressionError("limit '5' must be an integer")),
    pytest.param(1.5, InvalidExpressionError("limit '1.5' must be an integer")),
    pytest.param(10.0, InvalidExpressionError("limit '10.0' must be an integer")),
    pytest.param(
        1000000, InvalidExpressionError("limit '1000000' is capped at 10,000")
    ),
]


@pytest.mark.parametrize("value, exception", limit_tests)
def test_limit(value: Any, exception: Optional[Exception]) -> None:
    if exception is not None:
        with pytest.raises(type(exception), match=re.escape(str(exception))):
            Limit(value)
    else:
        assert Limit(value).limit == value


offset_tests = [
    pytest.param(0, None),
    pytest.param(10, None),
    pytest.param(-1, InvalidExpressionError("offset '-1' must be at least 0")),
    pytest.param("5", InvalidExpressionError("offset '5' must be an integer")),
    pytest.param(1.5, InvalidExpressionError("offset '1.5' must be an integer")),
    pytest.param(10.0, InvalidExpressionError("offset '10.0' must be an integer")),
]


@pytest.mark.parametrize("value, exception", offset_tests)
def test_offset(value: Any, exception: Optional[Exception]) -> None:
    if exception is not None:
        with pytest.raises(type(exception), match=re.escape(str(exception))):
            Offset(value)
    else:
        assert Offset(value).offset == value


granularity_tests = [
    pytest.param(10, None),
    pytest.param(0, InvalidExpressionError("granularity '0' must be at least 1")),
    pytest.param("5", InvalidExpressionError("granularity '5' must be an integer")),
    pytest.param(1.5, InvalidExpressionError("granularity '1.5' must be an integer")),
    pytest.param(10.0, InvalidExpressionError("granularity '10.0' must be an integer")),
]


@pytest.mark.parametrize("value, exception", granularity_tests)
def test_granularity(value: Any, exception: Optional[Exception]) -> None:
    if exception is not None:
        with pytest.raises(type(exception), match=re.escape(str(exception))):
            Granularity(value)
    else:
        assert Granularity(value).granularity == value


totals_tests = [
    pytest.param(True, None),
    pytest.param(0, InvalidExpressionError("totals must be a boolean")),
]


@pytest.mark.parametrize("value, exception", totals_tests)
def test_totals(value: Any, exception: Optional[Exception]) -> None:
    if exception is not None:
        with pytest.raises(type(exception), match=re.escape(str(exception))):
            Totals(value)
    else:
        assert Totals(value).totals == value


orderby_tests = [
    pytest.param(Column("foo"), Direction.ASC, None),
    pytest.param(Function("bar", [Column("foo")]), Direction.ASC, None),
    pytest.param(
        0,
        Direction.DESC,
        InvalidExpressionError(
            "OrderBy expression must be a Column, CurriedFunction or Function"
        ),
    ),
    pytest.param(
        Column("foo"),
        "ASC",
        InvalidExpressionError("OrderBy direction must be a Direction"),
    ),
]


@pytest.mark.parametrize("exp, direction, exception", orderby_tests)
def test_orderby(exp: Any, direction: Any, exception: Optional[Exception]) -> None:
    if exception is not None:
        with pytest.raises(type(exception), match=re.escape(str(exception))):
            OrderBy(exp, direction)
    else:
        assert OrderBy(exp, direction)


limitby_tests = [
    pytest.param([Column("foo")], 1, None),
    pytest.param(
        ["bar"],
        1,
        InvalidExpressionError(
            "LimitBy columns: Invalid element 'bar' which must be a list composed entirely of <class 'snuba_sdk.column.Column'>"
        ),
    ),
    pytest.param(
        [Column("foo")],
        "1",
        InvalidExpressionError("LimitBy count must be a positive integer (max 10,000)"),
    ),
    pytest.param(
        [Column("foo")],
        -1,
        InvalidExpressionError("LimitBy count must be a positive integer (max 10,000)"),
    ),
    pytest.param(
        [Column("foo")],
        15000,
        InvalidExpressionError("LimitBy count must be a positive integer (max 10,000)"),
    ),
]


@pytest.mark.parametrize("column, count, exception", limitby_tests)
def test_limitby(column: Any, count: Any, exception: Optional[Exception]) -> None:
    if exception is not None:
        with pytest.raises(type(exception), match=re.escape(str(exception))):
            LimitBy(column, count)
    else:
        assert LimitBy(column, count).count == count

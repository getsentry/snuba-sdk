import re
from typing import Any, Optional

import pytest

from snuba_sdk.column import Column
from snuba_sdk.expressions import (
    Consistent,
    Debug,
    DryRun,
    Granularity,
    InvalidExpression,
    Legacy,
    Limit,
    Offset,
    Totals,
    Turbo,
)
from snuba_sdk.function import Function
from snuba_sdk.orderby import Direction, LimitBy, OrderBy

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
        with pytest.raises(type(exception), match=re.escape(str(exception))):
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
        with pytest.raises(type(exception), match=re.escape(str(exception))):
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
        with pytest.raises(type(exception), match=re.escape(str(exception))):
            Granularity(value)
    else:
        assert Granularity(value).granularity == value


orderby_tests = [
    pytest.param(Column("foo"), Direction.ASC, None),
    pytest.param(Function("bar", [Column("foo")]), Direction.ASC, None),
    pytest.param(
        0,
        Direction.DESC,
        InvalidExpression(
            "OrderBy expression must be a Column, CurriedFunction or Function"
        ),
    ),
    pytest.param(
        Column("foo"), "ASC", InvalidExpression("OrderBy direction must be a Direction")
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


boolean_tests = [
    pytest.param("totals", Totals),
    pytest.param("consistent", Consistent),
    pytest.param("turbo", Turbo),
    pytest.param("debug", Debug),
    pytest.param("dry_run", DryRun),
    pytest.param("legacy", Legacy),
]


@pytest.mark.parametrize("name, flag", boolean_tests)
def test_boolean_flags(name: str, flag: Any) -> None:
    assert flag(True) is not None
    assert flag(False) is not None
    with pytest.raises(InvalidExpression, match=re.escape(f"{name} must be a boolean")):
        flag(0)

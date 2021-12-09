import re
from typing import Any, Optional

import pytest

from snuba_sdk.column import Column
from snuba_sdk.expressions import (
    Consistent,
    Debug,
    DryRun,
    Granularity,
    InvalidExpressionError,
    Legacy,
    Limit,
    Offset,
    ParentAPI,
    Totals,
    Turbo,
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
    with pytest.raises(
        InvalidExpressionError, match=re.escape(f"{name} must be a boolean")
    ):
        flag(0)


def test_parent_api() -> None:
    assert ParentAPI("something") is not None
    with pytest.raises(
        InvalidExpressionError, match=re.escape("0 must be non-empty string")
    ):
        ParentAPI(0)  # type: ignore

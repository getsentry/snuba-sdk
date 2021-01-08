import pytest
from typing import Any, Callable, Optional

from snuba_sdk.expressions import (
    Function,
    Column,
    Condition,
    InvalidExpression,
    Op,
)
from tests import cond
from snuba_sdk.visitors import Translation

tests = [
    pytest.param(
        cond(Column("event_id"), Op.EQ, "foo"),
        Condition(Column("event_id"), Op.EQ, "foo"),
        "event_id = 'foo'",
        None,
        id="basic condition test",
    ),
    pytest.param(
        cond(Function("toString", [Column("event_id")]), Op.NEQ, "foo"),
        Condition(Function("toString", [Column("event_id")]), Op.NEQ, "foo"),
        "toString(event_id) != 'foo'",
        None,
        id="lhs function",
    ),
    pytest.param(
        cond(Column("event_id"), Op.GT, Column("group_id")),
        Condition(Column("event_id"), Op.GT, Column("group_id")),
        "event_id > group_id",
        None,
        id="rhs column",
    ),
    pytest.param(
        cond(
            Column("event_id"), Op.NOT_LIKE, Function("toString", [Column("group_id")])
        ),
        Condition(
            Column("event_id"), Op.NOT_LIKE, Function("toString", [Column("group_id")])
        ),
        "event_id NOT LIKE toString(group_id)",
        None,
        id="rhs function",
    ),
    pytest.param(
        cond("foo", Op.EQ, "foo"),
        None,
        "",
        InvalidExpression(
            "invalid condition: LHS of a condition must be a Column or Function, not <class 'str'>"
        ),
        id="lhs invalid type",
    ),
    pytest.param(
        cond(Column("foo"), Column("bar"), "foo"),
        None,
        "",
        InvalidExpression("invalid condition: operator of a condition must be an Op"),
        id="op invalid type",
    ),
    pytest.param(
        cond(Column("foo"), Op.EQ, Op.EQ),
        None,
        "",
        InvalidExpression(
            "invalid condition: RHS of a condition must be a Column, Function or Scalar not <enum 'Op'>"
        ),
        id="rhs invalid type",
    ),
]


TRANSLATOR = Translation()


@pytest.mark.parametrize("cond_wrapper, valid, translated, exception", tests)
def test_functions(
    cond_wrapper: Callable[[], Any],
    valid: Function,
    translated: str,
    exception: Optional[Exception],
) -> None:
    def verify() -> None:
        exp = cond_wrapper()
        assert exp == valid
        assert exp.accept(TRANSLATOR) == translated

    if exception is not None:
        with pytest.raises(type(exception), match=str(exception)):
            verify()
    else:
        verify()

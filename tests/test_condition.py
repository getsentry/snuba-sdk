import pytest
import re
from typing import Any, Callable, Optional

from snuba_sdk.conditions import BooleanCondition, BooleanOp, Condition, Op
from snuba_sdk.expressions import (
    Function,
    Column,
    InvalidExpression,
)
from snuba_sdk.visitors import Translation
from tests import bool_cond, cond

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
            "invalid condition: LHS of a condition must be a Column, CurriedFunction or Function, not <class 'str'>"
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
            "invalid condition: RHS of a condition must be a Column, CurriedFunction, Function or Scalar not <enum 'Op'>"
        ),
        id="rhs invalid type",
    ),
]


TRANSLATOR = Translation()


@pytest.mark.parametrize("cond_wrapper, valid, translated, exception", tests)
def test_conditions(
    cond_wrapper: Callable[[], Any],
    valid: Optional[Condition],
    translated: Optional[str],
    exception: Optional[Exception],
) -> None:
    def verify() -> None:
        exp = cond_wrapper()
        assert exp == valid
        assert TRANSLATOR.visit(exp) == translated

    if exception is not None:
        with pytest.raises(type(exception), match=re.escape(str(exception))):
            verify()
    else:
        verify()


boolean_tests = [
    pytest.param(
        bool_cond(
            BooleanOp.OR,
            [Condition(Column("a"), Op.EQ, 1), Condition(Column("a"), Op.EQ, 2)],
        ),
        BooleanCondition(
            BooleanOp.OR,
            [Condition(Column("a"), Op.EQ, 1), Condition(Column("a"), Op.EQ, 2)],
        ),
        "(a = 1 OR a = 2)",
        None,
        id="basic boolean test",
    ),
    pytest.param(
        bool_cond(
            BooleanOp.AND,
            [
                Condition(Column("a"), Op.EQ, 1),
                Condition(Column("b"), Op.EQ, 2),
                Condition(Column("c"), Op.EQ, 3),
            ],
        ),
        BooleanCondition(
            BooleanOp.AND,
            [
                Condition(Column("a"), Op.EQ, 1),
                Condition(Column("b"), Op.EQ, 2),
                Condition(Column("c"), Op.EQ, 3),
            ],
        ),
        "(a = 1 AND b = 2 AND c = 3)",
        None,
        id="more than two boolean test",
    ),
    pytest.param(
        bool_cond(
            BooleanOp.AND,
            [
                Condition(Column("a"), Op.EQ, 1),
                BooleanCondition(
                    BooleanOp.OR,
                    [
                        Condition(Column("b"), Op.EQ, 2),
                        Condition(Column("c"), Op.EQ, 3),
                    ],
                ),
                BooleanCondition(
                    BooleanOp.AND,
                    [
                        Condition(Column("d"), Op.EQ, 4),
                        Condition(Column("e"), Op.EQ, 5),
                    ],
                ),
            ],
        ),
        BooleanCondition(
            BooleanOp.AND,
            [
                Condition(Column("a"), Op.EQ, 1),
                BooleanCondition(
                    BooleanOp.OR,
                    [
                        Condition(Column("b"), Op.EQ, 2),
                        Condition(Column("c"), Op.EQ, 3),
                    ],
                ),
                BooleanCondition(
                    BooleanOp.AND,
                    [
                        Condition(Column("d"), Op.EQ, 4),
                        Condition(Column("e"), Op.EQ, 5),
                    ],
                ),
            ],
        ),
        "(a = 1 AND (b = 2 OR c = 3) AND (d = 4 AND e = 5))",
        None,
        id="nested boolean test",
    ),
    pytest.param(
        bool_cond(
            "or",
            [Condition(Column("a"), Op.EQ, 1), Condition(Column("a"), Op.EQ, 2)],
        ),
        None,
        None,
        InvalidExpression("invalid boolean: operator of a boolean must be a BooleanOp"),
        id="invalid op",
    ),
    pytest.param(
        bool_cond(BooleanOp.OR, Condition(Column("a"), Op.EQ, 1)),
        None,
        None,
        InvalidExpression(
            "invalid boolean: conditions must be a list of other conditions"
        ),
        id="not a list",
    ),
    pytest.param(
        bool_cond(BooleanOp.OR, [Condition(Column("a"), Op.EQ, 1)]),
        None,
        None,
        InvalidExpression("invalid boolean: must supply at least two conditions"),
        id="only one",
    ),
    pytest.param(
        bool_cond(BooleanOp.OR, [Condition(Column("a"), Op.EQ, 1), Column("event_id")]),
        None,
        None,
        InvalidExpression(
            "invalid boolean: Column(name='event_id', subscriptable=None, key=None) is not a valid condition"
        ),
        id="not all conditions",
    ),
]


@pytest.mark.parametrize("cond_wrapper, valid, translated, exception", boolean_tests)
def test_boolean_condition(
    cond_wrapper: Callable[[], Any],
    valid: Optional[BooleanCondition],
    translated: Optional[str],
    exception: Optional[Exception],
) -> None:
    def verify() -> None:
        exp = cond_wrapper()
        assert exp == valid
        assert TRANSLATOR.visit(exp) == translated

    if exception is not None:
        with pytest.raises(type(exception), match=re.escape(str(exception))):
            verify()
    else:
        verify()

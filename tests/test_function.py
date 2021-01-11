import pytest
from typing import Any, Callable, Optional

from snuba_sdk.conditions import Op
from snuba_sdk.expressions import (
    Function,
    Column,
    InvalidExpression,
)
from snuba_sdk.visitors import Translation
from tests import col, func


tests = [
    pytest.param(
        func("toString", [Column("event_id")], "event_id_str"),
        Function("toString", [Column("event_id")], "event_id_str"),
        "toString(event_id) AS event_id_str",
        None,
        id="basic function test",
    ),
    pytest.param(
        func("toString", [Column("event_id")]),
        Function("toString", [Column("event_id")], None),
        "toString(event_id)",
        None,
        id="no alias required",
    ),
    pytest.param(
        func(
            "someFunc",
            [
                Column("event_id"),
                None,
                "stuff",
                func("toString", [Column("event_id")], "event_id_str"),
            ],
            "someFunc",
        ),
        Function(
            "someFunc",
            [
                Column("event_id"),
                None,
                "stuff",
                Function("toString", [Column("event_id")], "event_id_str"),
            ],
            "someFunc",
        ),
        "someFunc(event_id, NULL, 'stuff', toString(event_id) AS event_id_str) AS someFunc",
        None,
        id="all possible parameter types",
    ),
    pytest.param(
        func(1, ["foo"], "invalid"),
        None,
        "",
        InvalidExpression("function '1' must be a string"),
        id="invalid function",
    ),
    pytest.param(
        func("", ["foo"], "invalid"),
        None,
        "",
        InvalidExpression("function cannot be empty"),
        id="empty function",
    ),
    pytest.param(
        func("¡amigo!", ["foo"], "invalid"),
        None,
        "",
        InvalidExpression("function '¡amigo!' contains invalid characters"),
        id="empty function",
    ),
    pytest.param(
        func("foo", ["foo"], 10),
        None,
        "",
        InvalidExpression(
            "alias '10' of function foo must be None or a non-empty string"
        ),
        id="invalid alias type",
    ),
    pytest.param(
        func("foo", ["foo"], ""),
        None,
        "",
        InvalidExpression(
            "alias '' of function foo must be None or a non-empty string"
        ),
        id="empty alias",
    ),
    pytest.param(
        func("foo", ["foo"], "¡amigo!"),
        None,
        "",
        InvalidExpression(
            "alias '¡amigo!' of function foo contains invalid characters"
        ),
        id="invalid alias",
    ),
    pytest.param(
        func("toString", ["foo", Op.EQ, Column("foo")], "invalid"),
        None,
        "",
        InvalidExpression("parameter 'Op.EQ' of function toString is an invalid type"),
        id="invalid function parameter type",
    ),
    pytest.param(
        func("toString", ["foo", col("¡amigo!")], "invalid"),
        None,
        "",
        InvalidExpression("'¡amigo!' is empty or contains invalid characters"),
        id="invalid function parameter",
    ),
]


TRANSLATOR = Translation()


@pytest.mark.parametrize("func_wrapper, valid, translated, exception", tests)
def test_functions(
    func_wrapper: Callable[[], Any],
    valid: Function,
    translated: str,
    exception: Optional[Exception],
) -> None:
    def verify() -> None:
        exp = func_wrapper()
        assert exp == valid
        assert TRANSLATOR.visit(exp) == translated

    if exception is not None:
        with pytest.raises(type(exception), match=str(exception)):
            verify()
    else:
        verify()

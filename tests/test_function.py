import re
from typing import Any, Callable, Optional

import pytest

from snuba_sdk.column import Column, InvalidColumn
from snuba_sdk.conditions import Op
from snuba_sdk.function import CurriedFunction, Function, InvalidFunction
from snuba_sdk.visitors import Translation
from tests import col, cur_func, func

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
        func("foo", [Column("foo")], "equation[0]"),
        Function("foo", [Column("foo")], "equation[0]"),
        "foo(foo) AS equation[0]",
        None,
        id="alias with square brackets",
    ),
    pytest.param(
        func(1, ["foo"], "invalid"),
        None,
        "",
        InvalidFunction("function '1' must be a string"),
        id="invalid function",
    ),
    pytest.param(
        func("", ["foo"], "invalid"),
        None,
        "",
        InvalidFunction("function cannot be empty"),
        id="empty function",
    ),
    pytest.param(
        func("¡amigo!", ["foo"], "invalid"),
        None,
        "",
        InvalidFunction("function '¡amigo!' contains invalid characters"),
        id="empty function",
    ),
    pytest.param(
        func("foo", ["foo"], 10),
        None,
        "",
        InvalidFunction(
            "alias '10' of function foo must be None or a non-empty string"
        ),
        id="invalid alias type",
    ),
    pytest.param(
        func("foo", ["foo"], ""),
        None,
        "",
        InvalidFunction("alias '' of function foo must be None or a non-empty string"),
        id="empty alias",
    ),
    pytest.param(
        func("foo", ["foo"], "'amigo!"),
        None,
        "",
        InvalidFunction("alias ''amigo!' of function foo contains invalid characters"),
        id="invalid alias",
    ),
    pytest.param(
        func("toString", ["foo", Op.EQ, Column("foo")], "invalid"),
        None,
        "",
        InvalidFunction("parameter 'Op.EQ' of function toString is an invalid type"),
        id="invalid function parameter type",
    ),
    pytest.param(
        func("toString", ["foo", col("¡amigo!")], "invalid"),
        None,
        "",
        InvalidColumn("'¡amigo!' is empty or contains invalid characters"),
        id="invalid function parameter",
    ),
]


TRANSLATOR = Translation()


@pytest.mark.parametrize("func_wrapper, valid, translated, exception", tests)
def test_functions(
    func_wrapper: Callable[[], Any],
    valid: Optional[Function],
    translated: str,
    exception: Optional[Exception],
) -> None:
    def verify() -> None:
        exp = func_wrapper()
        assert exp == valid
        assert TRANSLATOR.visit(exp) == translated

    if exception is not None:
        with pytest.raises(type(exception), match=re.escape(str(exception))):
            verify()
    else:
        verify()


curried_tests = [
    pytest.param(
        cur_func(
            "someFunc",
            [Column("event_id"), None, "stuff"],
            [
                Column("event_id"),
                None,
                "stuff",
                [1, 2, 3],
                func("toString", [Column("event_id")], "event_id_str"),
                cur_func("quantile", [0.5], [Column("duration")], "cur_str"),
            ],
            "someFunc",
        ),
        CurriedFunction(
            "someFunc",
            [Column("event_id"), None, "stuff"],
            [
                Column("event_id"),
                None,
                "stuff",
                [1, 2, 3],
                Function("toString", [Column("event_id")], "event_id_str"),
                CurriedFunction("quantile", [0.5], [Column("duration")], "cur_str"),
            ],
            "someFunc",
        ),
        "someFunc(event_id, NULL, 'stuff')(event_id, NULL, 'stuff', array(1, 2, 3), toString(event_id) AS event_id_str, quantile(0.5)(duration) AS cur_str) AS someFunc",
        None,
        id="all curried possible parameter types",
    ),
    pytest.param(
        cur_func("someFunc", [], [Column("event_id")], "someFunc"),
        CurriedFunction(
            "someFunc",
            [],
            [Column("event_id")],
            "someFunc",
        ),
        "someFunc()(event_id) AS someFunc",
        None,
        id="zero length initializers",
    ),
    pytest.param(
        cur_func("someFunc", None, [Column("event_id")], "someFunc"),
        CurriedFunction(
            "someFunc",
            None,
            [Column("event_id")],
            "someFunc",
        ),
        "someFunc(event_id) AS someFunc",
        None,
        id="None initializers",
    ),
    pytest.param(
        cur_func("foo", [[1, 2, 3]], ["foo"], "invalid"),
        None,
        "",
        InvalidFunction("initializers to function foo must be a scalar or column"),
        id="invalid initializers",
    ),
]


@pytest.mark.parametrize("func_wrapper, valid, translated, exception", curried_tests)
def test_curried_functions(
    func_wrapper: Callable[[], Any],
    valid: Optional[CurriedFunction],
    translated: str,
    exception: Optional[Exception],
) -> None:
    def verify() -> None:
        exp = func_wrapper()
        assert exp == valid
        assert TRANSLATOR.visit(exp) == translated

    if exception is not None:
        with pytest.raises(type(exception), match=re.escape(str(exception))):
            verify()
    else:
        verify()

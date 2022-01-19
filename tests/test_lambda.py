from __future__ import annotations

import re
from typing import Any, Optional

import pytest

from snuba_sdk.column import Column
from snuba_sdk.function import Function, Identifier, InvalidLambdaError, Lambda
from snuba_sdk.visitors import Translation

TRANSLATOR = Translation()

tests = [
    pytest.param(
        ["x"],
        Function("identity", [Identifier("x")]),
        Lambda(["x"], Function("identity", [Identifier("x")])),
        "(`x`) -> identity(`x`)",
        None,
        id="basic lambda",
    ),
    pytest.param(
        ["x", "y"],
        Function("identity", [Identifier("x"), Identifier("y")]),
        Lambda(
            ["x", "y"],
            Function("identity", [Identifier("x"), Identifier("y")]),
        ),
        "(`x`, `y`) -> identity(`x`, `y`)",
        None,
        id="lambda with multiple identifiers",
    ),
    pytest.param(
        ["x"],
        Function(
            "identity",
            [Lambda(["y"], Function("tuple", [Identifier("x"), Identifier("y")]))],
        ),
        Lambda(
            ["x"],
            Function(
                "identity",
                [Lambda(["y"], Function("tuple", [Identifier("x"), Identifier("y")]))],
            ),
        ),
        "(`x`) -> identity((`y`) -> tuple(`x`, `y`))",
        None,
        id="lambda with nested lambdas",
    ),
    pytest.param(
        [1],
        Function("identity", [Identifier("x")]),
        None,
        "",
        InvalidLambdaError("1 is not a valid identifier"),
        id="invalid identifier",
    ),
    pytest.param(
        ["$sdsd!"],
        Function("identity", [Identifier("x")]),
        None,
        "",
        InvalidLambdaError("$sdsd! is not a valid identifier"),
        id="invalid identifier chars",
    ),
    pytest.param(
        ["x"],
        Column("x"),
        None,
        "",
        InvalidLambdaError("transformation must be a function"),
        id="invalid transformation",
    ),
]


@pytest.mark.parametrize(
    "identifiers, transformation, valid, translated, exception", tests
)
def test_lambda(
    identifiers: list[Any],
    transformation: Any,
    valid: Optional[Lambda],
    translated: str,
    exception: Optional[Exception],
) -> None:
    def verify() -> None:
        exp = Lambda(identifiers, transformation)
        assert exp == valid
        assert TRANSLATOR.visit(exp) == translated

    if exception is not None:
        with pytest.raises(type(exception), match=re.escape(str(exception))):
            verify()
    else:
        verify()

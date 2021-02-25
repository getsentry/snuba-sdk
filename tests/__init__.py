import types
from typing import Any, Callable, List, Optional

from snuba_sdk.conditions import BooleanCondition, Condition
from snuba_sdk.expressions import Column, CurriedFunction, Function


# Wrappers to lazily build the expressions
def cond(lhs: Any, op: Any, rhs: Any) -> Callable[[], Any]:
    def to_cond() -> Condition:
        return Condition(lhs, op, rhs)

    return to_cond


def bool_cond(op: Any, conditions: Any) -> Callable[[], Any]:
    def to_bool() -> BooleanCondition:
        return BooleanCondition(op, conditions)

    return to_bool


def col(name: Any) -> Callable[[], Any]:
    def to_column() -> Column:
        return Column(name)

    return to_column


def func(function: Any, parameters: List[Any], alias: Any = None) -> Callable[[], Any]:
    def to_func() -> Function:
        params = []
        for param in parameters:
            if isinstance(param, types.FunctionType):
                params.append(param())
            else:
                params.append(param)

        return Function(function, params, alias)

    return to_func


def cur_func(
    function: Any,
    initializers: Optional[List[Any]],
    parameters: List[Any],
    alias: Any = None,
) -> Callable[[], Any]:
    def to_func() -> CurriedFunction:
        initers = None
        if initializers is not None:
            initers = []
            for initer in initializers:
                if isinstance(initer, types.FunctionType):
                    initers.append(initer())
                else:
                    initers.append(initer)

        params = []
        for param in parameters:
            if isinstance(param, types.FunctionType):
                params.append(param())
            else:
                params.append(param)

        return CurriedFunction(function, initers, params, alias)

    return to_func

import types
from typing import Any, Callable, List

from snuba_sdk.expressions import Column, Condition, Function


# Wrappers to lazily build the expressions
def cond(lhs: Any, op: Any, rhs: Any) -> Callable[[], Any]:
    def to_cond() -> Condition:
        return Condition(lhs, op, rhs)

    return to_cond


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

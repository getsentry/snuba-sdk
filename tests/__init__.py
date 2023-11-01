from __future__ import annotations

import types
from typing import Any, Callable, Optional

from snuba_sdk.column import Column
from snuba_sdk.conditions import And, BooleanCondition, Condition, Or
from snuba_sdk.formula import Formula
from snuba_sdk.function import CurriedFunction, Function
from snuba_sdk.timeseries import Timeseries


# Wrappers to lazily build the expressions
def cond(lhs: Any, op: Any, rhs: Any = None) -> Callable[[], Any]:
    def to_cond() -> Condition:
        return Condition(lhs, op, rhs)

    return to_cond


def bool_cond(op: Any, conditions: Any) -> Callable[[], Any]:
    def to_bool() -> BooleanCondition:
        return BooleanCondition(op, conditions)

    return to_bool


def and_cond(conditions: Any) -> Callable[[], Any]:
    def to_and() -> And:
        return And(conditions)

    return to_and


def or_cond(conditions: Any) -> Callable[[], Any]:
    def to_or() -> Or:
        return Or(conditions)

    return to_or


def col(name: Any) -> Callable[[], Any]:
    def to_column() -> Column:
        return Column(name)

    return to_column


def func(function: Any, parameters: list[Any], alias: Any = None) -> Callable[[], Any]:
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
    initializers: Optional[list[Any]],
    parameters: list[Any],
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


def formula(
    operator: Any,
    parameters: Any,
    filters: Any = None,
    groupby: Any = None,
) -> Callable[[], Any]:
    def to_formula() -> Formula:
        return Formula(operator, parameters, filters, groupby)

    return to_formula


def timeseries(
    metric: Any,
    aggregate: Any,
    aggregate_params: Any = None,
    filters: Any = None,
    groupby: Any = None,
) -> Callable[[], Any]:
    def to_timeseries() -> Timeseries:
        return Timeseries(metric, aggregate, aggregate_params, filters, groupby)

    return to_timeseries

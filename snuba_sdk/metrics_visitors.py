from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import asdict
from typing import Any, Generic, Mapping, Sequence, TypeVar, Union

from snuba_sdk.aliased_expression import AliasedExpression
from snuba_sdk.column import Column
from snuba_sdk.conditions import BooleanCondition, Condition, ConditionGroup
from snuba_sdk.expressions import InvalidExpressionError
from snuba_sdk.formula import (
    PREFIX_ALIASES,
    PREFIX_TO_INFIX,
    Formula,
    FormulaParameterGroup,
)
from snuba_sdk.timeseries import Metric, MetricsScope, Rollup, Timeseries
from snuba_sdk.visitors import Translation

TVisited = TypeVar("TVisited")


class TimeseriesVisitor(ABC, Generic[TVisited]):
    def visit(self, timeseries: Timeseries) -> TVisited:
        fields = timeseries.get_fields()
        returns = {}
        for field in fields:
            if field == "aggregate_params":
                continue
            elif field == "aggregate":
                returns[field] = self._visit_aggregate(
                    getattr(timeseries, field), timeseries.aggregate_params
                )
            else:
                returns[field] = getattr(self, f"_visit_{field}")(
                    getattr(timeseries, field)
                )

        return self._combine(timeseries, returns)

    @abstractmethod
    def _combine(
        self,
        timeseries: Timeseries,
        returns: Mapping[str, TVisited | Mapping[str, TVisited]],
    ) -> TVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_metric(
        self, metric: Metric
    ) -> str | Mapping[str, TVisited | Mapping[str, TVisited | None]]:
        raise NotImplementedError

    @abstractmethod
    def _visit_aggregate(
        self, aggregate: str, aggregate_params: list[Any] | None
    ) -> TVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_filters(self, filters: ConditionGroup | None) -> TVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_groupby(
        self, groupby: list[Column | AliasedExpression] | None
    ) -> TVisited:
        raise NotImplementedError


# Used in the SELECT and in the ORDER BY
AGGREGATE_ALIAS = "aggregate_value"


def _visit_mql_filters(
    filters: ConditionGroup | None, expression_visitor: Translation
) -> str:
    conditions = []
    if filters is not None:
        for c in filters:
            if isinstance(c, Condition):
                conditions.append(expression_visitor._visit_condition_mql(c))
            elif isinstance(c, BooleanCondition):
                conditions.append(expression_visitor._visit_boolean_condition_mql(c))
        # We use by default the `AND` operator as joint operator for printing top level conditions.
        return "{" + " AND ".join(conditions) + "}"
    return ""


def _visit_mql_groupby(
    groupby: list[Column | AliasedExpression] | None, expression_visitor: Translation
) -> str:
    if groupby is not None:
        return " by (" + ", ".join(expression_visitor.visit(c) for c in groupby) + ")"
    return ""


class TimeseriesMQLPrinter(TimeseriesVisitor[str]):
    def __init__(
        self,
        expression_visitor: Translation | None = None,
        metrics_visitor: MetricMQLPrinter | None = None,
    ) -> None:
        self.expression_visitor = expression_visitor or Translation(is_mql=True)
        self.metrics_visitor = metrics_visitor or MetricMQLPrinter()

    def _combine(
        self,
        timeseries: Timeseries,
        returns: Mapping[str, str | Mapping[str, str]],
    ) -> str:
        mql_string = returns["metric"]
        assert isinstance(mql_string, str)
        if returns["aggregate"]:
            aggregate = returns["aggregate"]
            mql_string = f"{aggregate}({mql_string})"

        if returns["filters"]:
            filters = str(returns["filters"])
            mql_string += f"{filters}"

        if returns["groupby"]:
            groupby = str(returns["groupby"])
            mql_string += f"{groupby}"

        return mql_string

    def _visit_metric(self, metric: Metric) -> str:
        return self.metrics_visitor.visit(metric)

    def _visit_aggregate(
        self, aggregate: str, aggregate_params: list[Any] | None
    ) -> str:
        aggregate_params_st = ""
        if aggregate_params:
            aggregate_params_st = f"({', '.join(self.expression_visitor.visit(p) for p in aggregate_params)})"
        return f"{aggregate}{aggregate_params_st}"

    def _visit_filters(self, filters: ConditionGroup | None) -> str:
        return _visit_mql_filters(filters, self.expression_visitor)

    def _visit_groupby(self, groupby: list[Column | AliasedExpression] | None) -> str:
        return _visit_mql_groupby(groupby, self.expression_visitor)


class FormulaMQLPrinter:
    def __init__(self, timeseries_visitor: TimeseriesMQLPrinter | None = None) -> None:
        self.timeseries_visitor = timeseries_visitor or TimeseriesMQLPrinter()
        self.expression_visitor = self.timeseries_visitor.expression_visitor

    def _visit_parameter(self, parameter: FormulaParameterGroup) -> str:
        if isinstance(parameter, Timeseries):
            return self.timeseries_visitor.visit(parameter)
        elif isinstance(parameter, Formula):
            return self.visit(parameter)

        return str(parameter)

    def _visit_aggregate_params(self, aggregate_params: list[Any] | None) -> str:
        if aggregate_params:
            return "(" + ", ".join(str(param) for param in aggregate_params) + ")"
        return ""

    def _visit_filters(self, filters: ConditionGroup | None) -> str:
        return _visit_mql_filters(filters, self.expression_visitor)

    def _visit_groupby(self, groupby: list[Column | AliasedExpression] | None) -> str:
        return _visit_mql_groupby(groupby, self.expression_visitor)

    def visit(self, formula: Formula) -> str:
        assert formula.parameters is not None

        parameters = [self._visit_parameter(p) for p in formula.parameters]

        # Infix vs. prefix
        # TODO: Formulas currently only support simple math, however in the future they could support
        # arbitrary functions (e.g. failure_rate(sum(...), 50)). In that case, they could be represented
        # as prefix functions.
        param_strings = []
        for p in parameters:
            if isinstance(p, str):
                param_strings.append(p)
        if formula.function_name in PREFIX_TO_INFIX:
            separator = f" {PREFIX_TO_INFIX[formula.function_name]} "
            mql_string = f"({separator.join(param_strings)})"
        else:
            mql_string = (
                f"{PREFIX_ALIASES.get(formula.function_name) or formula.function_name}"
                f"{self._visit_aggregate_params(formula.aggregate_params)}({', '.join(param_strings)})"
            )

        mql_string += f"{self._visit_filters(formula.filters)}"
        mql_string += f"{self._visit_groupby(formula.groupby)}"

        return mql_string


class MetricVisitor(ABC, Generic[TVisited]):
    @abstractmethod
    def visit(self, metric: Metric) -> TVisited:
        raise NotImplementedError


class MetricMQLPrinter(MetricVisitor[str]):
    def visit(self, metric: Metric) -> str:
        if metric.mri is not None:
            return metric.mri

        if metric.public_name is not None:
            return metric.public_name

        raise InvalidExpressionError(
            "metric.mri or metric.public is required for serialization"
        )


class RollupVisitor(ABC, Generic[TVisited]):
    @abstractmethod
    def visit(self, rollup: Rollup) -> TVisited:
        raise NotImplementedError


class RollupMQLPrinter(RollupVisitor[Mapping[str, Union[str, int, None]]]):
    def visit(self, rollup: Rollup) -> dict[str, str | int | None]:
        return {
            "orderby": rollup.orderby.value if rollup.orderby else None,
            "granularity": rollup.granularity,
            "interval": rollup.interval,
            "with_totals": str(rollup.totals) if rollup.totals is not None else None,
        }


class ScopeVisitor(ABC, Generic[TVisited]):
    @abstractmethod
    def visit(self, scope: MetricsScope) -> TVisited:
        raise NotImplementedError


class ScopeMQLPrinter(ScopeVisitor[Mapping[str, Union[str, Sequence[int]]]]):
    def visit(self, scope: MetricsScope) -> dict[str, str | list[int]]:
        return asdict(scope)

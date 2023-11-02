from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Generic, Mapping, Sequence, Union, TypeVar

from snuba_sdk.column import Column
from snuba_sdk.conditions import And, Condition, ConditionGroup, Op
from snuba_sdk.expressions import InvalidExpressionError, Totals
from snuba_sdk.formula import ArithmeticOperator, Formula, FormulaParameterGroup
from snuba_sdk.function import CurriedFunction, Function
from snuba_sdk.orderby import Direction, OrderBy
from snuba_sdk.timeseries import Metric, MetricsScope, Rollup, Timeseries
from snuba_sdk.visitors import Translation

TVisited = TypeVar("TVisited")


class FormulaVisitor(ABC, Generic[TVisited]):
    def visit(self, formula: Formula) -> Mapping[str, TVisited]:
        fields = formula.get_fields()
        returns = {}
        for field in fields:
            returns[field] = getattr(self, f"_visit_{field}")(
                getattr(formula, field)
            )

        return self._combine(formula, returns)

    @abstractmethod
    def _combine(
        self,
        formula: Formula,
        returns: Mapping[str, TVisited | Mapping[str, TVisited]],
    ) -> Mapping[str, TVisited]:
        raise NotImplementedError

    @abstractmethod
    def _visit_operator(self, operator: ArithmeticOperator) -> TVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_parameters(self, parameters: Sequence[FormulaParameterGroup]) -> TVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_filters(self, filters: ConditionGroup | None) -> TVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_groupby(self, groupby: list[Column] | None) -> TVisited:
        raise NotImplementedError


class FormulaSnQLPrinter(FormulaVisitor[str]):
    def __init__(
        self,
        expression_visitor: Translation | None = None,
    ) -> None:
        self.expression_visitor = expression_visitor or Translation()
        self.timeseries_visitor = TimeseriesSnQLPrinter(self.expression_visitor)

    def _combine(
        self,
        formula: Formula,
        returns: Mapping[str, str | Mapping[str, str]],
    ) -> Mapping[str, str]:
        # Collapse down the groupby and filters of formula to the timeseries
        if 'parameters' in returns:
            for parameter in returns['parameters']:
                if isinstance(parameter, dict):
                    # parameter is a timeseries
                    if 'filters' in returns and returns["filters"] is not None and returns["filters"] != "":
                        parameter["filters"] = " AND ".join([returns["filters"], parameter["filters"]])
                        returns["filters"] = ""
                    if 'groupby' in returns and returns["groupby"] is not None and returns["groupby"] != "":
                        parameter["groupby"] = ", ".join([returns["groupby"], parameter["groupby"]])
                        returns["groupby"] = ""

        return returns

    def _visit_operator(self, operator: str) -> str:
        return operator

    def _visit_parameters(self, parameters: Sequence[FormulaParameterGroup] | None) -> Union[str, dict[str, Any]]:
        ret = []
        for parameter in parameters:
            if isinstance(parameter, Formula):
                ret.append(self.visit(parameter))
            elif isinstance(parameter, Timeseries):
                ret.append(self.timeseries_visitor.visit(parameter))
            else:
                ret.append(parameter)
        return ret

    def _visit_filters(self, filters: ConditionGroup | None) -> str:
        if filters is not None:
            return " AND ".join(self.expression_visitor.visit(c) for c in filters)
        return ""

    def _visit_groupby(self, groupby: list[Column] | None) -> str:
        if groupby is not None:
            return ", ".join(self.expression_visitor.visit(c) for c in groupby)
        return ""


class TimeseriesVisitor(ABC, Generic[TVisited]):
    def visit(self, timeseries: Timeseries) -> Mapping[str, TVisited]:
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
    ) -> Mapping[str, TVisited]:
        raise NotImplementedError

    @abstractmethod
    def _visit_metric(self, metric: Metric) -> Mapping[str, TVisited]:
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
    def _visit_groupby(self, groupby: list[Column] | None) -> TVisited:
        raise NotImplementedError


# Used in the SELECT and in the ORDER BY
AGGREGATE_ALIAS = "aggregate_value"


class TimeseriesSnQLPrinter(TimeseriesVisitor[str]):
    def __init__(
        self,
        expression_visitor: Translation | None = None,
        metrics_visitor: MetricSnQLPrinter | None = None,
    ) -> None:
        self.expression_visitor = expression_visitor or Translation()
        self.metrics_visitor = metrics_visitor or MetricSnQLPrinter(expression_visitor)

    def _combine(
        self,
        timeseries: Timeseries,
        returns: Mapping[str, str | Mapping[str, str]],
    ) -> Mapping[str, str]:
        metric_data = returns["metric"]
        assert isinstance(metric_data, Mapping)  # mypy

        ret = {
            "entity": metric_data["entity"],
            "aggregate": str(returns["aggregate"]),
            "metric_filter": metric_data["metric_filter"],
        }

        if returns["filters"] is not None:
            ret["filters"] = str(returns["filters"])

        if returns["groupby"] is not None:
            ret["groupby"] = str(returns["groupby"])

        if timeseries.groupby is not None:
            ret[
                "groupby"
            ] = f"{', '.join(self.expression_visitor.visit(c) for c in timeseries.groupby)}"

        return ret

    def _visit_metric(self, metric: Metric) -> Mapping[str, str]:
        return self.metrics_visitor.visit(metric)

    def _visit_aggregate(
        self, aggregate: str, aggregate_params: list[Any] | None
    ) -> str:
        aggregate = CurriedFunction(
            aggregate, aggregate_params, [Column("value")], AGGREGATE_ALIAS
        )
        return self.expression_visitor.visit(aggregate)

    def _visit_filters(self, filters: ConditionGroup | None) -> str:
        if filters is not None:
            return " AND ".join(self.expression_visitor.visit(c) for c in filters)
        return ""

    def _visit_groupby(self, groupby: list[Column] | None) -> str:
        if groupby is not None:
            return ", ".join(self.expression_visitor.visit(c) for c in groupby)
        return ""


class MetricVisitor(ABC, Generic[TVisited]):
    @abstractmethod
    def visit(self, metric: Metric) -> TVisited:
        raise NotImplementedError


class MetricSnQLPrinter(MetricVisitor[Mapping[str, str]]):
    def __init__(self, translator: Translation | None = None) -> None:
        self.translator = translator or Translation()

    def visit(self, metric: Metric) -> Mapping[str, str]:
        if metric.id is None:
            raise InvalidExpressionError("metric.id is required for serialization")

        metric_filter = Condition(
            lhs=Column("metric_id"),
            op=Op.EQ,
            rhs=metric.id,
        )
        # TODO: Add a function that looks up the appropriate entity for a metric?
        assert metric.entity is not None
        return {
            "entity": metric.entity,
            "metric_filter": self.translator.visit(metric_filter),
        }


class RollupVisitor(ABC, Generic[TVisited]):
    @abstractmethod
    def visit(self, rollup: Rollup) -> TVisited:
        raise NotImplementedError


class RollupSnQLPrinter(RollupVisitor[Mapping[str, str]]):
    def __init__(self, translator: Translation | None = None) -> None:
        self.translator = translator or Translation()

    def visit(self, rollup: Rollup) -> Mapping[str, str]:
        condition = None
        if rollup.granularity is not None:
            condition = Condition(
                lhs=Column("granularity"),
                op=Op.EQ,
                rhs=rollup.granularity,
            )

        interval = ""
        orderby = ""
        with_totals = ""
        if rollup.interval:
            interval_exp = Function(
                "toStartOfInterval",
                [
                    Column("timestamp"),
                    Function("toIntervalSecond", [rollup.interval]),
                    "Universal",
                ],
                alias="time",
            )
            interval = self.translator.visit(interval_exp)
            orderby_exp = OrderBy(Column("time"), Direction.ASC)
            orderby = self.translator.visit(orderby_exp)
            if rollup.totals:
                with_totals = f"TOTALS {self.translator.visit(Totals(rollup.totals))}"
        elif rollup.orderby is not None:
            orderby_exp = OrderBy(Column(AGGREGATE_ALIAS), rollup.orderby)
            orderby = self.translator.visit(orderby_exp)

        return {
            "orderby": orderby,
            "filter": self.translator.visit(condition) if condition else "",
            "interval": interval,
            "with_totals": with_totals,
        }


class ScopeVisitor(ABC, Generic[TVisited]):
    @abstractmethod
    def visit(self, scope: MetricsScope) -> TVisited:
        raise NotImplementedError


class ScopeSnQLPrinter(ScopeVisitor[str]):
    def __init__(self, translator: Translation | None = None) -> None:
        self.translator = translator or Translation()

    def visit(self, scope: MetricsScope) -> str:
        condition = And(
            [
                Condition(
                    Column("org_id"),
                    Op.IN,
                    scope.org_ids,
                ),
                Condition(
                    Column("project_id"),
                    Op.IN,
                    scope.project_ids,
                ),
                Condition(
                    Column("use_case_id"),
                    Op.EQ,
                    scope.use_case_id,
                ),
            ]
        )

        return self.translator.visit(condition)

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import asdict
from typing import Any, Generic, Mapping, Sequence, TypeVar, Union

from snuba_sdk.aliased_expression import AliasedExpression
from snuba_sdk.column import Column
from snuba_sdk.conditions import (
    OPERATOR_TO_FUNCTION,
    And,
    BooleanCondition,
    Condition,
    ConditionGroup,
    Op,
)
from snuba_sdk.expressions import InvalidExpressionError, Totals
from snuba_sdk.formula import PREFIX_TO_INFIX, Formula, FormulaParameterGroup
from snuba_sdk.function import CurriedFunction, Function
from snuba_sdk.orderby import Direction, OrderBy
from snuba_sdk.timeseries import Metric, MetricsScope, Rollup, Timeseries
from snuba_sdk.visitors import Translation

TVisited = TypeVar("TVisited")


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
    def _visit_metric(self, metric: Metric) -> str | Mapping[str, TVisited]:
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

    def _visit_groupby(self, groupby: list[Column | AliasedExpression] | None) -> str:
        if groupby is not None:
            return ", ".join(self.expression_visitor.visit(c) for c in groupby)
        return ""


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
    ) -> Mapping[str, str]:
        metric_data = returns["metric"]
        assert isinstance(metric_data, Mapping)
        mql_string = metric_data["metric_name"]
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

        return {"mql_string": mql_string, "entity": metric_data["entity"]}

    def _visit_metric(self, metric: Metric) -> Mapping[str, str]:
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

    def _visit_parameter(self, parameter: FormulaParameterGroup) -> Mapping[str, str]:
        if isinstance(parameter, Timeseries):
            return self.timeseries_visitor.visit(parameter)
        elif isinstance(parameter, Formula):
            return self.visit(parameter)

        return {"mql_string": str(parameter)}

    def _visit_filters(self, filters: ConditionGroup | None) -> str:
        return _visit_mql_filters(filters, self.expression_visitor)

    def _visit_groupby(self, groupby: list[Column | AliasedExpression] | None) -> str:
        return _visit_mql_groupby(groupby, self.expression_visitor)

    def visit(self, formula: Formula) -> Mapping[str, str]:
        assert formula.parameters is not None

        parameters = [self._visit_parameter(p) for p in formula.parameters]

        # Infix vs. prefix
        # TODO: Formulas currently only support simple math, however in the future they could support
        # arbitrary functions (e.g. failure_rate(sum(...), 50)). In that case, they could be represented
        # as prefix functions.
        if formula.function_name in PREFIX_TO_INFIX:
            separator = f" {PREFIX_TO_INFIX[formula.function_name]} "
            mql_string = f"({separator.join(p['mql_string'] for p in parameters)})"
        else:
            mql_string = f"{formula.function_name}({', '.join(p['mql_string'] for p in parameters)})"

        mql_string += f"{self._visit_filters(formula.filters)}"
        mql_string += f"{self._visit_groupby(formula.groupby)}"

        entity = next(
            p.get("entity") for p in parameters if p.get("entity") is not None
        )
        assert entity is not None
        return {
            "mql_string": mql_string,
            "entity": entity,
        }


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


class MetricMQLPrinter(MetricVisitor[Mapping[str, str]]):
    def visit(self, metric: Metric) -> Mapping[str, str]:
        if metric.mri is None and metric.public_name is None:
            raise InvalidExpressionError(
                "metric.mri or metric.public is required for serialization"
            )
        if metric.entity is None:
            raise InvalidExpressionError("metric.entity is required for serialization")
        if metric.mri:
            return {"metric_name": metric.mri, "entity": metric.entity}
        assert metric.public_name is not None
        return {"metric_name": metric.public_name, "entity": metric.entity}


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


class ScopeMQLPrinter(ScopeVisitor[Mapping[str, Union[str, Sequence[int]]]]):
    def visit(self, scope: MetricsScope) -> dict[str, str | list[int]]:
        return asdict(scope)


# Normally this would be a visitor class, but this is itself a hack to get derived metrics off the ground
# This can be better codified when we write a visitor that converts Formulas to MQL.
class FormulaSnQLVisitor:
    def __init__(
        self, timeseries_visitor: TimeseriesFormulaSnQLPrinter | None = None
    ) -> None:
        self.timeseries_visitor = timeseries_visitor or TimeseriesFormulaSnQLPrinter()
        self.expression_visitor = self.timeseries_visitor.expression_visitor

    def _visit_parameter(
        self,
        side: Formula | Timeseries | int | float | str,
        filters: ConditionGroup | None,
    ) -> Mapping[str, str]:
        if isinstance(side, (float, int)):
            return {"select": f"{side}"}
        assert isinstance(
            side, Timeseries
        )  # This is guaranteed by the Formula validator

        if filters:
            if side.filters is not None:
                side = side.set_filters(
                    [*side.filters, *filters]
                )  # Push formula filters down into timeseries
            else:
                side = side.set_filters(filters)

        tdata = self.timeseries_visitor.visit(side)
        ret = {
            "entity": str(tdata["entity"]),
            "groupby": str(tdata["groupby"]),
            "select": str(tdata["aggregate"]),
        }

        return ret

    def visit(self, formula: Formula) -> Mapping[str, str]:
        parameters = []
        if formula.parameters is not None:
            parameters = [
                self._visit_parameter(p, formula.filters) for p in formula.parameters
            ]

        entity = None
        for p in parameters:
            if isinstance(p, dict):
                if "entity" in p:
                    if entity is None:
                        entity = p["entity"]
                    elif entity != p["entity"]:
                        raise InvalidExpressionError(
                            "Formulas can only operate on a single entity"
                        )
        assert entity is not None  # This is guaranteed by the Formula validator
        ret = {"entity": entity}

        # collect all the group bys
        groupbys = []
        if formula.groupby:
            for g in formula.groupby:
                groupbys.append(self.expression_visitor.visit(g))

        for p in parameters:
            if isinstance(p, dict) and "groupby" in p:
                groupbys.append(p["groupby"])
                break  # The validator guarantees all the groupbys are equal, so we only need one

        ret["groupby"] = ", ".join([g for g in groupbys if g != ""])

        # Collect select statements
        parameters = ", ".join(p["select"] for p in parameters)
        ret["aggregate"] = f"{formula.function_name}({parameters}) AS {AGGREGATE_ALIAS}"

        return ret


class TimeseriesFormulaSnQLPrinter(TimeseriesVisitor[str]):
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
        metric_data = self.metrics_visitor.visit(timeseries.metric)
        assert isinstance(metric_data, Mapping)  # mypy

        ret = {
            "entity": metric_data["entity"],
        }

        metric_filter = Function("equals", [Column("metric_id"), timeseries.metric.id])

        # In Formulas, the filters need to be built into the aggregate as infix functions
        filters = []
        if timeseries.filters:
            for cond in timeseries.filters:
                assert isinstance(cond.op, Op)  # BooleanOp is not a valid function
                op = OPERATOR_TO_FUNCTION[cond.op]
                filters.append(Function(op.value, [cond.lhs, cond.rhs]))

        and_filters = Function("and", [metric_filter, *filters])
        aggregate = CurriedFunction(
            f"{timeseries.aggregate}If",
            timeseries.aggregate_params,
            [Column("value"), and_filters],
        )
        ret["aggregate"] = self.expression_visitor.visit(aggregate)

        if returns["groupby"] is not None:
            ret["groupby"] = str(returns["groupby"])

        return ret

    def _visit_metric(self, metric: Metric) -> Mapping[str, str]:
        return {}

    def _visit_aggregate(
        self, aggregate: str, aggregate_params: list[Any] | None
    ) -> str:
        return ""

    def _visit_filters(self, filters: ConditionGroup | None) -> str:
        return ""

    def _visit_groupby(self, groupby: list[Column | AliasedExpression] | None) -> str:
        if groupby is not None:
            return ", ".join(self.expression_visitor.visit(c) for c in groupby)
        return ""

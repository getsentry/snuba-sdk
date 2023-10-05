from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Generic, Mapping, Sequence, TypeVar

# Import the module due to sphinx autodoc problems
# https://github.com/agronholm/sphinx-autodoc-typehints#dealing-with-circular-imports
from snuba_sdk import metrics_query as main
from snuba_sdk.aliased_expression import AliasedExpression
from snuba_sdk.column import Column
from snuba_sdk.conditions import BooleanCondition, Condition, ConditionGroup, Op
from snuba_sdk.expressions import list_type
from snuba_sdk.metrics_visitors import (
    RollupSnQLPrinter,
    ScopeSnQLPrinter,
    TimeseriesSnQLPrinter,
)
from snuba_sdk.timeseries import MetricsScope, Rollup, Timeseries
from snuba_sdk.visitors import Translation


class InvalidMetricsQueryError(Exception):
    pass


QVisited = TypeVar("QVisited")


class MetricsQueryVisitor(ABC, Generic[QVisited]):
    def visit(self, query: main.MetricsQuery) -> QVisited:
        fields = query.get_fields()
        returns = {}
        for field in fields:
            returns[field] = getattr(self, f"_visit_{field}")(getattr(query, field))

        return self._combine(query, returns)

    @abstractmethod
    def _combine(
        self,
        query: main.MetricsQuery,
        returns: Mapping[str, QVisited | Mapping[str, QVisited]],
    ) -> QVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_query(self, query: Timeseries | None) -> Mapping[str, QVisited]:
        raise NotImplementedError

    @abstractmethod
    def _visit_filters(self, filters: ConditionGroup | None) -> QVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_groupby(
        self, groupby: Sequence[Column | AliasedExpression] | None
    ) -> QVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_start(self, start: datetime | None) -> QVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_end(self, end: datetime | None) -> QVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_rollup(self, rollup: Rollup | None) -> Mapping[str, QVisited]:
        raise NotImplementedError

    @abstractmethod
    def _visit_scope(self, scope: MetricsScope | None) -> QVisited:
        raise NotImplementedError


class SnQLPrinter(MetricsQueryVisitor[str]):
    def __init__(self, pretty: bool = False) -> None:
        self.pretty = pretty
        self.expression_visitor = Translation()
        self.timeseries_visitor = TimeseriesSnQLPrinter(self.expression_visitor)
        self.rollup_visitor = RollupSnQLPrinter(self.expression_visitor)
        self.scope_visitor = ScopeSnQLPrinter(self.expression_visitor)
        self.separator = "\n" if self.pretty else " "
        self.match_clause = "MATCH ({entity})"
        self.select_clause = "SELECT {select_columns}"
        self.groupby_clause = "BY {groupby_columns}"
        self.where_clause = "WHERE {where_clauses}"
        self.orderby_clause = "ORDER BY {orderby_columns}"

    def _combine(
        self, query: main.MetricsQuery, returns: Mapping[str, str | Mapping[str, str]]
    ) -> str:
        timeseries_data = returns["query"]
        assert isinstance(timeseries_data, dict)

        entity = timeseries_data["entity"]

        select_columns = []
        select_columns.append(timeseries_data["aggregate"])

        rollup_data = returns["rollup"]
        assert isinstance(rollup_data, dict)

        groupby_columns = []
        orderby_columns = []
        where_clauses = []

        # Rollup information is a little complicated
        # If an interval is specified, then we need to use the interval
        # function to properly group by and order the query.
        # If totals is specified, then the order by will come from the
        # rollup itself and we don't use interval data.
        if rollup_data["interval"]:
            groupby_columns.append(rollup_data["interval"])
        if rollup_data["orderby"]:
            orderby_columns.append(rollup_data["orderby"])
        where_clauses.append(rollup_data["filter"])

        if timeseries_data["groupby"]:
            groupby_columns.append(timeseries_data["groupby"])
        if returns["groupby"]:
            groupby_columns.append(returns["groupby"])

        where_clauses.append(timeseries_data["metric_filter"])
        if timeseries_data["filters"]:
            where_clauses.append(timeseries_data["filters"])
        if returns["filters"]:
            where_clauses.append(returns["filters"])

        where_clauses.append(returns["scope"])
        where_clauses.append(returns["start"])
        where_clauses.append(returns["end"])

        groupby_clause = (
            self.groupby_clause.format(groupby_columns=", ".join(groupby_columns))
            if groupby_columns
            else ""
        )
        orderby_clause = (
            self.orderby_clause.format(orderby_columns=", ".join(orderby_columns))
            if orderby_columns
            else ""
        )

        return self.separator.join(
            [
                self.match_clause.format(entity=entity),
                self.select_clause.format(select_columns=", ".join(select_columns)),
                groupby_clause,
                self.where_clause.format(where_clauses=" AND ".join(where_clauses)),
                orderby_clause,
            ]
        ).strip()

    def _visit_query(self, query: Timeseries | None) -> Mapping[str, str]:
        if query is None:
            raise InvalidMetricsQueryError("MetricQuery.query must not be None")

        return self.timeseries_visitor.visit(query)

    def _visit_filters(self, filters: ConditionGroup | None) -> str:
        if filters is not None:
            return f"{' AND '.join(self.expression_visitor.visit(c) for c in filters)}"
        return ""

    def _visit_groupby(
        self, groupby: Sequence[Column | AliasedExpression] | None
    ) -> str:
        if groupby is not None:
            return f"{', '.join(self.expression_visitor.visit(g) for g in groupby)}"
        return ""

    def _visit_start(self, start: datetime | None) -> str:
        if start is None:
            raise InvalidMetricsQueryError("MetricQuery.start must not be None")

        condition = Condition(Column("timestamp"), Op.GTE, start)
        return self.expression_visitor.visit(condition)

    def _visit_end(self, end: datetime | None) -> str:
        if end is None:
            raise InvalidMetricsQueryError("MetricQuery.end must not be None")

        condition = Condition(Column("timestamp"), Op.LT, end)
        return self.expression_visitor.visit(condition)

    def _visit_rollup(self, rollup: Rollup | None) -> Mapping[str, str]:
        if rollup is None:
            raise InvalidMetricsQueryError("MetricQuery.rollup must not be None")

        return self.rollup_visitor.visit(rollup)

    def _visit_scope(self, scope: MetricsScope | None) -> str:
        if scope is None:
            raise InvalidMetricsQueryError("MetricQuery.scope must not be None")

        return self.scope_visitor.visit(scope)


class Validator(MetricsQueryVisitor[None]):
    def _combine(
        self, query: main.MetricsQuery, returns: Mapping[str, None | Mapping[str, None]]
    ) -> None:
        pass

    def _visit_query(self, query: Timeseries | None) -> Mapping[str, None]:
        if query is None:
            raise InvalidMetricsQueryError("query is required for a metrics query")
        elif not isinstance(query, Timeseries):
            raise InvalidMetricsQueryError("query must be a Timeseries")
        query.validate()
        return {}  # Necessary for typing

    def _visit_filters(self, filters: ConditionGroup | None) -> None:
        if filters is not None:
            if not list_type(filters, (Condition, BooleanCondition)):
                raise InvalidMetricsQueryError("filters must be a list of Conditions")
            (c.validate() for c in filters)

    def _visit_groupby(
        self, groupby: Sequence[Column | AliasedExpression] | None
    ) -> None:
        if groupby is not None:
            if not list_type(groupby, (Column, AliasedExpression)):
                raise InvalidMetricsQueryError("groupby must be a list of Columns")
            for g in groupby:
                g.validate()

    def _visit_start(self, start: datetime | None) -> None:
        if start is None:
            raise InvalidMetricsQueryError("start is required for a metrics query")
        elif not isinstance(start, datetime):
            raise InvalidMetricsQueryError("start must be a datetime")

    def _visit_end(self, end: datetime | None) -> None:
        if end is None:
            raise InvalidMetricsQueryError("end is required for a metrics query")
        elif not isinstance(end, datetime):
            raise InvalidMetricsQueryError("end must be a datetime")

    def _visit_rollup(self, rollup: Rollup | None) -> Mapping[str, None]:
        if rollup is None:
            raise InvalidMetricsQueryError("rollup is required for a metrics query")
        elif not isinstance(rollup, Rollup):
            raise InvalidMetricsQueryError("rollup must be a Rollup object")
        rollup.validate()
        return {}

    def _visit_scope(self, scope: MetricsScope | None) -> None:
        if scope is None:
            raise InvalidMetricsQueryError("scope is required for a metrics query")
        elif not isinstance(scope, MetricsScope):
            raise InvalidMetricsQueryError("scope must be a MetricsScope object")
        scope.validate()

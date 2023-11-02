from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Generic, Mapping, TypeVar

# Import the module due to sphinx autodoc problems
# https://github.com/agronholm/sphinx-autodoc-typehints#dealing-with-circular-imports
from snuba_sdk import metrics_query as main
from snuba_sdk.column import Column
from snuba_sdk.conditions import Condition, Op
from snuba_sdk.expressions import Limit, Offset
from snuba_sdk.formula import Formula
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

    @abstractmethod
    def _visit_limit(self, limit: Limit | None) -> QVisited:
        raise NotImplementedError

    def _visit_offset(self, offset: Offset | None) -> QVisited:
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
        self.limit_clause = "LIMIT {limit}"
        self.offset_clause = "OFFSET {offset}"

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

        where_clauses.append(timeseries_data["metric_filter"])
        if timeseries_data["filters"]:
            where_clauses.append(timeseries_data["filters"])

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

        limit_clause = ""
        if returns["limit"]:
            limit_clause = self.limit_clause.format(limit=returns["limit"])

        offset_clause = ""
        if returns["offset"]:
            offset_clause = self.offset_clause.format(offset=returns["offset"])

        totals_clause = ""
        if rollup_data["with_totals"]:
            totals_clause = rollup_data["with_totals"]

        clauses = [
            self.match_clause.format(entity=entity),
            self.select_clause.format(select_columns=", ".join(select_columns)),
            groupby_clause,
            self.where_clause.format(where_clauses=" AND ".join(where_clauses)),
            orderby_clause,
            limit_clause,
            offset_clause,
            totals_clause,
        ]

        return self.separator.join(filter(lambda x: x != "", clauses)).strip()

    def _visit_query(self, query: Timeseries | Formula | None) -> Mapping[str, str]:
        if query is None:
            raise InvalidMetricsQueryError("MetricQuery.query must not be None")
        if isinstance(query, Formula):
            raise InvalidMetricsQueryError(
                "Serializing a Formula in MetricQuery.query is unsupported"
            )
        return self.timeseries_visitor.visit(query)

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

    def _visit_limit(self, limit: Limit | None) -> str:
        if limit is not None:
            return self.expression_visitor.visit(limit)
        return ""

    def _visit_offset(self, offset: Offset | None) -> str:
        if offset is not None:
            return self.expression_visitor.visit(offset)
        return ""


class Validator(MetricsQueryVisitor[None]):
    def _combine(
        self, query: main.MetricsQuery, returns: Mapping[str, None | Mapping[str, None]]
    ) -> None:
        pass

    def _visit_query(self, query: Timeseries | Formula | None) -> Mapping[str, None]:
        if query is None:
            raise InvalidMetricsQueryError("query is required for a metrics query")
        elif not isinstance(query, (Timeseries, Formula)):
            raise InvalidMetricsQueryError("query must be a Timeseries or Formula")
        query.validate()
        return {}  # Necessary for typing

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

        # Since the granularity is inferred by the API, it can be initially None, but must be present when
        # the query is ultimately serialized and sent to Snuba.
        if rollup.granularity is None:
            raise InvalidMetricsQueryError("granularity must be set on the rollup")

        rollup.validate()
        return {}

    def _visit_scope(self, scope: MetricsScope | None) -> None:
        if scope is None:
            raise InvalidMetricsQueryError("scope is required for a metrics query")
        elif not isinstance(scope, MetricsScope):
            raise InvalidMetricsQueryError("scope must be a MetricsScope object")
        scope.validate()

    def _visit_limit(self, limit: Limit | None) -> None:
        if limit is None:
            return
        elif not isinstance(limit, Limit):
            raise InvalidMetricsQueryError("limit must be a Limit object")

        limit.validate()

    def _visit_offset(self, offset: Offset | None) -> None:
        if offset is None:
            return
        elif not isinstance(offset, Offset):
            raise InvalidMetricsQueryError("offset must be a Offset object")

        offset.validate()

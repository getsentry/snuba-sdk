from __future__ import annotations

import copy
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Generic, Mapping, Union, TypeVar

# Import the module due to sphinx autodoc problems
# https://github.com/agronholm/sphinx-autodoc-typehints#dealing-with-circular-imports
from snuba_sdk import metrics_query as main
from snuba_sdk.column import Column
from snuba_sdk.conditions import Condition, Op
from snuba_sdk.expressions import Limit, Offset
from snuba_sdk.formula import Formula, FormulaSnQL
from snuba_sdk.metrics_visitors import (
    FormulaSnQLPrinter,
    RollupSnQLPrinter,
    ScopeSnQLPrinter,
    TimeseriesSnQLPrinter,
)
from snuba_sdk.query import SnQLString
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
        self.formula_visitor = FormulaSnQLPrinter(self.expression_visitor)
        self.timeseries_visitor = TimeseriesSnQLPrinter(self.expression_visitor)
        self.rollup_visitor = RollupSnQLPrinter(self.expression_visitor)
        self.scope_visitor = ScopeSnQLPrinter(self.expression_visitor)

    def _combine(
        self, query: main.MetricsQuery, returns: Mapping[str, str | Mapping[str, str]]
    ) -> Union[SnQLString, FormulaSnQL]:
        query_data = returns["query"]
        assert isinstance(query_data, dict)
        if "operator" in query_data:
            # it is a formula
            formula_snql = FormulaSnQL()
            formula_snql.operator = query_data["operator"]
            params = []
            for parameter in query_data["parameters"]:
                if isinstance(parameter, dict):
                    new_returns = copy.deepcopy(returns)
                    new_returns["query"] = parameter
                    params.append(self._combine(query, new_returns))
                else:
                    params.append(parameter)
            formula_snql.parameters = params
            return formula_snql
        return self._build_snql_string(query, returns)

    def _build_snql_string(self, query: main.MetricsQuery, returns: Mapping[str, str | Mapping[str, str]]) -> SnQLString:
        snql_string = SnQLString(self.pretty)
        query_data = returns["query"]
        entity = query_data["entity"]

        select_columns = []
        select_columns.append(query_data["aggregate"])

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

        if query_data["groupby"]:
            groupby_columns.append(query_data["groupby"])

        where_clauses.append(query_data["metric_filter"])
        if query_data["filters"]:
            where_clauses.append(query_data["filters"])

        where_clauses.append(returns["scope"])
        where_clauses.append(returns["start"])
        where_clauses.append(returns["end"])

        snql_string.groupby_clause = (
            snql_string.groupby_clause.format(groupby_columns=", ".join(groupby_columns))
            if groupby_columns
            else ""
        )

        snql_string.orderby_clause = (
            snql_string.orderby_clause.format(orderby_columns=", ".join(orderby_columns))
            if orderby_columns
            else ""
        )

        snql_string.limit_clause = snql_string.limit_clause.format(limit=returns["limit"]) if returns["limit"] else ""
        snql_string.offset_clause = snql_string.offset_clause.format(offset=returns["offset"]) if returns["offset"] else ""
        snql_string.totals_clause = rollup_data["with_totals"] if rollup_data["with_totals"] else ""

        snql_string.match_clause = snql_string.match_clause.format(entity=entity)
        snql_string.select_clause = snql_string.select_clause.format(select_columns=", ".join(select_columns))
        snql_string.where_clause = snql_string.where_clause.format(where_clauses=" AND ".join(where_clauses))

        return snql_string

    def _visit_query(self, query: Timeseries | Formula | None) -> Mapping[str, str]:
        if query is None:
            raise InvalidMetricsQueryError("MetricQuery.query must not be None")
        if isinstance(query, Formula):
            return self.formula_visitor.visit(query)
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

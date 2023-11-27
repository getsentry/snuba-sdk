from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Mapping

from snuba_sdk import metrics_query as main
from snuba_sdk.expressions import Limit, Offset
from snuba_sdk.formula import Formula
from snuba_sdk.metrics_query_visitors import InvalidMetricsQueryError
from snuba_sdk.metrics_visitors import (
    RollupSnQLPrinter,
    ScopeSnQLPrinter,
    TimeseriesMQLPrinter,
)
from snuba_sdk.mql_context import MQLContext
from snuba_sdk.timeseries import MetricsScope, Rollup, Timeseries
from snuba_sdk.visitors import Translation


class MQLVisitor(ABC):
    def visit(self, query: main.MetricsQuery) -> Mapping[str, Any]:
        fields = query.get_fields()
        returns = {}
        for field in fields:
            returns[field] = getattr(self, f"_visit_{field}")(getattr(query, field))

        return self._combine(query, returns)

    @abstractmethod
    def _combine(
        self,
        query: main.MetricsQuery,
        returns: Mapping[str, Mapping[str, Any]],
    ) -> Mapping[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def _visit_query(self, query: Timeseries | None) -> Mapping[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def _visit_start(self, start: datetime | None) -> datetime:
        raise NotImplementedError

    @abstractmethod
    def _visit_end(self, end: datetime | None) -> datetime:
        raise NotImplementedError

    @abstractmethod
    def _visit_rollup(self, rollup: Rollup | None) -> Rollup:
        raise NotImplementedError

    @abstractmethod
    def _visit_scope(self, scope: MetricsScope | None) -> MetricsScope:
        raise NotImplementedError

    @abstractmethod
    def _visit_limit(self, limit: Limit | None) -> Limit | None:
        raise NotImplementedError

    def _visit_offset(self, offset: Offset | None) -> Offset | None:
        raise NotImplementedError


class MQLPrinter(MQLVisitor):
    def __init__(self) -> None:
        self.expression_visitor = Translation()
        self.timeseries_visitor = TimeseriesMQLPrinter(self.expression_visitor)
        self.rollup_visitor = RollupSnQLPrinter(self.expression_visitor)
        self.scope_visitor = ScopeSnQLPrinter(self.expression_visitor)

    def _combine(
        self, query: main.MetricsQuery, returns: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        """
        TODO: This printer only supports Timeseries queries for now. We will need to extend this
        for Formula queries. For now, this only returns the MQL string.
        """
        assert isinstance(returns["query"], Mapping)  # mypy
        mql_string = returns["query"]["mql_string"]
        mql_context = MQLContext(
            start=returns["start"],
            end=returns["end"],
            rollup=returns["rollup"],
            scope=returns["scope"],
            limit=returns["limit"],
            offset=returns["offset"],
        )
        return {
            "mql": mql_string,
            "mql_context": mql_context.serialize(),
        }

    def _visit_query(self, query: Timeseries | Formula | None) -> Mapping[str, str]:
        if query is None:
            raise InvalidMetricsQueryError("MetricQuery.query must not be None")
        if isinstance(query, Formula):
            raise InvalidMetricsQueryError(
                "Serializing a Formula in MetricQuery.query is unsupported"
            )

        return self.timeseries_visitor.visit(query)

    def _visit_start(self, start: datetime | None) -> datetime:
        if start is None:
            raise InvalidMetricsQueryError("MetricQuery.start must not be None")

        return start

    def _visit_end(self, end: datetime | None) -> datetime:
        if end is None:
            raise InvalidMetricsQueryError("MetricQuery.end must not be None")

        return end

    def _visit_rollup(self, rollup: Rollup | None) -> Rollup:
        if rollup is None:
            raise InvalidMetricsQueryError("MetricQuery.rollup must not be None")

        return rollup

    def _visit_scope(self, scope: MetricsScope | None) -> MetricsScope:
        if scope is None:
            raise InvalidMetricsQueryError("MetricQuery.scope must not be None")

        return scope

    def _visit_limit(self, limit: Limit | None) -> Limit | None:
        return limit

    def _visit_offset(self, offset: Offset | None) -> Offset | None:
        return offset

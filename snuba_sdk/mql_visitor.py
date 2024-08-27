from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import asdict
from datetime import datetime
from typing import Any, Mapping

from snuba_sdk import metrics_query as main
from snuba_sdk.expressions import Extrapolate, Limit, Offset
from snuba_sdk.formula import Formula
from snuba_sdk.metrics_query_visitors import InvalidMetricsQueryError
from snuba_sdk.metrics_visitors import (
    FormulaMQLPrinter,
    RollupMQLPrinter,
    ScopeMQLPrinter,
    TimeseriesMQLPrinter,
)
from snuba_sdk.mql.mql import parse_mql
from snuba_sdk.mql_context import MQLContext
from snuba_sdk.timeseries import MetricsScope, Rollup, Timeseries
from snuba_sdk.visitors import Translation


class MQLVisitor(ABC):
    def visit(self, query: main.MetricsQuery) -> dict[str, Any]:
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
    ) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def _visit_query(self, query: Timeseries | None) -> str:
        raise NotImplementedError

    @abstractmethod
    def _visit_start(self, start: datetime | None) -> str:
        raise NotImplementedError

    @abstractmethod
    def _visit_end(self, end: datetime | None) -> str:
        raise NotImplementedError

    @abstractmethod
    def _visit_rollup(self, rollup: Rollup | None) -> dict[str, str | int | None]:
        raise NotImplementedError

    @abstractmethod
    def _visit_scope(self, scope: MetricsScope | None) -> dict[str, str | list[int]]:
        raise NotImplementedError

    @abstractmethod
    def _visit_indexer_mappings(
        self, indexer_mappings: dict[str, str | int]
    ) -> dict[str, str | int]:
        raise NotImplementedError

    @abstractmethod
    def _visit_limit(self, limit: Limit | None) -> int | None:
        raise NotImplementedError

    @abstractmethod
    def _visit_offset(self, offset: Offset | None) -> int | None:
        raise NotImplementedError

    @abstractmethod
    def _visit_extrapolate(self, extrapolate: Extrapolate | None) -> bool | None:
        raise NotImplementedError


class MQLPrinter(MQLVisitor):
    def __init__(self) -> None:
        self.expression_visitor = Translation(is_mql=True)
        self.timeseries_visitor = TimeseriesMQLPrinter(self.expression_visitor)
        self.formula_visitor = FormulaMQLPrinter(self.timeseries_visitor)
        self.rollup_visitor = RollupMQLPrinter()
        self.scope_visitor = ScopeMQLPrinter()

    def _combine(
        self, query: main.MetricsQuery, returns: Mapping[str, Any]
    ) -> dict[str, Any]:
        assert isinstance(returns["query"], str)  # mypy
        mql_string = returns["query"]
        mql_context = MQLContext(
            start=returns["start"],
            end=returns["end"],
            rollup=returns["rollup"],
            scope=returns["scope"],
            limit=returns["limit"],
            offset=returns["offset"],
            extrapolate=returns["extrapolate"],
            indexer_mappings=returns["indexer_mappings"],
        )
        return {
            "mql": mql_string,
            "mql_context": asdict(mql_context),
        }

    def _visit_query(self, query: Timeseries | Formula | str | None) -> str:
        if query is None:
            raise InvalidMetricsQueryError("MetricQuery.query must not be None")
        elif isinstance(query, Formula):
            return self.formula_visitor.visit(query)
        elif isinstance(query, Timeseries):
            return self.timeseries_visitor.visit(query)
        else:
            query = parse_mql(query)
            return self._visit_query(query)

    def _visit_start(self, start: datetime | None) -> str:
        if start is None:
            raise InvalidMetricsQueryError("MetricQuery.start must not be None")

        return start.isoformat()

    def _visit_end(self, end: datetime | None) -> str:
        if end is None:
            raise InvalidMetricsQueryError("MetricQuery.end must not be None")

        return end.isoformat()

    def _visit_rollup(self, rollup: Rollup | None) -> dict[str, str | int | None]:
        if rollup is None:
            raise InvalidMetricsQueryError("MetricQuery.rollup must not be None")

        return self.rollup_visitor.visit(rollup)

    def _visit_scope(self, scope: MetricsScope | None) -> dict[str, str | list[int]]:
        if scope is None:
            raise InvalidMetricsQueryError("MetricQuery.scope must not be None")

        return self.scope_visitor.visit(scope)

    def _visit_limit(self, limit: Limit | None) -> int | None:
        return limit.limit if limit is not None else None

    def _visit_offset(self, offset: Offset | None) -> int | None:
        return offset.offset if offset is not None else None

    def _visit_extrapolate(self, extrapolate: Extrapolate | None) -> bool | None:
        return extrapolate.extrapolate if extrapolate is not None else None

    def _visit_indexer_mappings(
        self, indexer_mappings: dict[str, str | int]
    ) -> dict[str, str | int]:
        if indexer_mappings is None:
            raise InvalidMetricsQueryError(
                "MetricQuery.indexer_mappings must not be None"
            )
        return indexer_mappings

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Generic, Mapping, Optional, TypeVar

# Import the module due to sphinx autodoc problems
# https://github.com/agronholm/sphinx-autodoc-typehints#dealing-with-circular-imports
from snuba_sdk import metrics_query as main
from snuba_sdk.expressions import Extrapolate, Limit, Offset
from snuba_sdk.formula import Formula
from snuba_sdk.mql.mql import parse_mql
from snuba_sdk.timeseries import MetricsScope, Rollup, Timeseries


class InvalidMetricsQueryError(Exception):
    pass


QVisited = TypeVar("QVisited")


class MetricsQueryVisitor(ABC, Generic[QVisited]):
    def visit(self, query: main.MetricsQuery) -> QVisited | Mapping[str, QVisited]:
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
    ) -> QVisited | Mapping[str, QVisited]:
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
    def _visit_rollup(
        self, rollup: Rollup | None
    ) -> Mapping[str, QVisited | Mapping[str, QVisited]]:
        raise NotImplementedError

    @abstractmethod
    def _visit_scope(
        self, scope: MetricsScope | None
    ) -> QVisited | Mapping[QVisited, list[int] | Optional[QVisited]]:
        raise NotImplementedError

    @abstractmethod
    def _visit_limit(self, limit: Limit | None) -> QVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_offset(self, offset: Offset | None) -> QVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_extrapolate(self, extrapolate: Extrapolate | None) -> QVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_indexer_mappings(
        self, indexer_mappings: dict[str, str | int] | None
    ) -> QVisited:
        raise NotImplementedError


class Validator(MetricsQueryVisitor[None]):
    def _combine(
        self, query: main.MetricsQuery, returns: Mapping[str, None | Mapping[str, None]]
    ) -> None:
        pass

    def _visit_query(
        self, query: Timeseries | Formula | str | None
    ) -> Mapping[str, None]:
        if query is None:
            raise InvalidMetricsQueryError("query is required for a metrics query")
        elif not isinstance(query, (Timeseries, Formula, str)):
            raise InvalidMetricsQueryError(
                "query must be a Timeseries or Formula or MQL string"
            )

        if isinstance(query, str):
            # Parse the MQL string into the Formula/Timeseries object
            try:
                query = parse_mql(query)
            except Exception as e:
                raise InvalidMetricsQueryError(f"invalid MQL: {e}")

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

    def _visit_extrapolate(self, extrapolate: Extrapolate | None) -> None:
        if extrapolate is None:
            return
        elif not isinstance(extrapolate, Extrapolate):
            raise InvalidMetricsQueryError("extrapolate must be a Extrapolate object")

        extrapolate.validate()

    def _visit_indexer_mappings(
        self, indexer_mappings: dict[str, str | int] | None
    ) -> None:
        if indexer_mappings is None:
            return
        if not isinstance(indexer_mappings, dict):
            raise InvalidMetricsQueryError("indexer_mappings must be a dictionary")

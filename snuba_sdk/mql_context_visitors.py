from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Generic, Mapping, Optional, TypeVar

from snuba_sdk import mql_context as main
from snuba_sdk.expressions import Limit, Offset
from snuba_sdk.metrics_visitors import AGGREGATE_ALIAS
from snuba_sdk.timeseries import MetricsScope, Rollup


class InvalidMQLContextError(Exception):
    pass


QVisited = TypeVar("QVisited")


class MQLContextVisitor(ABC, Generic[QVisited]):
    def visit(self, query: main.MQLContext) -> Mapping[str, QVisited]:
        fields = query.get_fields()
        returns = {}
        for field in fields:
            if field == "entity":
                continue
            returns[field] = getattr(self, f"_visit_{field}")(getattr(query, field))

        return self._combine(query, returns)

    @abstractmethod
    def _combine(
        self,
        query: main.MQLContext,
        returns: Mapping[str, QVisited | Mapping[str, QVisited]],
    ) -> Mapping[str, QVisited]:
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
    def _visit_indexer_mappings(
        self, indexer_mappings: dict[str, Any] | None
    ) -> Mapping[str, QVisited]:
        raise NotImplementedError


class MQLContextPrinter(MQLContextVisitor[str]):
    def _combine(
        self, query: main.MQLContext, returns: Mapping[str, str | Mapping[str, str]]
    ) -> Mapping[str, Any]:
        return {
            "start": returns["start"],
            "end": returns["end"],
            "rollup": returns["rollup"],
            "scope": returns["scope"],
            "limit": returns["limit"],
            "offset": returns["offset"],
            "indexer_mappings": returns["indexer_mappings"],
        }

    def _visit_entity(self, entity: str | None) -> str:
        if entity is None:
            raise InvalidMQLContextError("MetricQuery.query must not be None")

        return entity

    def _visit_start(self, start: datetime | None) -> str:
        if start is None:
            raise InvalidMQLContextError("MetricQuery.start must not be None")

        return start.isoformat()

    def _visit_end(self, end: datetime | None) -> str:
        if end is None:
            raise InvalidMQLContextError("MetricQuery.end must not be None")

        return end.isoformat()

    def _visit_rollup(self, rollup: Rollup | None) -> Mapping[str, Any]:
        if rollup is None:
            raise InvalidMQLContextError("MetricQuery.rollup must not be None")

        granularity = ""
        if rollup.granularity is not None:
            granularity = str(rollup.granularity)

        interval = ""
        orderby = {"column_name": "", "direction": ""}
        with_totals = ""
        if rollup.interval:
            interval = str(rollup.interval)
            orderby = {"column_name": "time", "direction": "ASC"}
            if rollup.totals:
                with_totals = "{rollup.totals}"
        elif rollup.orderby is not None:
            orderby = {
                "column_name": AGGREGATE_ALIAS,
                "direction": rollup.orderby.value,
            }

        return {
            "orderby": orderby,
            "granularity": granularity,
            "interval": interval,
            "with_totals": with_totals,
        }

    def _visit_scope(self, scope: MetricsScope | None) -> str | Mapping[str, Any]:
        if scope is None:
            raise InvalidMQLContextError("MetricQuery.scope must not be None")

        return {
            "org_ids": scope.org_ids,
            "project_ids": scope.project_ids,
            "use_case_id": scope.use_case_id,
        }

    def _visit_limit(self, limit: Limit | None) -> str:
        if limit is not None:
            return str(limit.limit)
        return ""

    def _visit_offset(self, offset: Offset | None) -> str:
        if offset is not None:
            return str(offset.offset)
        return ""

    def _visit_indexer_mappings(
        self, indexer_mappings: dict[str, Any] | None
    ) -> dict[str, Any]:
        if not indexer_mappings:
            return {}
        return indexer_mappings


class Validator(MQLContextVisitor[None]):
    def _combine(
        self, query: main.MQLContext, returns: Mapping[str, None | Mapping[str, None]]
    ) -> Mapping[str, Any]:
        return {}

    def _visit_start(self, start: datetime | None) -> None:
        if start is None:
            raise InvalidMQLContextError("start is required for a MQL context")
        elif not isinstance(start, datetime):
            raise InvalidMQLContextError("start must be a datetime")

    def _visit_end(self, end: datetime | None) -> None:
        if end is None:
            raise InvalidMQLContextError("end is required for a MQL context")
        elif not isinstance(end, datetime):
            raise InvalidMQLContextError("end must be a datetime")

    def _visit_rollup(self, rollup: Rollup | None) -> Mapping[str, None]:
        if rollup is None:
            raise InvalidMQLContextError("rollup is required for a MQL context")
        elif not isinstance(rollup, Rollup):
            raise InvalidMQLContextError("rollup must be a Rollup object")

        # Since the granularity is inferred by the API, it can be initially None, but must be present when
        # the query is ultimately serialized and sent to Snuba.
        if rollup.granularity is None:
            raise InvalidMQLContextError("granularity must be set on the rollup")

        rollup.validate()
        return {}

    def _visit_scope(self, scope: MetricsScope | None) -> None:
        if scope is None:
            raise InvalidMQLContextError("scope is required for a MQL context")
        elif not isinstance(scope, MetricsScope):
            raise InvalidMQLContextError("scope must be a MetricsScope object")
        scope.validate()

    def _visit_limit(self, limit: Limit | None) -> None:
        if limit is None:
            return
        elif not isinstance(limit, Limit):
            raise InvalidMQLContextError("limit must be a Limit object")

        limit.validate()

    def _visit_offset(self, offset: Offset | None) -> None:
        if offset is None:
            return
        elif not isinstance(offset, Offset):
            raise InvalidMQLContextError("offset must be a Offset object")

        offset.validate()

    def _visit_indexer_mappings(
        self, indexer_mapping: Mapping[str, Any] | None
    ) -> Mapping[str, Any]:
        if indexer_mapping is None:
            return {}
        if not isinstance(indexer_mapping, dict):
            raise InvalidMQLContextError("indexer_mapping must be a dictionary")
        return {}

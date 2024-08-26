from __future__ import annotations

import json
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any

from snuba_sdk.expressions import Extrapolate, Limit, Offset
from snuba_sdk.formula import Formula
from snuba_sdk.metrics_query_visitors import Validator
from snuba_sdk.mql_visitor import MQLPrinter
from snuba_sdk.query import BaseQuery
from snuba_sdk.query_optimizers.or_optimizer import OrOptimizer
from snuba_sdk.query_visitors import InvalidQueryError
from snuba_sdk.timeseries import MetricsScope, Rollup, Timeseries


@dataclass
class MetricsQuery(BaseQuery):
    """
    A query on a set of timeseries. This class gets translated into a Snuba request string
    that returns a list of timeseries data. In order to allow this class to be built incrementally,
    it is not validated until it is serialized. Any specified filters or groupby fields are pushed
    down to each of the Timeseries in the query field. It is immutable, so any set functions return
    a new copy of the query, which also allows chaining calls.

    This class is distinct from the Query class to allow for more specific validation and to provide
    a simpler syntax for writing timeseries queries, which have fewer available features.
    """

    query: Timeseries | Formula | str | None = None
    start: datetime | None = None
    end: datetime | None = None
    rollup: Rollup | None = None
    scope: MetricsScope | None = None
    limit: Limit | None = None
    offset: Offset | None = None
    extrapolate: Extrapolate | None = None
    indexer_mappings: dict[str, str | int] | None = None

    def _replace(self, field: str, value: Any) -> MetricsQuery:
        new = replace(self, **{field: value})
        return new

    def set_query(self, query: Formula | Timeseries | str) -> MetricsQuery:
        if not isinstance(query, (Formula, Timeseries, str)):
            raise InvalidQueryError(
                "query must be a Formula or Timeseries or MQL string"
            )
        return self._replace("query", query)

    def set_start(self, start: datetime) -> MetricsQuery:
        if not isinstance(start, datetime):
            raise InvalidQueryError("start must be a datetime")
        return self._replace("start", start)

    def set_end(self, end: datetime) -> MetricsQuery:
        if not isinstance(end, datetime):
            raise InvalidQueryError("end must be a datetime")
        return self._replace("end", end)

    def set_rollup(self, rollup: Rollup) -> MetricsQuery:
        if not isinstance(rollup, Rollup):
            raise InvalidQueryError("rollup must be a Rollup")
        return self._replace("rollup", rollup)

    def set_scope(self, scope: MetricsScope) -> MetricsQuery:
        if not isinstance(scope, MetricsScope):
            raise InvalidQueryError("scope must be a MetricsScope")
        return self._replace("scope", scope)

    def set_limit(self, limit: int) -> MetricsQuery:
        return self._replace("limit", Limit(limit))

    def set_offset(self, offset: int) -> MetricsQuery:
        return self._replace("offset", Offset(offset))

    def set_extrapolate(self, extrapolate: bool) -> MetricsQuery:
        return self._replace("extrapolate", Extrapolate(extrapolate))

    def set_indexer_mappings(
        self, indexer_mappings: dict[str, str | int]
    ) -> MetricsQuery:
        return self._replace("indexer_mappings", indexer_mappings)

    def validate(self) -> None:
        Validator().visit(self)

    def __str__(self) -> str:
        result = MQL_PRINTER.visit(self)
        return json.dumps(result, indent=4)

    def print(self) -> str:
        self.validate()
        result = MQL_PRINTER.visit(self)
        return json.dumps(result, indent=4)

    def serialize(self) -> str | dict[str, Any]:
        self.validate()
        self._optimize()
        result = MQL_PRINTER.visit(self)
        return result

    def _optimize(self) -> None:
        if (
            isinstance(self.query, (Formula, Timeseries))
            and self.query.filters is not None
        ):
            new_filters = OrOptimizer().optimize(self.query.filters)
            if new_filters is not None:
                self.query = replace(self.query, filters=new_filters)


MQL_PRINTER = MQLPrinter()
VALIDATOR = Validator()

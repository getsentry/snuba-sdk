from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any

from snuba_sdk.formula import Formula
from snuba_sdk.metrics_query_visitors import SnQLPrinter, Validator
from snuba_sdk.query import BaseQuery
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

    query: Timeseries | Formula | None = None
    start: datetime | None = None
    end: datetime | None = None
    rollup: Rollup | None = None
    scope: MetricsScope | None = None

    def _replace(self, field: str, value: Any) -> MetricsQuery:
        new = replace(self, **{field: value})
        return new

    def set_query(self, query: Formula | Timeseries) -> MetricsQuery:
        if not isinstance(query, (Formula, Timeseries)):
            raise InvalidQueryError("query must be a Formula or Timeseries")
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

    def validate(self) -> None:
        Validator().visit(self)

    def __str__(self) -> str:
        return PRETTY_PRINTER.visit(self)

    def serialize(self) -> str:
        self.validate()
        return SNQL_PRINTER.visit(self)

    def print(self) -> str:
        self.validate()
        return PRETTY_PRINTER.visit(self)


SNQL_PRINTER = SnQLPrinter()
PRETTY_PRINTER = SnQLPrinter(pretty=True)
VALIDATOR = Validator()

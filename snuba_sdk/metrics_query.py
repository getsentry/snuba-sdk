from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any

from snuba_sdk.column import Column
from snuba_sdk.conditions import BooleanCondition, Condition, ConditionGroup
from snuba_sdk.expressions import list_type
from snuba_sdk.function import Function
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

    # TODO: This should support some kind of calculation. Simply using Function
    # causes import loop problems.
    query: Timeseries | None = None
    filters: ConditionGroup | None = None
    groupby: list[Column] | None = None
    start: datetime | None = None
    end: datetime | None = None
    rollup: Rollup | None = None
    scope: MetricsScope | None = None

    def _replace(self, field: str, value: Any) -> MetricsQuery:
        new = replace(self, **{field: value})
        return new

    def set_query(self, query: Function | Timeseries) -> MetricsQuery:
        if not isinstance(query, (Function, Timeseries)):
            raise InvalidQueryError("query must be a Function or Timeseries")
        return self._replace("query", query)

    def set_filters(self, filters: ConditionGroup) -> MetricsQuery:
        if not list_type(filters, (BooleanCondition, Condition)):
            raise InvalidQueryError("filters must be a list of Conditions")
        return self._replace("filters", filters)

    def set_groupby(self, groupby: list[Column]) -> MetricsQuery:
        if not list_type(groupby, (Column,)):
            raise InvalidQueryError("groupby must be a list of Columns")
        return self._replace("groupby", groupby)

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

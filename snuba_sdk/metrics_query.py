from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any

from snuba_sdk.column import Column
from snuba_sdk.conditions import Condition
from snuba_sdk.expressions import Expression, InvalidExpressionError, list_type
from snuba_sdk.function import Function
from snuba_sdk.orderby import Direction
from snuba_sdk.query import BaseQuery
from snuba_sdk.query_visitors import InvalidQueryError
from snuba_sdk.timeseries import Timeseries

ALLOWED_GRANULARITIES = (60, 3600, 86400)


@dataclass(frozen=True)
class Rollup(Expression):
    """
    Rollup instructs how the timeseries queries should be grouped on time. If the query is for a set of timeseries, then
    the interval field should be specified. It is the number of seconds to group the timeseries by.
    For a query that returns only the totals, specify Totals(True). A totals query can be ordered using the orderby field.
    """

    interval: int | None = None
    totals: bool | None = None
    orderby: Direction | None = None  # TODO: This doesn't make sense: ordered by what?

    def validate(self) -> None:
        if self.interval is not None:
            if not isinstance(self.interval, int):
                raise InvalidExpressionError(
                    f"interval must be an integer and one of {ALLOWED_GRANULARITIES}"
                )

            # TODO: this can allow more values once we support automatic granularity calculations
            if self.interval not in ALLOWED_GRANULARITIES:
                raise InvalidExpressionError(
                    f"interval {self.interval} must be one of {ALLOWED_GRANULARITIES}"
                )

        if self.interval is not None and self.totals is not None:
            raise InvalidExpressionError(
                "Only one of interval and totals can be set: Timeseries can't be rolled up by an interval and by a total"
            )

        if self.totals is not None:
            if not isinstance(self.totals, bool):
                raise InvalidExpressionError("totals must be a boolean")

        if self.orderby is not None:
            if not isinstance(self.orderby, Direction):
                raise InvalidExpressionError("orderby must be a Direction object")

        if self.totals is None and self.orderby is not None:
            raise InvalidExpressionError(
                "Metric queries can't be ordered without using totals"
            )

        if self.interval is None and not self.totals:
            raise InvalidExpressionError(
                "Rollup must have at least one of interval or totals"
            )


@dataclass
class MetricScope(Expression):
    """
    This contains all the meta information necessary to resolve a metric and to safely query
    the metrics dataset. All these values get automatically added to the query conditions.
    The idea of this class is to contain all the filter values that are not represented by
    tags in the API.

    use_case_id is treated separately since it can be derived separate from the MRIs of the
    metrics in the outer query.
    """

    org_ids: list[int]
    project_ids: list[int]
    use_case_id: int | None = None

    def validate(self) -> None:
        if not list_type(self.org_ids, (int,)):
            raise InvalidExpressionError("org_ids must be a list of integers")

        if not list_type(self.project_ids, (int,)):
            raise InvalidExpressionError("project_ids must be a list of integers")

        if self.use_case_id is not None and not isinstance(self.use_case_id, int):
            raise InvalidExpressionError("use_case_id must be an int")

    def set_use_case_id(self, use_case_id: int) -> MetricScope:
        if not isinstance(use_case_id, int):
            raise InvalidExpressionError("use_case_id must be an int")
        return replace(self, use_case_id=use_case_id)


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
    filters: list[Condition] | None = None
    groupby: list[Column] | None = None
    start: datetime | None = None
    end: datetime | None = None
    rollup: Rollup | None = None
    scope: MetricScope | None = None

    def _replace(self, field: str, value: Any) -> MetricsQuery:
        new = replace(self, **{field: value})
        return new

    def set_query(self, query: Function | Timeseries) -> MetricsQuery:
        if not isinstance(query, (Function, Timeseries)):
            raise InvalidQueryError("query must be a Function or Timeseries")
        return self._replace("query", query)

    def set_filters(self, filters: list[Condition]) -> MetricsQuery:
        if not list_type(filters, (Condition,)):
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

    def set_scope(self, scope: MetricScope) -> MetricsQuery:
        if not isinstance(scope, MetricScope):
            raise InvalidQueryError("scope must be a MetricScope")
        return self._replace("scope", scope)

    def validate(self) -> None:
        # TODO: Implement visitor
        raise InvalidQueryError("Not implemented")

    # TODO: implement a nicer version of this
    # def __str__(self) -> str:
    #     raise InvalidQueryError("Not implemented")

    # TODO: Implement a vistor for this
    def serialize(self) -> str:
        self.validate()
        raise InvalidQueryError("Not implemented")

    def print(self) -> str:
        self.validate()
        raise InvalidQueryError("Not implemented")

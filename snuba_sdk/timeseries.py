from __future__ import annotations

from dataclasses import dataclass, fields, replace
from typing import Any, Sequence

from snuba_sdk.column import Column
from snuba_sdk.conditions import Condition
from snuba_sdk.expressions import InvalidExpressionError, is_literal, list_type
from snuba_sdk.orderby import Direction


class InvalidTimeseriesError(Exception):
    pass


@dataclass(frozen=True)
class Metric:
    """
    Metric represents a raw metric that is being populated. It can be created with
    one of public name, mri or raw ID.
    """

    public_name: str | None = None
    mri: str | None = None
    id: int | None = None
    entity: str | None = None

    def __post_init__(self) -> None:
        self.validate()

    def get_fields(self) -> Sequence[str]:
        self_fields = fields(self)  # Verified the order in the Python source
        return tuple(f.name for f in self_fields)

    def validate(self) -> None:
        if self.public_name is not None and not isinstance(self.public_name, str):
            raise InvalidTimeseriesError("public_name must be a string")
        if self.mri is not None and not isinstance(self.mri, str):
            raise InvalidTimeseriesError("mri must be a string")
        if self.id is not None and not isinstance(self.id, int):
            raise InvalidTimeseriesError("id must be an integer")
        if self.entity is not None and not isinstance(self.entity, str):
            raise InvalidTimeseriesError("entity must be a string")

        if all(v is None for v in (self.public_name, self.mri, self.id)):
            raise InvalidTimeseriesError(
                "Metric must have at least one of public_name, mri or id"
            )

    def set_mri(self, mri: str) -> Metric:
        if not isinstance(mri, str):
            raise InvalidExpressionError("mri must be an str")
        return replace(self, mri=mri)

    def set_public_name(self, public_name: str) -> Metric:
        if not isinstance(public_name, str):
            raise InvalidExpressionError("public_name must be an str")
        return replace(self, public_name=public_name)

    def set_id(self, id: int) -> Metric:
        if not isinstance(id, int):
            raise InvalidExpressionError("id must be an int")
        return replace(self, id=id)

    def set_entity(self, entity: str) -> Metric:
        if not isinstance(entity, str):
            raise InvalidExpressionError("entity must be an str")
        return replace(self, entity=entity)


@dataclass
class Timeseries:
    """
    A code representation of a single timeseries. This is the basic unit of a metrics query.
    A raw metric, aggregated by an aggregate function. It can be filtered by tag conditions.
    It can also grouped by a set of tag values, which will return one timeseries for each unique
    grouping of tag values.
    """

    metric: Metric
    aggregate: str
    aggregate_params: list[Any] | None = None
    filters: list[Condition] | None = None
    groupby: list[Column] | None = None

    def __post_init__(self) -> None:
        self.validate()

    def get_fields(self) -> Sequence[str]:
        self_fields = fields(self)  # Verified the order in the Python source
        return tuple(f.name for f in self_fields)

    def validate(self) -> None:
        if not isinstance(self.metric, Metric):
            raise InvalidTimeseriesError("metric must be an instance of a Metric")
        self.metric.validate()

        # TODO: Restrict which specific aggregates are allowed
        # TODO: Validate aggregate_params based on the aggregate supplied e.g. quantile needs a float
        if not isinstance(self.aggregate, str):
            raise InvalidTimeseriesError("aggregate must be a string")
        if self.aggregate_params is not None:
            if not isinstance(self.aggregate_params, list):
                raise InvalidTimeseriesError("aggregate_params must be a list")
            for p in self.aggregate_params:
                if not is_literal(p):
                    raise InvalidTimeseriesError(
                        "aggregate_params can only be literal types"
                    )

        # TODO: Validate these are tag conditions only
        # TODO: Validate these are simple conditions e.g. tag[x] op literal
        if self.filters is not None:
            if not isinstance(self.filters, list):
                raise InvalidTimeseriesError("filters must be a list")
            for f in self.filters:
                if not isinstance(f, Condition):
                    raise InvalidTimeseriesError("filters must be a list of Conditions")

        # TODO: Can you group by meta information like project_id?
        # TODO: Validate these are appropriate columns for grouping
        if self.groupby is not None:
            if not isinstance(self.groupby, list):
                raise InvalidTimeseriesError("groupby must be a list")
            for g in self.groupby:
                if not isinstance(g, Column):
                    raise InvalidTimeseriesError("groupby must be a list of Columns")

    def set_metric(self, metric: Metric) -> Timeseries:
        if not isinstance(metric, Metric):
            raise InvalidTimeseriesError("metric must be a Metric")
        return replace(self, metric=metric)


ALLOWED_GRANULARITIES = (60, 3600, 86400)


@dataclass(frozen=True)
class Rollup:
    """
    Rollup instructs how the timeseries queries should be grouped on time. If the query is for a set of timeseries, then
    the interval field should be specified. It is the number of seconds to group the timeseries by.
    For a query that returns only the totals, specify Totals(True). A totals query can be ordered using the orderby field.
    """

    interval: int | None = None
    totals: bool | None = None
    orderby: Direction | None = None  # TODO: This doesn't make sense: ordered by what?

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        # TODO: This value is currently used directly to select a granularity
        # However, a totals query in theory shouldn't need to specify an interval
        # Ensure an interval is always set for now, and once we have code in place
        # to allow arbitrary intervals we can also add code to pick the appropriate
        # granularity based on the start/end interval of the query.
        if self.interval is None:
            raise InvalidExpressionError(
                f"interval must be an integer and one of {ALLOWED_GRANULARITIES}"
            )

        if not isinstance(self.interval, int):
            raise InvalidExpressionError(
                f"interval must be an integer and one of {ALLOWED_GRANULARITIES}"
            )

        # TODO: this can allow more values once we support automatic granularity calculations
        if self.interval not in ALLOWED_GRANULARITIES:
            raise InvalidExpressionError(
                f"interval {self.interval} must be one of {ALLOWED_GRANULARITIES}"
            )

        # TODO: Add this back once we can properly infer granularity. See comment above.
        # if self.interval is not None and self.totals is not None:
        #     raise InvalidExpressionError(
        #         "Only one of interval and totals can be set: Timeseries can't be rolled up by an interval and by a total"
        #     )

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

        # TODO: Add this back once we can properly infer granularity. See comment above.
        # if self.interval is None and not self.totals:
        #     raise InvalidExpressionError(
        #         "Rollup must have at least one of interval or totals"
        #     )


@dataclass
class MetricsScope:
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
    use_case_id: str | None = None

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        if not list_type(self.org_ids, (int,)):
            raise InvalidExpressionError("org_ids must be a list of integers")

        if not list_type(self.project_ids, (int,)):
            raise InvalidExpressionError("project_ids must be a list of integers")

        if self.use_case_id is not None and not isinstance(self.use_case_id, str):
            raise InvalidExpressionError("use_case_id must be an str")

    def set_use_case_id(self, use_case_id: str) -> MetricsScope:
        if not isinstance(use_case_id, str):
            raise InvalidExpressionError("use_case_id must be an str")
        return replace(self, use_case_id=use_case_id)

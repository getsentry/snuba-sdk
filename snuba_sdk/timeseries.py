from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any

from snuba_sdk.column import Column
from snuba_sdk.conditions import Condition
from snuba_sdk.expressions import Expression, InvalidExpressionError, is_literal


@dataclass(frozen=True)
class Metric(Expression):
    """
    Metric represents a raw metric that is being populated. It can be created with
    one of public name, mri or raw ID.
    """

    public_name: str | None = None
    mri: str | None = None
    id: int | None = None

    def validate(self) -> None:
        if not isinstance(self.public_name, str) and self.public_name is not None:
            raise InvalidExpressionError("public_name must be a string")
        if not isinstance(self.mri, str) and self.mri is not None:
            raise InvalidExpressionError("mri must be a string")
        if not isinstance(self.id, int) and self.id is not None:
            raise InvalidExpressionError("id must be an integer")

        if all(v is None for v in (self.public_name, self.mri, self.id)):
            raise InvalidExpressionError(
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


@dataclass
class Timeseries(Expression):
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

    def validate(self) -> None:
        if not isinstance(self.metric, Metric):
            raise InvalidExpressionError("metric must be an instance of a Metric")
        self.metric.validate()

        # TODO: Restrict which specific aggregates are allowed
        # TODO: Validate aggregate_params based on the aggregate supplied e.g. quantile needs a float
        if not isinstance(self.aggregate, str):
            raise InvalidExpressionError("aggregate must be a string")
        if self.aggregate_params is not None:
            if not isinstance(self.aggregate_params, list):
                raise InvalidExpressionError("aggregate_params must be a list")
            for p in self.aggregate_params:
                if not is_literal(p):
                    raise InvalidExpressionError(
                        "aggregate_params can only be simple types"
                    )

        # TODO: Validate these are tag conditions only
        # TODO: Validate these are simple conditions e.g. tag[x] op literal
        if self.filters is not None:
            if not isinstance(self.filters, list):
                raise InvalidExpressionError("filters must be a list")
            for f in self.filters:
                if not isinstance(f, Condition):
                    raise InvalidExpressionError("filters must be a list of Conditions")

        # TODO: Can you group by meta information like project_id?
        # TODO: Validate these are appropriate columns for grouping
        if self.groupby is not None:
            if not isinstance(self.groupby, list):
                raise InvalidExpressionError("groupby must be a list")
            for g in self.groupby:
                if not isinstance(g, Column):
                    raise InvalidExpressionError("groupby must be a list of Columns")

    def set_metric(self, metric: Metric) -> Timeseries:
        if not isinstance(metric, Metric):
            raise InvalidExpressionError("metric must be a Metric")
        return replace(self, metric=metric)

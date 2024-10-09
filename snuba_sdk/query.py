from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, fields, replace
from typing import Any, Optional, Sequence, Union

from snuba_sdk.aliased_expression import AliasedExpression
from snuba_sdk.column import Column
from snuba_sdk.conditions import BooleanCondition, Condition, ConditionGroup
from snuba_sdk.entity import Entity
from snuba_sdk.expressions import Granularity, Limit, Offset, Totals, list_type
from snuba_sdk.function import CurriedFunction, Function
from snuba_sdk.orderby import LimitBy, OrderBy
from snuba_sdk.query_visitors import InvalidQueryError, Printer, Validator
from snuba_sdk.relationships import Join
from snuba_sdk.storage import Storage

from snuba_sdk.query_optimizers.or_optimizer import OrOptimizer

VALIDATOR = Validator()


SelectableExpression = Union[AliasedExpression, Column, CurriedFunction, Function]
SelectableExpressionType: list[type] = [
    AliasedExpression,
    Column,
    CurriedFunction,
    Function,
]


class BaseQuery(ABC):
    """
    This base class is what the Request is aware of and interacts with. Any other type of query
    needs to provide these functions so the request can properly validate/serialize/print it.
    """

    @abstractmethod
    def validate(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def serialize(self) -> str | dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def print(self) -> str:
        raise NotImplementedError

    def get_fields(self) -> Sequence[str]:
        self_fields = fields(self)  # Verified the order in the Python source
        return tuple(f.name for f in self_fields)


@dataclass(frozen=True)
class Query(BaseQuery):
    """
    A code representation of a SnQL query. It is immutable, so any set functions
    return a new copy of the query. Unlike Expressions it is possible to
    instantiate a Query that is invalid. Any of the translation functions will
    validate the query before translating them, so the query must be valid before
    they are called.
    """

    # These must be listed in the order that they must appear in the SnQL query.
    match: Union[Entity, Storage, Join, Query]
    select: Optional[Sequence[SelectableExpression]] = None
    groupby: Optional[Sequence[SelectableExpression]] = None
    array_join: Optional[Sequence[Column]] = None
    where: Optional[ConditionGroup] = None
    having: Optional[ConditionGroup] = None
    orderby: Optional[Sequence[OrderBy]] = None
    limitby: Optional[LimitBy] = None
    limit: Optional[Limit] = None
    offset: Optional[Offset] = None
    granularity: Optional[Granularity] = None
    totals: Optional[Totals] = None

    def __post_init__(self) -> None:
        """
        This has a different validation flow from normal expressions, since a query
        is not necessarily always correct. For example, you can create a Query with
        no select columns, which will fail in the validate. However it shouldn't fail
        right away since the select columns can be added later.

        """
        if not isinstance(self.match, (Query, Join, Entity, Storage)):
            raise InvalidQueryError(
                "queries must have a valid Entity, Storage, Join or Query"
            )

        if isinstance(self.match, Query):
            try:
                self.match.validate()
            except Exception as e:
                raise InvalidQueryError(f"inner query is invalid: {e}") from e

    def _replace(self, field: str, value: Any) -> Query:
        new = replace(self, **{field: value})
        return new

    def set_match(self, match: Union[Entity, Storage, Join, Query]) -> Query:
        if not isinstance(match, (Entity, Join, Query)):
            raise InvalidQueryError(
                f"{match} must be a valid Entity, Storage, Join or Query"
            )
        elif isinstance(match, Query):
            try:
                match.validate()
            except Exception as e:
                raise InvalidQueryError(f"inner query is invalid: {e}") from e

        return self._replace("match", match)

    def set_select(self, select: Sequence[SelectableExpression]) -> Query:
        if not list_type(select, SelectableExpressionType) or not select:
            raise InvalidQueryError(
                "select clause must be a non-empty list of SelectableExpression"
            )
        return self._replace("select", select)

    def set_groupby(self, groupby: Sequence[SelectableExpression]) -> Query:
        if not list_type(groupby, SelectableExpressionType):
            raise InvalidQueryError(
                "groupby clause must be a list of SelectableExpression"
            )
        return self._replace("groupby", groupby)

    def set_array_join(self, array_join: Sequence[Column]) -> Query:
        if not list_type(array_join, [Column]) or len(array_join) < 1:
            raise InvalidQueryError("array join must be a non-empty list of Column")

        return self._replace("array_join", array_join)

    def set_where(self, conditions: ConditionGroup) -> Query:
        if not list_type(conditions, (BooleanCondition, Condition)):
            raise InvalidQueryError("where clause must be a list of conditions")
        return self._replace("where", conditions)

    def set_having(self, conditions: ConditionGroup) -> Query:
        if not list_type(conditions, (BooleanCondition, Condition)):
            raise InvalidQueryError("having clause must be a list of conditions")
        return self._replace("having", conditions)

    def set_orderby(self, orderby: Sequence[OrderBy]) -> Query:
        if not list_type(orderby, (OrderBy,)):
            raise InvalidQueryError("orderby clause must be a list of OrderBy")
        return self._replace("orderby", orderby)

    def set_limitby(self, limitby: LimitBy) -> Query:
        if not isinstance(limitby, LimitBy):
            raise InvalidQueryError("limitby clause must be a LimitBy")
        return self._replace("limitby", limitby)

    def set_limit(self, limit: int) -> Query:
        return self._replace("limit", Limit(limit))

    def set_offset(self, offset: int) -> Query:
        return self._replace("offset", Offset(offset))

    def set_granularity(self, granularity: int) -> Query:
        return self._replace("granularity", Granularity(granularity))

    def set_totals(self, totals: bool) -> Query:
        return self._replace("totals", Totals(totals))

    def validate(self) -> None:
        VALIDATOR.visit(self)

    def __str__(self) -> str:
        return self.serialize()

    def serialize(self) -> str:
        self.validate()
        optimized = self._optimize()
        return Printer().visit(optimized)

    def print(self) -> str:
        self.validate()
        return Printer(pretty=True).visit(self)

    def _optimize(self) -> Query:
        if self.where is not None:
            new_where = OrOptimizer().optimize(self.where)
            if new_where is not None:
                return replace(self, where=new_where)
        return self

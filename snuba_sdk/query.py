from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any, List, Optional, Sequence, Union

from snuba_sdk.conditions import Condition
from snuba_sdk.entity import Entity
from snuba_sdk.expressions import (
    Column,
    Function,
    Granularity,
    Limit,
    LimitBy,
    Offset,
    OrderBy,
    Totals,
)
from snuba_sdk.query_visitors import InvalidQuery, Printer, Translator, Validator


def list_type(vals: List[Any], type_classes: Sequence[Any]) -> bool:
    return isinstance(vals, list) and all(
        isinstance(v, tuple(type_classes)) for v in vals
    )


PRINTER = Printer()
PRETTY_PRINTER = Printer(pretty=True)
VALIDATOR = Validator()
TRANSLATOR = Translator()


@dataclass(frozen=True)
class Query:
    dataset: str
    match: Entity
    select: Optional[List[Union[Column, Function]]] = None
    groupby: Optional[List[Union[Column, Function]]] = None
    where: Optional[List[Condition]] = None
    having: Optional[List[Condition]] = None
    orderby: Optional[List[OrderBy]] = None
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
        # TODO: Whitelist of Datasets and possible entities
        if not isinstance(self.dataset, str) or self.dataset == "":
            raise InvalidQuery("queries must have a valid dataset")

        if not isinstance(self.match, Entity):
            raise InvalidQuery("queries must have a valid Entity")

    def _replace(self, field: str, value: Any) -> Query:
        new = replace(self, **{field: value})
        return new

    def set_match(self, match: Entity) -> Query:
        if not isinstance(match, Entity):
            raise InvalidQuery(f"{match} must be a valid Entity")
        return self._replace("match", match)

    def set_select(self, select: List[Union[Column, Function]]) -> Query:
        if not list_type(select, (Column, Function)) or not select:
            raise InvalidQuery(
                "select clause must be a non-empty list of Column and/or Function"
            )
        return self._replace("select", select)

    def set_groupby(self, groupby: List[Union[Column, Function]]) -> Query:
        if not list_type(groupby, (Column, Function)):
            raise InvalidQuery(
                "groupby clause must be a list of Column and/or Function"
            )
        return self._replace("groupby", groupby)

    def set_where(self, conditions: List[Condition]) -> Query:
        if not list_type(conditions, (Condition,)):
            raise InvalidQuery("where clause must be a list of Condition")
        return self._replace("where", conditions)

    def set_having(self, conditions: List[Condition]) -> Query:
        if not list_type(conditions, (Condition,)):
            raise InvalidQuery("having clause must be a list of Condition")
        return self._replace("having", conditions)

    def set_orderby(self, orderby: List[OrderBy]) -> Query:
        if not list_type(orderby, (OrderBy,)):
            raise InvalidQuery("orderby clause must be a list of OrderBy")
        return self._replace("orderby", orderby)

    def set_limitby(self, limitby: LimitBy) -> Query:
        if not isinstance(limitby, LimitBy):
            raise InvalidQuery("limitby clause must be a LimitBy")
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
        self.validate()
        return PRINTER.visit(self)

    def print(self) -> str:
        self.validate()
        return PRETTY_PRINTER.visit(self)

    def snuba(self) -> str:
        self.validate()
        return TRANSLATOR.visit(self)

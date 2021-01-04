from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any, List, Optional, Sequence, Union

from snuba_sdk.entity import Entity
from snuba_sdk.expressions import (
    Column,
    Function,
    Granularity,
    Limit,
    Offset,
)
from snuba_sdk.conditions import Condition


class InvalidQuery(Exception):
    pass


def list_type(vals: List[Any], type_classes: Sequence[Any]) -> bool:
    return not isinstance(vals, list) or not all(
        isinstance(v, tuple(type_classes)) for v in vals
    )


@dataclass(frozen=True)
class Query:
    dataset: str
    match: Entity
    select: Optional[List[Union[Column, Function]]] = None
    groupby: Optional[List[Union[Column, Function]]] = None
    where: Optional[List[Condition]] = None
    limit: Optional[Limit] = None
    offset: Optional[Offset] = None
    granularity: Optional[Granularity] = None

    def __post_init__(self) -> None:
        """
        This has a different validation flow from normal expressions, since a query
        is not necessarily always correct. For example, you can create a Query with
        no select columns, which will fail in the validate. However it shouldn't fail
        right away since the select columns can be added later.

        """
        if not isinstance(self.match, Entity):
            raise InvalidQuery("queries must have a valid Entity")

    def _replace(self, field: str, value: Any) -> Query:
        new = replace(self, **{field: value})
        new.validate()
        return new

    def set_match(self, match: Entity) -> Query:
        if not isinstance(match, Entity):
            raise InvalidQuery(f"{match} must be a valid Entity")
        return self._replace("match", match)

    def set_select(self, select: List[Union[Column, Function]]) -> Query:
        if not list_type(select, (Column, Function)):
            raise InvalidQuery("select clause must be a list of Column and/or Function")
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
        return self._replace("conditions", conditions)

    def set_limit(self, limit: int) -> Query:
        return self._replace("limit", limit)

    def set_offset(self, offset: int) -> Query:
        return self._replace("offset", Offset(offset))

    def set_granularity(self, granularity: int) -> Query:
        return self._replace("granularity", Granularity(granularity))

    def validate(self) -> None:
        # TODO: Contextual validation. E.g. top level functions in select
        # require aliases
        self.match.validate()
        clauses = [self.select, self.groupby, self.where]
        for exps in clauses:
            if exps is not None:
                for exp in exps:
                    exp.validate()

        self.limit.validate() if self.limit is not None else None
        self.offset.validate() if self.offset is not None else None
        self.granularity.validate() if self.granularity is not None else None

    def translate(self) -> str:
        self.validate()

        clauses = []
        clauses.append(f"MATCH {self.match.translate()}")
        if self.select is not None:
            clauses.append(f"SELECT {', '.join(s.translate() for s in self.select)}")

        if self.groupby is not None:
            clauses.append(f"BY {', '.join(g.translate() for g in self.groupby)}")

        if self.where is not None:
            clauses.append(f"WHERE {', '.join(w.translate() for w in self.where)}")

        if self.limit is not None:
            clauses.append(f"LIMIT {self.limit.translate()}")

        if self.offset is not None:
            clauses.append(f"OFFSET {self.offset.translate()}")

        if self.granularity is not None:
            clauses.append(f"GRANULARITY {self.granularity.translate()}")

        return "\n".join(clauses)

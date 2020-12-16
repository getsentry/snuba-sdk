from __future__ import annotations

from dataclasses import dataclass, replace
from typing import List, Optional, Union

from snuba_sdk import Expression
from snuba_sdk.entity import Entity
from snuba_sdk.expressions import (
    Column,
    Condition,
    Function,
    Granularity,
    Limit,
    Offset,
)


@dataclass(frozen=True)
class Query(Expression):
    dataset: str
    match: Entity
    select: Optional[List[Union[Column, Function]]] = None
    groupby: Optional[List[Union[Column, Function]]] = None
    where: Optional[List[Condition]] = None
    limit: Optional[Limit] = None
    offset: Optional[Offset] = None
    granularity: Optional[Granularity] = None

    def set_match(self, match: Entity) -> Query:
        new = replace(self, match=match)
        new.validate()
        return new

    def set_select(self, select: List[Union[Column, Function]]) -> Query:
        new = replace(self, select=select)
        new.validate()
        return new

    def set_groupby(self, groupby: List[Union[Column, Function]]) -> Query:
        new = replace(self, groupby=groupby)
        new.validate()
        return new

    def set_where(self, conditions: List[Condition]) -> Query:
        new = replace(self, conditions=conditions)
        new.validate()
        return new

    def set_limit(self, limit: int) -> Query:
        new = replace(self, limit=Limit(limit))
        new.validate()
        return new

    def set_offset(self, offset: int) -> Query:
        new = replace(self, offset=Offset(offset))
        new.validate()
        return new

    def set_granularity(self, granularity: int) -> Query:
        new = replace(self, granularity=Granularity(granularity))
        new.validate()
        return new

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

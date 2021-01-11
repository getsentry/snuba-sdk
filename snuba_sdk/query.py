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
    Offset,
)
from snuba_sdk.visitors import Translation


class InvalidQuery(Exception):
    pass


def list_type(vals: List[Any], type_classes: Sequence[Any]) -> bool:
    return isinstance(vals, list) and all(
        isinstance(v, tuple(type_classes)) for v in vals
    )


TRANSLATOR = Translation()


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

    def set_limit(self, limit: int) -> Query:
        return self._replace("limit", Limit(limit))

    def set_offset(self, offset: int) -> Query:
        return self._replace("offset", Offset(offset))

    def set_granularity(self, granularity: int) -> Query:
        return self._replace("granularity", Granularity(granularity))

    def validate(self) -> None:
        # TODO: Contextual validations:
        # - Must have certain conditions (project, timestamp, organization etc.)

        if not self.select:
            raise InvalidQuery("query must have at least one column in select")

        # Top level functions in the select clause must have an alias
        non_aggregates = []
        has_aggregates = False
        for exp in self.select:
            if isinstance(exp, Function) and not exp.alias:
                raise InvalidQuery(
                    f"{TRANSLATOR.visit(exp)} must have an alias in the select"
                )

            if not isinstance(exp, Function) or not exp.is_aggregate():
                non_aggregates.append(exp)
            else:
                has_aggregates = True

        # Non-aggregate expressions must be in the groupby if there is an aggregate
        if has_aggregates and len(non_aggregates) > 0:
            if not self.groupby or len(self.groupby) == 0:
                raise InvalidQuery(
                    "groupby must be included if there are aggregations in the select"
                )

            for group_exp in non_aggregates:
                if group_exp not in self.groupby:
                    raise InvalidQuery(
                        f"{TRANSLATOR.visit(group_exp)} missing from the groupby"
                    )

        # TODO - It's not clear if this is worth doing. Each of these components was validated
        # when they were created, so do we need to validate again? It is possible for someone
        # to override a component with something invalid, but if there is only so much
        # idiot proofing that can be done with Python.
        try:
            self.match.validate()
            clauses = [self.select, self.groupby, self.where]
            for exps in clauses:
                if exps is not None:
                    for expr in exps:
                        expr.validate()

            self.limit.validate() if self.limit is not None else None
            self.offset.validate() if self.offset is not None else None
            self.granularity.validate() if self.granularity is not None else None
        except Exception as e:
            raise InvalidQuery(f"invalid query: {str(e)}")

    def translate(self) -> str:
        self.validate()

        clauses = []
        clauses.append(f"MATCH {TRANSLATOR.visit(self.match)}")
        if self.select is not None:
            clauses.append(
                f"SELECT {', '.join(TRANSLATOR.visit(s) for s in self.select)}"
            )

        if self.groupby is not None:
            clauses.append(f"BY {', '.join(TRANSLATOR.visit(g) for g in self.groupby)}")

        if self.where is not None:
            clauses.append(
                f"WHERE {' AND '.join(TRANSLATOR.visit(w) for w in self.where)}"
            )

        if self.limit is not None:
            clauses.append(f"LIMIT {TRANSLATOR.visit(self.limit)}")

        if self.offset is not None:
            clauses.append(f"OFFSET {TRANSLATOR.visit(self.offset)}")

        if self.granularity is not None:
            clauses.append(f"GRANULARITY {TRANSLATOR.visit(self.granularity)}")

        return " ".join(clauses)

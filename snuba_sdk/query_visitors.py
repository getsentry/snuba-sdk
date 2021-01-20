from __future__ import annotations

import json
from abc import ABC, abstractmethod
from typing import (
    Generic,
    List,
    Mapping,
    Optional,
    TypeVar,
    TYPE_CHECKING,
    Union,
)

from snuba_sdk.entity import Entity
from snuba_sdk.conditions import Condition
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
from snuba_sdk.visitors import Translation

if TYPE_CHECKING:
    from snuba_sdk.query import Query


class InvalidQuery(Exception):
    pass


QVisited = TypeVar("QVisited")


class QueryVisitor(ABC, Generic[QVisited]):
    def visit(self, query: Query) -> QVisited:
        fields = query.get_fields()
        returns = {}
        for field in fields:
            returns[field] = getattr(self, f"_visit_{field}")(getattr(query, field))

        return self._combine(query, returns)

    @abstractmethod
    def _combine(self, query: Query, returns: Mapping[str, QVisited]) -> QVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_dataset(self, dataset: str) -> QVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_match(self, match: Entity) -> QVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_select(
        self, select: Optional[List[Union[Column, Function]]]
    ) -> QVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_groupby(
        self, groupby: Optional[List[Union[Column, Function]]]
    ) -> QVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_where(self, where: Optional[List[Condition]]) -> QVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_having(self, having: Optional[List[Condition]]) -> QVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_orderby(self, orderby: Optional[List[OrderBy]]) -> QVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_limitby(self, limitby: Optional[LimitBy]) -> QVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_limit(self, limit: Optional[Limit]) -> QVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_offset(self, offset: Optional[Offset]) -> QVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_granularity(self, granularity: Optional[Granularity]) -> QVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_totals(self, granularity: Optional[Totals]) -> QVisited:
        raise NotImplementedError


class Printer(QueryVisitor[str]):
    def __init__(self, pretty: bool = False) -> None:
        self.translator = Translation()
        self.pretty = pretty

    def _combine(self, query: Query, returns: Mapping[str, str]) -> str:
        clauses = query.get_fields()[1:]  # Ignore dataset for now
        separator = "\n" if self.pretty else " "
        formatted = separator.join([returns[c] for c in clauses if returns[c]])
        if self.pretty:
            formatted = f"-- DATASET: {returns['dataset']}\n{formatted}"

        return formatted

    def _visit_dataset(self, dataset: str) -> str:
        return dataset

    def _visit_match(self, match: Entity) -> str:
        return f"MATCH {self.translator.visit(match)}"

    def _visit_select(self, select: Optional[List[Union[Column, Function]]]) -> str:
        if select:
            return f"SELECT {', '.join(self.translator.visit(s) for s in select)}"
        return ""

    def _visit_groupby(self, groupby: Optional[List[Union[Column, Function]]]) -> str:
        if groupby:
            return f"BY {', '.join(self.translator.visit(g) for g in groupby)}"
        return ""

    def _visit_where(self, where: Optional[List[Condition]]) -> str:
        if where:
            return f"WHERE {' AND '.join(self.translator.visit(w) for w in where)}"
        return ""

    def _visit_having(self, having: Optional[List[Condition]]) -> str:
        if having:
            return f"HAVING {' AND '.join(self.translator.visit(h) for h in having)}"
        return ""

    def _visit_orderby(self, orderby: Optional[List[OrderBy]]) -> str:
        if orderby:
            return f"ORDER BY {', '.join(self.translator.visit(o) for o in orderby)}"
        return ""

    def _visit_limitby(self, limitby: Optional[LimitBy]) -> str:
        if limitby is not None:
            return f"LIMIT {self.translator.visit(limitby)}"
        return ""

    def _visit_limit(self, limit: Optional[Limit]) -> str:
        if limit is not None:
            return f"LIMIT {self.translator.visit(limit)}"
        return ""

    def _visit_offset(self, offset: Optional[Offset]) -> str:
        if offset is not None:
            return f"OFFSET {self.translator.visit(offset)}"
        return ""

    def _visit_granularity(self, granularity: Optional[Granularity]) -> str:
        if granularity is not None:
            return f"GRANULARITY {self.translator.visit(granularity)}"
        return ""

    def _visit_totals(self, totals: Optional[Totals]) -> str:
        if totals is not None:
            return f"TOTALS {self.translator.visit(totals)}"
        return ""


class Translator(Printer):
    def __init__(self) -> None:
        super().__init__(False)

    def _combine(self, query: Query, returns: Mapping[str, str]) -> str:
        formatted_query = super()._combine(query, returns)
        body = {"dataset": query.dataset, "query": formatted_query}

        return json.dumps(body)


class Validator(QueryVisitor[None]):
    def _combine(self, query: Query, returns: Mapping[str, None]) -> None:
        # TODO: Contextual validations:
        # - Must have certain conditions (project, timestamp, organization etc.)

        if query.select is None or len(query.select) == 0:
            raise InvalidQuery("query must have at least one column in select")

        # - limit by must be a field in select
        if query.limitby is not None:
            found = False
            for s in query.select:
                if s == query.limitby.column:
                    found = True
                    break

            if not found:
                raise InvalidQuery(
                    f"{query.limitby.column} in limitby clause is missing from select clause"
                )

        # Top level functions in the select clause must have an alias
        non_aggregates = []
        has_aggregates = False
        for exp in query.select:
            if isinstance(exp, Function) and not exp.alias:
                raise InvalidQuery(f"{exp} must have an alias in the select")

            if not isinstance(exp, Function) or not exp.is_aggregate():
                non_aggregates.append(exp)
            else:
                has_aggregates = True

        # Non-aggregate expressions must be in the groupby if there is an aggregate
        if has_aggregates and len(non_aggregates) > 0:
            if not query.groupby or len(query.groupby) == 0:
                raise InvalidQuery(
                    "groupby must be included if there are aggregations in the select"
                )

            for group_exp in non_aggregates:
                if group_exp not in query.groupby:
                    raise InvalidQuery(f"{group_exp} missing from the groupby")

    def _visit_dataset(self, dataset: str) -> None:
        pass

    def _visit_match(self, match: Entity) -> None:
        match.validate()

    def _visit_select(self, select: Optional[List[Union[Column, Function]]]) -> None:
        if select is not None:
            for s in select:
                s.validate()

    def _visit_groupby(self, groupby: Optional[List[Union[Column, Function]]]) -> None:
        if groupby is not None:
            for g in groupby:
                g.validate()

    def _visit_where(self, where: Optional[List[Condition]]) -> None:
        if where is not None:
            for w in where:
                w.validate()

    def _visit_having(self, having: Optional[List[Condition]]) -> None:
        if having is not None:
            for h in having:
                h.validate()

    def _visit_orderby(self, orderby: Optional[List[OrderBy]]) -> None:
        if orderby is not None:
            for o in orderby:
                o.validate()

    def _visit_limitby(self, limitby: Optional[LimitBy]) -> None:
        if limitby is not None:
            limitby.validate()

    def _visit_limit(self, limit: Optional[Limit]) -> None:
        if limit is not None:
            limit.validate()

    def _visit_offset(self, offset: Optional[Offset]) -> None:
        if offset is not None:
            offset.validate()

    def _visit_granularity(self, granularity: Optional[Granularity]) -> None:
        if granularity is not None:
            granularity.validate()

    def _visit_totals(self, totals: Optional[Totals]) -> None:
        if totals is not None:
            totals.validate()

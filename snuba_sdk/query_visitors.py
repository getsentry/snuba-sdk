from __future__ import annotations

import json
from abc import ABC, abstractmethod
from typing import (
    Any,
    Generic,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Set,
    TypeVar,
    Union,
)

# Import the module due to sphinx autodoc problems
# https://github.com/agronholm/sphinx-autodoc-typehints#dealing-with-circular-imports
from snuba_sdk import query as main
from snuba_sdk.column import Column
from snuba_sdk.conditions import ConditionGroup
from snuba_sdk.entity import Entity
from snuba_sdk.expressions import (
    Consistent,
    Debug,
    DryRun,
    Expression,
    Granularity,
    Legacy,
    Limit,
    Offset,
    ParentAPI,
    Totals,
    Turbo,
)
from snuba_sdk.function import CurriedFunction, Function
from snuba_sdk.orderby import LimitBy, OrderBy
from snuba_sdk.query_validation import validate_match
from snuba_sdk.relationships import Join
from snuba_sdk.snuba import is_aggregation_function
from snuba_sdk.visitors import ExpressionFinder, Translation, entity_aliases


class InvalidQueryError(Exception):
    pass


QVisited = TypeVar("QVisited")


def is_aggregate(
    function: Union[Function, CurriedFunction],
    aggregate_aliases: Optional[set[str]] = None,
) -> bool:
    if is_aggregation_function(function.function, aggregate_aliases):
        return True

    if function.parameters is not None:
        for param in function.parameters:
            if (
                aggregate_aliases
                and isinstance(param, Column)
                and param.name in aggregate_aliases
            ):
                return True
            elif isinstance(param, (CurriedFunction, Function)) and is_aggregate(
                param, aggregate_aliases
            ):
                return True

    return False


def find_column_in_function(
    column: Column, function: Union[Function, CurriedFunction]
) -> bool:
    if function.parameters is not None:
        for param in function.parameters:
            if param == column:
                return True
            elif isinstance(
                param, (Function, CurriedFunction)
            ) and find_column_in_function(column, param):
                return True

    return False


class QueryVisitor(ABC, Generic[QVisited]):
    def visit(self, query: main.Query) -> QVisited:
        fields = query.get_fields()
        returns = {}
        for field in fields:
            returns[field] = getattr(self, f"_visit_{field}")(getattr(query, field))

        return self._combine(query, returns)

    @abstractmethod
    def _combine(self, query: main.Query, returns: Mapping[str, QVisited]) -> QVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_dataset(self, dataset: str) -> QVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_match(self, match: Union[Entity, Join, main.Query]) -> QVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_select(
        self, select: Optional[Sequence[main.SelectableExpression]]
    ) -> QVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_groupby(
        self, groupby: Optional[Sequence[main.SelectableExpression]]
    ) -> QVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_array_join(self, array_join: Optional[Sequence[Column]]) -> QVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_where(self, where: Optional[ConditionGroup]) -> QVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_having(self, having: Optional[ConditionGroup]) -> QVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_orderby(self, orderby: Optional[Sequence[OrderBy]]) -> QVisited:
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
    def _visit_parent_api(self, parent_api: ParentAPI) -> QVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_totals(self, totals: Totals) -> QVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_consistent(self, consistent: Consistent) -> QVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_turbo(self, turbo: Turbo) -> QVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_debug(self, debug: Debug) -> QVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_dry_run(self, dry_run: DryRun) -> QVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_legacy(self, legacy: Legacy) -> QVisited:
        raise NotImplementedError


class Printer(QueryVisitor[str]):
    def __init__(self, pretty: bool = False, is_inner: bool = False) -> None:
        self.translator = Translation()
        self.pretty = pretty
        self.is_inner = is_inner

    def visit(self, query: main.Query) -> str:
        if isinstance(query.match, Join):
            with entity_aliases(self.translator):
                return super().visit(query)

        return super().visit(query)

    def _combine(self, query: main.Query, returns: Mapping[str, str]) -> str:
        clause_order = query.get_fields()
        # These fields are encoded outside of the SQL
        to_skip = (
            "dataset",
            "consistent",
            "turbo",
            "debug",
            "dry_run",
            "legacy",
            "parent_api",
        )

        separator = "\n" if (self.pretty and not self.is_inner) else " "
        formatted = separator.join(
            [returns[c] for c in clause_order if c not in to_skip and returns[c]]
        )

        if self.pretty and not self.is_inner:
            prefix = ""
            for skip in to_skip:
                if returns.get(skip):
                    prefix += f"-- {skip.upper()}: {returns[skip]}\n"
            formatted = f"{prefix}{formatted}"

        return formatted

    def _visit_dataset(self, dataset: str) -> str:
        return dataset

    def _visit_match(self, match: Union[Entity, Join, main.Query]) -> str:
        if isinstance(match, (Entity, Join)):
            return f"MATCH {self.translator.visit(match)}"

        # We need a separate translator that can recurse through the subqueries
        # with different settings.
        translator = Printer(self.pretty, True)
        subquery = translator.visit(match)
        return "MATCH { %s }" % subquery

    def _visit_select(
        self, select: Optional[Sequence[main.SelectableExpression]]
    ) -> str:
        if select:
            return f"SELECT {', '.join(self.translator.visit(s) for s in select)}"
        return ""

    def _visit_groupby(
        self, groupby: Optional[Sequence[main.SelectableExpression]]
    ) -> str:
        if groupby:
            return f"BY {', '.join(self.translator.visit(g) for g in groupby)}"
        return ""

    def _visit_array_join(self, array_join: Optional[Sequence[Column]]) -> str:
        if array_join:
            return f"ARRAY JOIN {', '.join(self.translator.visit(col) for col in array_join)}"
        return ""

    def _visit_where(self, where: Optional[ConditionGroup]) -> str:
        if where:
            return f"WHERE {' AND '.join(self.translator.visit(w) for w in where)}"
        return ""

    def _visit_having(self, having: Optional[ConditionGroup]) -> str:
        if having:
            return f"HAVING {' AND '.join(self.translator.visit(h) for h in having)}"
        return ""

    def _visit_orderby(self, orderby: Optional[Sequence[OrderBy]]) -> str:
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

    def _visit_parent_api(self, parent_api: Optional[ParentAPI]) -> str:
        return parent_api.name if parent_api is not None else ""

    def _visit_totals(self, totals: Totals) -> str:
        if totals:
            return f"TOTALS {self.translator.visit(totals)}"
        return ""

    def _visit_consistent(self, consistent: Consistent) -> str:
        return str(consistent) if consistent else ""

    def _visit_turbo(self, turbo: Turbo) -> str:
        return str(turbo) if turbo else ""

    def _visit_debug(self, debug: Debug) -> str:
        return str(debug) if debug else ""

    def _visit_dry_run(self, dry_run: DryRun) -> str:
        return str(dry_run) if dry_run else ""

    def _visit_legacy(self, legacy: Legacy) -> str:
        return str(legacy) if legacy else ""


class Translator(Printer):
    def __init__(self) -> None:
        super().__init__(False)

    def _combine(self, query: main.Query, returns: Mapping[str, str]) -> str:
        formatted_query = super()._combine(query, returns)
        if self.is_inner:
            return formatted_query

        body: MutableMapping[str, Union[str, bool]] = {
            "dataset": query.dataset,
            "query": formatted_query,
        }
        if query.consistent:
            body["consistent"] = query.consistent.value
        if query.turbo:
            body["turbo"] = query.turbo.value
        if query.debug:
            body["debug"] = query.debug.value
        if query.dry_run:
            body["dry_run"] = query.dry_run.value
        if query.legacy:
            body["legacy"] = query.legacy.value
        if query.parent_api:
            body["parent_api"] = query.parent_api.name

        return json.dumps(body)


class ExpressionSearcher(QueryVisitor[Set[Expression]]):
    def __init__(self, exp_type: Any) -> None:
        self.expression_finder = ExpressionFinder(exp_type)

    def _combine(
        self, query: main.Query, returns: Mapping[str, set[Expression]]
    ) -> set[Expression]:
        found = set()
        for ret in returns.values():
            found |= ret
        return found

    def _visit_dataset(self, dataset: str) -> set[Expression]:
        return set()

    def _visit_match(self, match: Union[Entity, Join, main.Query]) -> set[Expression]:
        if isinstance(match, (Entity, Join)):
            return self.expression_finder.visit(match)
        return set()

    def __aggregate(self, terms: Optional[Sequence[Expression]]) -> set[Expression]:
        found = set()
        if terms:
            for t in terms:
                found |= self.expression_finder.visit(t)
        return found

    def _visit_select(
        self, select: Optional[Sequence[main.SelectableExpression]]
    ) -> set[Expression]:
        return self.__aggregate(select)

    def _visit_groupby(
        self, groupby: Optional[Sequence[main.SelectableExpression]]
    ) -> set[Expression]:
        return self.__aggregate(groupby)

    def _visit_array_join(
        self, array_join: Optional[Sequence[Column]]
    ) -> set[Expression]:
        return self.__aggregate(array_join)

    def _visit_where(self, where: Optional[ConditionGroup]) -> set[Expression]:
        return self.__aggregate(where)

    def _visit_having(self, having: Optional[ConditionGroup]) -> set[Expression]:
        return self.__aggregate(having)

    def _visit_orderby(self, orderby: Optional[Sequence[OrderBy]]) -> set[Expression]:
        return self.__aggregate(orderby)

    def _visit_limitby(self, limitby: Optional[LimitBy]) -> set[Expression]:
        return self.expression_finder.visit(limitby) if limitby else set()

    def _visit_limit(self, limit: Optional[Limit]) -> set[Expression]:
        return self.expression_finder.visit(limit) if limit else set()

    def _visit_offset(self, offset: Optional[Offset]) -> set[Expression]:
        return self.expression_finder.visit(offset) if offset else set()

    def _visit_granularity(self, granularity: Optional[Granularity]) -> set[Expression]:
        return self.expression_finder.visit(granularity) if granularity else set()

    def _visit_parent_api(self, parent_api: Optional[ParentAPI]) -> set[Expression]:
        return self.expression_finder.visit(parent_api) if parent_api else set()

    def _visit_totals(self, totals: Totals) -> set[Expression]:
        return self.expression_finder.visit(totals) if totals else set()

    def _visit_consistent(self, consistent: Consistent) -> set[Expression]:
        return self.expression_finder.visit(consistent) if consistent else set()

    def _visit_turbo(self, turbo: Turbo) -> set[Expression]:
        return self.expression_finder.visit(turbo) if turbo else set()

    def _visit_debug(self, debug: Debug) -> set[Expression]:
        return self.expression_finder.visit(debug) if debug else set()

    def _visit_dry_run(self, dry_run: DryRun) -> set[Expression]:
        return self.expression_finder.visit(dry_run) if dry_run else set()

    def _visit_legacy(self, legacy: Legacy) -> set[Expression]:
        return self.expression_finder.visit(legacy) if legacy else set()


class Validator(QueryVisitor[None]):
    def __init__(self) -> None:
        super().__init__()
        self.column_finder = ExpressionSearcher(Column)

    def _combine(self, query: main.Query, returns: Mapping[str, None]) -> None:
        validate_match(query, self.column_finder)

        if query.select is None or len(query.select) == 0:
            raise InvalidQueryError("query must have at least one expression in select")

        if query.totals and not query.groupby:
            raise InvalidQueryError("totals is only valid with a groupby")

    def _visit_dataset(self, dataset: str) -> None:
        pass

    def _visit_match(self, match: Union[Entity, Join, main.Query]) -> None:
        match.validate()

    def __list_validate(self, values: Optional[Sequence[Expression]]) -> None:
        if values is not None:
            for v in values:
                v.validate()

    def _visit_select(
        self, select: Optional[Sequence[main.SelectableExpression]]
    ) -> None:
        self.__list_validate(select)

    def _visit_groupby(
        self, groupby: Optional[Sequence[main.SelectableExpression]]
    ) -> None:
        self.__list_validate(groupby)

    def _visit_array_join(self, array_join: Optional[Sequence[Column]]) -> None:
        self.__list_validate(array_join)

    def _visit_where(self, where: Optional[ConditionGroup]) -> None:
        self.__list_validate(where)

    def _visit_having(self, having: Optional[ConditionGroup]) -> None:
        self.__list_validate(having)

    def _visit_orderby(self, orderby: Optional[Sequence[OrderBy]]) -> None:
        self.__list_validate(orderby)

    def _visit_limitby(self, limitby: Optional[LimitBy]) -> None:
        limitby.validate() if limitby is not None else None

    def _visit_limit(self, limit: Optional[Limit]) -> None:
        limit.validate() if limit is not None else None

    def _visit_offset(self, offset: Optional[Offset]) -> None:
        offset.validate() if offset is not None else None

    def _visit_granularity(self, granularity: Optional[Granularity]) -> None:
        granularity.validate() if granularity is not None else None

    def _visit_parent_api(self, parent_api: Optional[ParentAPI]) -> None:
        parent_api.validate() if parent_api is not None else None

    def _visit_totals(self, totals: Totals) -> None:
        totals.validate()

    def _visit_consistent(self, consistent: Consistent) -> None:
        consistent.validate()

    def _visit_turbo(self, turbo: Turbo) -> None:
        turbo.validate()

    def _visit_debug(self, debug: Debug) -> None:
        debug.validate()

    def _visit_dry_run(self, dry_run: DryRun) -> None:
        dry_run.validate()

    def _visit_legacy(self, legacy: Legacy) -> None:
        legacy.validate()

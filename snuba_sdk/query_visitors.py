import json
from abc import ABC, abstractmethod
from typing import (
    Generic,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    TYPE_CHECKING,
    TypeVar,
    Union,
)

from snuba_sdk.entity import Entity
from snuba_sdk.conditions import BooleanCondition, Condition
from snuba_sdk.expressions import (
    Column,
    Consistent,
    CurriedFunction,
    Debug,
    Expression,
    Function,
    Granularity,
    Limit,
    LimitBy,
    Offset,
    OrderBy,
    Totals,
    Turbo,
)
from snuba_sdk.visitors import Translation

if TYPE_CHECKING:
    # Import the module due to sphinx autodoc problems
    # https://github.com/agronholm/sphinx-autodoc-typehints#dealing-with-circular-imports
    from snuba_sdk import query


class InvalidQuery(Exception):
    pass


QVisited = TypeVar("QVisited")


class QueryVisitor(ABC, Generic[QVisited]):
    def visit(self, query: "query.Query") -> QVisited:
        fields = query.get_fields()
        returns = {}
        for field in fields:
            returns[field] = getattr(self, f"_visit_{field}")(getattr(query, field))

        return self._combine(query, returns)

    @abstractmethod
    def _combine(
        self, query: "query.Query", returns: Mapping[str, QVisited]
    ) -> QVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_dataset(self, dataset: str) -> QVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_match(self, match: Entity) -> QVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_select(
        self, select: Optional[Sequence[Union[Column, CurriedFunction, Function]]]
    ) -> QVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_groupby(
        self, groupby: Optional[Sequence[Union[Column, CurriedFunction, Function]]]
    ) -> QVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_where(
        self, where: Optional[Sequence[Union[BooleanCondition, Condition]]]
    ) -> QVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_having(
        self, having: Optional[Sequence[Union[BooleanCondition, Condition]]]
    ) -> QVisited:
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


class Printer(QueryVisitor[str]):
    def __init__(self, pretty: bool = False) -> None:
        self.translator = Translation()
        self.pretty = pretty

    def _combine(self, query: "query.Query", returns: Mapping[str, str]) -> str:
        clause_order = query.get_fields()
        # These fields are encoded outside of the SQL
        to_skip = ("dataset", "consistent", "turbo", "debug")

        separator = "\n" if self.pretty else " "
        formatted = separator.join(
            [returns[c] for c in clause_order if c not in to_skip and returns[c]]
        )
        if self.pretty:
            prefix = ""
            for skip in to_skip:
                if returns.get(skip):
                    prefix += f"-- {skip.upper()}: {returns[skip]}\n"
            formatted = f"{prefix}{formatted}"

        return formatted

    def _visit_dataset(self, dataset: str) -> str:
        return dataset

    def _visit_match(self, match: Entity) -> str:
        return f"MATCH {self.translator.visit(match)}"

    def _visit_select(
        self, select: Optional[Sequence[Union[Column, CurriedFunction, Function]]]
    ) -> str:
        if select:
            return f"SELECT {', '.join(self.translator.visit(s) for s in select)}"
        return ""

    def _visit_groupby(
        self, groupby: Optional[Sequence[Union[Column, CurriedFunction, Function]]]
    ) -> str:
        if groupby:
            return f"BY {', '.join(self.translator.visit(g) for g in groupby)}"
        return ""

    def _visit_where(
        self, where: Optional[Sequence[Union[BooleanCondition, Condition]]]
    ) -> str:
        if where:
            return f"WHERE {' AND '.join(self.translator.visit(w) for w in where)}"
        return ""

    def _visit_having(
        self, having: Optional[Sequence[Union[BooleanCondition, Condition]]]
    ) -> str:
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


class Translator(Printer):
    def __init__(self) -> None:
        super().__init__(False)

    def _combine(self, query: "query.Query", returns: Mapping[str, str]) -> str:
        formatted_query = super()._combine(query, returns)
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

        return json.dumps(body)


class Validator(QueryVisitor[None]):
    def _combine(self, query: "query.Query", returns: Mapping[str, None]) -> None:
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
            if isinstance(exp, (CurriedFunction, Function)) and not exp.alias:
                raise InvalidQuery(f"{exp} must have an alias in the select")

            if (
                not isinstance(exp, (CurriedFunction, Function))
                or not exp.is_aggregate()
            ):
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

        if query.totals and not query.groupby:
            raise InvalidQuery("totals is only valid with a groupby")

    def _visit_dataset(self, dataset: str) -> None:
        pass

    def _visit_match(self, match: Entity) -> None:
        match.validate()

    def __list_validate(self, values: Optional[Sequence[Expression]]) -> None:
        if values is not None:
            for v in values:
                v.validate()

    def _visit_select(
        self, select: Optional[Sequence[Union[Column, CurriedFunction, Function]]]
    ) -> None:
        self.__list_validate(select)

    def _visit_groupby(
        self, groupby: Optional[Sequence[Union[Column, CurriedFunction, Function]]]
    ) -> None:
        self.__list_validate(groupby)

    def _visit_where(
        self, where: Optional[Sequence[Union[BooleanCondition, Condition]]]
    ) -> None:
        self.__list_validate(where)

    def _visit_having(
        self, having: Optional[Sequence[Union[BooleanCondition, Condition]]]
    ) -> None:
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

    def _visit_totals(self, totals: Totals) -> None:
        totals.validate()

    def _visit_consistent(self, consistent: Consistent) -> None:
        consistent.validate()

    def _visit_turbo(self, turbo: Turbo) -> None:
        turbo.validate()

    def _visit_debug(self, debug: Debug) -> None:
        debug.validate()

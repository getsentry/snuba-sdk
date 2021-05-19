import json
from abc import ABC, abstractmethod
from typing import (
    Any,
    Generic,
    List,
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
from snuba_sdk.conditions import BooleanCondition, Condition
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
    Totals,
    Turbo,
)
from snuba_sdk.function import CurriedFunction, Function
from snuba_sdk.orderby import LimitBy, OrderBy
from snuba_sdk.relationships import Join
from snuba_sdk.snuba import is_aggregation_function
from snuba_sdk.visitors import ExpressionFinder, Translation


class InvalidQuery(Exception):
    pass


QVisited = TypeVar("QVisited")


def is_aggregate(
    function: Union[Function, CurriedFunction],
    aggregate_aliases: Optional[Set[str]] = None,
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
    def visit(self, query: "main.Query") -> QVisited:
        fields = query.get_fields()
        returns = {}
        for field in fields:
            returns[field] = getattr(self, f"_visit_{field}")(getattr(query, field))

        return self._combine(query, returns)

    @abstractmethod
    def _combine(
        self, query: "main.Query", returns: Mapping[str, QVisited]
    ) -> QVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_dataset(self, dataset: str) -> QVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_match(self, match: Union[Entity, Join, "main.Query"]) -> QVisited:
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
    def _visit_array_join(self, array_join: Optional[Column]) -> QVisited:
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

    def visit(self, query: "main.Query") -> str:
        self.translator.use_entity_aliases = isinstance(query.match, Join)
        return super().visit(query)

    def _combine(self, query: "main.Query", returns: Mapping[str, str]) -> str:
        clause_order = query.get_fields()
        # These fields are encoded outside of the SQL
        to_skip = ("dataset", "consistent", "turbo", "debug", "dry_run", "legacy")

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

    def _visit_match(self, match: Union[Entity, Join, "main.Query"]) -> str:
        if isinstance(match, (Entity, Join)):
            return f"MATCH {self.translator.visit(match)}"

        # We need a separate translator that can recurse through the subqueries
        # with different settings.
        translator = Printer(self.pretty, True)
        subquery = translator.visit(match)
        return "MATCH { %s }" % subquery

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

    def _visit_array_join(self, array_join: Optional[Column]) -> str:
        if array_join:
            return f"ARRAY JOIN {self.translator.visit(array_join)}"
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

    def _visit_dry_run(self, dry_run: DryRun) -> str:
        return str(dry_run) if dry_run else ""

    def _visit_legacy(self, legacy: Legacy) -> str:
        return str(legacy) if legacy else ""


class Translator(Printer):
    def __init__(self) -> None:
        super().__init__(False)

    def _combine(self, query: "main.Query", returns: Mapping[str, str]) -> str:
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

        return json.dumps(body)


class ExpressionSearcher(QueryVisitor[Set[Expression]]):
    def __init__(self, exp_type: Any) -> None:
        self.expression_finder = ExpressionFinder(exp_type)

    def _combine(
        self, query: "main.Query", returns: Mapping[str, Set[Expression]]
    ) -> Set[Expression]:
        found = set()
        for ret in returns.values():
            found |= ret
        return found

    def _visit_dataset(self, dataset: str) -> Set[Expression]:
        return set()

    def _visit_match(self, match: Union[Entity, Join, "main.Query"]) -> Set[Expression]:
        if isinstance(match, (Entity, Join)):
            return self.expression_finder.visit(match)
        return set()

    def __aggregate(self, terms: Optional[Sequence[Expression]]) -> Set[Expression]:
        found = set()
        if terms:
            for t in terms:
                found |= self.expression_finder.visit(t)
        return found

    def _visit_select(
        self, select: Optional[Sequence[Union[Column, CurriedFunction, Function]]]
    ) -> Set[Expression]:
        return self.__aggregate(select)

    def _visit_groupby(
        self, groupby: Optional[Sequence[Union[Column, CurriedFunction, Function]]]
    ) -> Set[Expression]:
        return self.__aggregate(groupby)

    def _visit_array_join(self, array_join: Optional[Column]) -> Set[Expression]:
        return self.expression_finder.visit(array_join) if array_join else set()

    def _visit_where(
        self, where: Optional[Sequence[Union[BooleanCondition, Condition]]]
    ) -> Set[Expression]:
        return self.__aggregate(where)

    def _visit_having(
        self, having: Optional[Sequence[Union[BooleanCondition, Condition]]]
    ) -> Set[Expression]:
        return self.__aggregate(having)

    def _visit_orderby(self, orderby: Optional[Sequence[OrderBy]]) -> Set[Expression]:
        return self.__aggregate(orderby)

    def _visit_limitby(self, limitby: Optional[LimitBy]) -> Set[Expression]:
        return self.expression_finder.visit(limitby) if limitby else set()

    def _visit_limit(self, limit: Optional[Limit]) -> Set[Expression]:
        return self.expression_finder.visit(limit) if limit else set()

    def _visit_offset(self, offset: Optional[Offset]) -> Set[Expression]:
        return self.expression_finder.visit(offset) if offset else set()

    def _visit_granularity(self, granularity: Optional[Granularity]) -> Set[Expression]:
        return self.expression_finder.visit(granularity) if granularity else set()

    def _visit_totals(self, totals: Totals) -> Set[Expression]:
        return self.expression_finder.visit(totals) if totals else set()

    def _visit_consistent(self, consistent: Consistent) -> Set[Expression]:
        return self.expression_finder.visit(consistent) if consistent else set()

    def _visit_turbo(self, turbo: Turbo) -> Set[Expression]:
        return self.expression_finder.visit(turbo) if turbo else set()

    def _visit_debug(self, debug: Debug) -> Set[Expression]:
        return self.expression_finder.visit(debug) if debug else set()

    def _visit_dry_run(self, dry_run: DryRun) -> Set[Expression]:
        return self.expression_finder.visit(dry_run) if dry_run else set()

    def _visit_legacy(self, legacy: Legacy) -> Set[Expression]:
        return self.expression_finder.visit(legacy) if legacy else set()


class Validator(QueryVisitor[None]):
    def __init__(self) -> None:
        super().__init__()
        self.column_finder = ExpressionSearcher(Column)

    def _combine(self, query: "main.Query", returns: Mapping[str, None]) -> None:
        # TODO: Contextual validations:
        # - Must have certain conditions (project, timestamp, organization etc.)

        # If the match is a subquery, then the outer query can only reference columns
        # from the subquery.
        all_columns = self.column_finder.visit(query)
        if isinstance(query.match, main.Query):
            inner_match = set()
            assert query.match.select is not None
            for s in query.match.select:
                if isinstance(s, CurriedFunction):
                    inner_match.add(s.alias)
                elif isinstance(s, Column):
                    inner_match.add(s.name)

            for c in all_columns:
                if isinstance(c, Column) and c.name not in inner_match:
                    raise InvalidQuery(
                        f"outer query is referencing column {c.name} that does not exist in subquery"
                    )
        # In a Join, all the columns must have a qualifying entity with a valid alias.
        elif isinstance(query.match, Join):
            entity_aliases = {
                alias: entity for alias, entity in query.match.get_alias_mappings()
            }
            column_exps = self.column_finder.visit(query)
            for c in column_exps:
                assert isinstance(c, Column)
                if c.entity is None:
                    raise InvalidQuery(f"{c.name} must have a qualifying entity")
                elif c.entity.alias not in entity_aliases:
                    raise InvalidQuery(
                        f"{c.name} has unknown entity alias {c.entity.alias}"
                    )
                elif entity_aliases[c.entity.alias] != c.entity.name:
                    raise InvalidQuery(
                        f"{c.name} has incorrect alias for entity {c.entity.name}: {c.entity.alias}"
                    )

        if query.select is None or len(query.select) == 0:
            raise InvalidQuery("query must have at least one expression in select")

        # Non-aggregate expressions must be in the groupby if there is an aggregate
        non_aggregates: List[Union[Column, CurriedFunction, Function]] = []
        has_aggregates = False

        # Legacy queries use aliases of aggregates in the select clause. This validation
        # needs to understand the difference between a Column and something that looks
        # like a Column but is actually an alias to an aggregate. Gather these "alias
        # columns" here so the validator can properly check.
        alias_columns = set()
        aggregate_aliases = set()
        for exp in query.select:
            if (
                isinstance(exp, (CurriedFunction, Function))
                and is_aggregate(exp)
                and exp.alias
            ):
                alias_columns.add(Column(exp.alias))
                aggregate_aliases.add(exp.alias)

        for exp in query.select:
            # If a column is actually an alias for an aggregate, it shouldn't be counted as
            # a non-aggregate.
            if isinstance(exp, Column) and exp not in alias_columns:
                non_aggregates.append(exp)
            elif isinstance(exp, (CurriedFunction, Function)) and not is_aggregate(
                exp, aggregate_aliases
            ):
                non_aggregates.append(exp)
            else:
                has_aggregates = True

        if has_aggregates and len(non_aggregates) > 0:
            if not query.groupby or len(query.groupby) == 0:
                raise InvalidQuery(
                    "groupby must be included if there are aggregations in the select"
                )

            for group_exp in non_aggregates:
                # Legacy passes aliases in the groupby instead of whole expressions.
                # We need to check for both.
                if isinstance(group_exp, Function):
                    if (
                        group_exp.alias is not None
                        and Column(group_exp.alias) in query.groupby
                    ):
                        continue

                    # If this is a function wrapper around a groupby column, then this is also allowed
                    # e.g. BY x, toString(x)
                    is_wrapper = False
                    for exp in query.groupby:
                        if isinstance(exp, Column) and find_column_in_function(
                            exp, group_exp
                        ):
                            is_wrapper = True
                            break

                    if is_wrapper:
                        continue

                if group_exp not in query.groupby:
                    raise InvalidQuery(f"{group_exp} missing from the groupby")

        if query.totals and not query.groupby:
            raise InvalidQuery("totals is only valid with a groupby")

    def _visit_dataset(self, dataset: str) -> None:
        pass

    def _visit_match(self, match: Union[Entity, Join, "main.Query"]) -> None:
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

    def _visit_array_join(self, array_join: Optional[Column]) -> None:
        array_join.validate() if array_join is not None else None

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

    def _visit_dry_run(self, dry_run: DryRun) -> None:
        dry_run.validate()

    def _visit_legacy(self, legacy: Legacy) -> None:
        legacy.validate()

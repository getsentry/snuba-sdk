from dataclasses import dataclass, fields, replace
from typing import Any, List, Optional, Sequence, Union

from snuba_sdk.column import Column
from snuba_sdk.conditions import BooleanCondition, Condition
from snuba_sdk.entity import Entity
from snuba_sdk.expressions import (
    Consistent,
    Debug,
    DryRun,
    Granularity,
    Legacy,
    Limit,
    Offset,
    Totals,
    Turbo,
)
from snuba_sdk.function import CurriedFunction, Function
from snuba_sdk.orderby import LimitBy, OrderBy
from snuba_sdk.query_visitors import InvalidQuery, Printer, Translator, Validator
from snuba_sdk.relationships import Join


def list_type(vals: Sequence[Any], type_classes: Sequence[Any]) -> bool:
    return isinstance(vals, list) and all(
        isinstance(v, tuple(type_classes)) for v in vals
    )


PRINTER = Printer()
PRETTY_PRINTER = Printer(pretty=True)
VALIDATOR = Validator()
TRANSLATOR = Translator()


@dataclass(frozen=True)
class Query:
    """
    A code representation of a SnQL query. It is immutable, so any set functions
    return a new copy of the query. Unlike Expressions it is possible to
    instantiate a Query that is invalid. Any of the translation functions will
    validate the query before translating them, so the query must be valid before
    they are called.
    """

    # These must be listed in the order that they must appear in the SnQL query.
    dataset: str
    match: Union[Entity, Join, "Query"]
    select: Optional[List[Union[Column, CurriedFunction, Function]]] = None
    groupby: Optional[List[Union[Column, CurriedFunction, Function]]] = None
    array_join: Optional[Column] = None
    where: Optional[List[Union[BooleanCondition, Condition]]] = None
    having: Optional[List[Union[BooleanCondition, Condition]]] = None
    orderby: Optional[List[OrderBy]] = None
    limitby: Optional[LimitBy] = None
    limit: Optional[Limit] = None
    offset: Optional[Offset] = None
    granularity: Optional[Granularity] = None
    totals: Totals = Totals(False)
    consistent: Consistent = Consistent(False)
    turbo: Turbo = Turbo(False)
    debug: Debug = Debug(False)
    dry_run: DryRun = DryRun(False)
    legacy: Legacy = Legacy(False)

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

        if not isinstance(self.match, (Query, Join, Entity)):
            raise InvalidQuery("queries must have a valid Entity, Join or Query")

        if isinstance(self.match, Query):
            try:
                self.match.validate()
            except Exception as e:
                raise InvalidQuery(f"inner query is invalid: {e}")

    def _replace(self, field: str, value: Any) -> "Query":
        new = replace(self, **{field: value})
        return new

    def get_fields(self) -> Sequence[str]:
        self_fields = fields(self)  # Verified the order in the Python source
        return tuple(f.name for f in self_fields)

    def set_match(self, match: Union[Entity, Join, "Query"]) -> "Query":
        if not isinstance(match, (Entity, Join, Query)):
            raise InvalidQuery(f"{match} must be a valid Entity, Join or Query")
        elif isinstance(match, Query):
            try:
                match.validate()
            except Exception as e:
                raise InvalidQuery(f"inner query is invalid: {e}")

        return self._replace("match", match)

    def set_select(
        self, select: Sequence[Union[Column, CurriedFunction, Function]]
    ) -> "Query":
        if not list_type(select, (Column, CurriedFunction, Function)) or not select:
            raise InvalidQuery(
                "select clause must be a non-empty list of Column and/or Function"
            )
        return self._replace("select", select)

    def set_groupby(
        self, groupby: Sequence[Union[Column, CurriedFunction, Function]]
    ) -> "Query":
        if not list_type(groupby, (Column, CurriedFunction, Function)):
            raise InvalidQuery(
                "groupby clause must be a list of Column and/or Function"
            )
        return self._replace("groupby", groupby)

    def set_array_join(self, array_join: Column) -> "Query":
        if not isinstance(array_join, Column):
            raise InvalidQuery("array join must be a Column")

        return self._replace("array_join", array_join)

    def set_where(
        self, conditions: Sequence[Union[BooleanCondition, Condition]]
    ) -> "Query":
        if not list_type(conditions, (BooleanCondition, Condition)):
            raise InvalidQuery("where clause must be a list of conditions")
        return self._replace("where", conditions)

    def set_having(
        self, conditions: Sequence[Union[BooleanCondition, Condition]]
    ) -> "Query":
        if not list_type(conditions, (BooleanCondition, Condition)):
            raise InvalidQuery("having clause must be a list of conditions")
        return self._replace("having", conditions)

    def set_orderby(self, orderby: Sequence[OrderBy]) -> "Query":
        if not list_type(orderby, (OrderBy,)):
            raise InvalidQuery("orderby clause must be a list of OrderBy")
        return self._replace("orderby", orderby)

    def set_limitby(self, limitby: LimitBy) -> "Query":
        if not isinstance(limitby, LimitBy):
            raise InvalidQuery("limitby clause must be a LimitBy")
        return self._replace("limitby", limitby)

    def set_limit(self, limit: int) -> "Query":
        return self._replace("limit", Limit(limit))

    def set_offset(self, offset: int) -> "Query":
        return self._replace("offset", Offset(offset))

    def set_granularity(self, granularity: int) -> "Query":
        return self._replace("granularity", Granularity(granularity))

    def set_totals(self, totals: bool) -> "Query":
        return self._replace("totals", Totals(totals))

    def set_consistent(self, consistent: bool) -> "Query":
        return self._replace("consistent", Consistent(consistent))

    def set_turbo(self, turbo: bool) -> "Query":
        return self._replace("turbo", Turbo(turbo))

    def set_debug(self, debug: bool) -> "Query":
        return self._replace("debug", Debug(debug))

    def set_dry_run(self, dry_run: bool) -> "Query":
        return self._replace("dry_run", DryRun(dry_run))

    def set_legacy(self, legacy: bool) -> "Query":
        return self._replace("legacy", Legacy(legacy))

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

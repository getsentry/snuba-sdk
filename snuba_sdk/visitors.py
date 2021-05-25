import re
from abc import ABC, abstractmethod
from datetime import date, datetime
from typing import Any, Generic, Set, TypeVar

from snuba_sdk.column import Column
from snuba_sdk.conditions import BooleanCondition, Condition, is_unary
from snuba_sdk.entity import Entity
from snuba_sdk.expressions import (
    Consistent,
    Debug,
    DryRun,
    Expression,
    Granularity,
    InvalidExpression,
    Legacy,
    Limit,
    Offset,
    Scalar,
    ScalarType,
    Totals,
    Turbo,
    is_scalar,
)
from snuba_sdk.function import CurriedFunction, Function
from snuba_sdk.orderby import LimitBy, OrderBy
from snuba_sdk.relationships import Join, Relationship

# validation regexes
unescaped_quotes = re.compile(r"(?<!\\)'")
unescaped_newline = re.compile(r"(?<!\\)\n")


TVisited = TypeVar("TVisited")


class ExpressionVisitor(ABC, Generic[TVisited]):
    def visit(self, node: Expression) -> TVisited:
        if isinstance(node, Column):
            return self._visit_column(node)
        elif isinstance(node, (CurriedFunction, Function)):
            return self._visit_curried_function(node)
        elif isinstance(node, Entity):
            return self._visit_entity(node)
        elif isinstance(node, Relationship):
            return self._visit_relationship(node)
        elif isinstance(node, Join):
            return self._visit_join(node)
        elif isinstance(node, Condition):
            return self._visit_condition(node)
        elif isinstance(node, BooleanCondition):
            return self._visit_boolean_condition(node)
        elif isinstance(node, OrderBy):
            return self._visit_orderby(node)
        elif isinstance(node, Limit):
            return self._visit_int_literal(node.limit)
        elif isinstance(node, Offset):
            return self._visit_int_literal(node.offset)
        elif isinstance(node, LimitBy):
            return self._visit_limitby(node)
        elif isinstance(node, Granularity):
            return self._visit_int_literal(node.granularity)
        elif isinstance(node, Totals):
            return self._visit_totals(node)
        elif isinstance(node, Consistent):
            return self._visit_consistent(node)
        elif isinstance(node, Turbo):
            return self._visit_turbo(node)
        elif isinstance(node, Debug):
            return self._visit_debug(node)
        elif isinstance(node, DryRun):
            return self._visit_dry_run(node)
        elif isinstance(node, Legacy):
            return self._visit_legacy(node)

        assert False, f"Unhandled Expression: {node}"

    @abstractmethod
    def _visit_column(self, column: Column) -> TVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_curried_function(self, func: CurriedFunction) -> TVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_int_literal(self, literal: int) -> TVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_entity(self, entity: Entity) -> TVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_relationship(self, relationship: Relationship) -> TVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_join(self, join: Join) -> TVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_condition(self, cond: Condition) -> TVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_boolean_condition(self, cond: BooleanCondition) -> TVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_orderby(self, orderby: OrderBy) -> TVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_limitby(self, limitby: LimitBy) -> TVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_totals(self, totals: Totals) -> TVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_consistent(self, consistent: Consistent) -> TVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_turbo(self, turbo: Turbo) -> TVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_debug(self, debug: Debug) -> TVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_dry_run(self, dry_run: DryRun) -> TVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_legacy(self, legacy: Legacy) -> TVisited:
        raise NotImplementedError


class Translation(ExpressionVisitor[str]):
    def __init__(self, use_entity_aliases: bool = False):
        # Eventually JOINs will set this to True, but single entity/sub queries
        # don't support entity aliases.
        self.use_entity_aliases = use_entity_aliases

    def _stringify_scalar(self, value: ScalarType) -> str:
        if value is None:
            return "NULL"
        elif isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, (str, bytes)):
            if isinstance(value, bytes):
                decoded = value.decode()
            else:
                decoded = value

            decoded = unescaped_quotes.sub("\\'", decoded)
            decoded = unescaped_newline.sub("\\\\n", decoded)
            return f"'{decoded}'"
        elif isinstance(value, (int, float)):
            return f"{value}"
        elif isinstance(value, datetime):
            # Snuba expects naive UTC datetimes, so convert to that
            if value.tzinfo is not None:
                delta = value.utcoffset()
                assert delta is not None
                value = value - delta
                value = value.replace(tzinfo=None)
            return f"toDateTime('{value.isoformat()}')"
        elif isinstance(value, date):
            return f"toDateTime('{value.isoformat()}')"
        elif isinstance(value, Expression):
            return self.visit(value)
        elif isinstance(value, list):
            is_scalar(value)  # Throws on an invalid array
            return f"array({', '.join([self._stringify_scalar(v) for v in value])})"
        elif isinstance(value, tuple):
            is_scalar(value)  # Throws on an invalid tuple
            return f"tuple({', '.join([self._stringify_scalar(v) for v in value])})"

        raise InvalidExpression(f"'{value}' is not a valid scalar")

    def _visit_column(self, column: Column) -> str:
        alias_clause = ""
        if column.entity is not None and self.use_entity_aliases:
            alias_clause = f"{column.entity.alias}."
        return f"{alias_clause}{column.name}"

    def _visit_curried_function(self, func: CurriedFunction) -> str:
        alias = "" if func.alias is None else f" AS {func.alias}"
        initialize_clause = ""
        if func.initializers is not None:
            initializers = []
            for initer in func.initializers:
                if isinstance(initer, Column):
                    initializers.append(self.visit(initer))
                elif isinstance(initer, tuple(Scalar)):
                    initializers.append(self._stringify_scalar(initer))

            initialize_clause = f"({', '.join(initializers)})"

        param_clause = ""
        if func.parameters is not None:
            params = []
            for param in func.parameters:
                if isinstance(param, (Column, CurriedFunction, Function)):
                    params.append(self.visit(param))
                elif is_scalar(param):
                    params.append(self._stringify_scalar(param))

            param_clause = f"({', '.join(params)})"

        return f"{func.function}{initialize_clause}{param_clause}{alias}"

    def _visit_int_literal(self, literal: int) -> str:
        return f"{literal:d}"

    def _visit_entity(self, entity: Entity) -> str:
        alias_clause = ""
        if entity.alias is not None and self.use_entity_aliases:
            alias_clause = f"{entity.alias}: "

        sample_clause = ""
        if entity.sample is not None:
            if entity.sample % 1 == 0:
                sample_clause = f" SAMPLE {entity.sample:.1f}"
            else:
                sample_clause = f" SAMPLE {entity.sample:f}"
        return f"({alias_clause}{entity.name}{sample_clause})"

    def _visit_relationship(self, relationship: Relationship) -> str:
        return f"{self.visit(relationship.lhs)} -[{relationship.name}]-> {self.visit(relationship.rhs)}"

    def _visit_join(self, join: Join) -> str:
        return ", ".join([self.visit(rel) for rel in join.relationships])

    def _visit_condition(self, cond: Condition) -> str:
        rhs = None
        if is_unary(cond.op):
            rhs = ""
        elif isinstance(cond.rhs, (Column, CurriedFunction, Function)):
            rhs = f" {self.visit(cond.rhs)}"
        elif is_scalar(cond.rhs):
            rhs = f" {self._stringify_scalar(cond.rhs)}"

        assert rhs is not None
        return f"{self.visit(cond.lhs)} {cond.op.value}{rhs}"

    def _visit_boolean_condition(self, cond: BooleanCondition) -> str:
        conds = [self.visit(c) for c in cond.conditions]
        cond_str = f" {cond.op.value} ".join(conds)
        return f"({cond_str})"

    def _visit_orderby(self, orderby: OrderBy) -> str:
        return f"{self.visit(orderby.exp)} {orderby.direction.value}"

    def _visit_limitby(self, limitby: LimitBy) -> str:
        return f"{limitby.count} BY {self.visit(limitby.column)}"

    def _visit_totals(self, totals: Totals) -> str:
        return str(totals)

    def _visit_consistent(self, consistent: Consistent) -> str:
        return str(consistent)

    def _visit_turbo(self, turbo: Turbo) -> str:
        return str(turbo)

    def _visit_debug(self, debug: Debug) -> str:
        return str(debug)

    def _visit_dry_run(self, dry_run: DryRun) -> str:
        return str(dry_run)

    def _visit_legacy(self, legacy: Legacy) -> str:
        return str(legacy)


class ExpressionFinder(ExpressionVisitor[Set[Expression]]):
    def __init__(self, exp_type: Any) -> None:
        self.exp_type = exp_type

    def _visit_column(self, column: Column) -> Set[Expression]:
        if isinstance(column, self.exp_type):
            return set([column])
        return set()

    def _visit_curried_function(self, func: CurriedFunction) -> Set[Expression]:
        if isinstance(func, self.exp_type):
            return set([func])

        found = set()
        if func.initializers is not None:
            for initer in func.initializers:
                if isinstance(initer, Expression):
                    found |= self.visit(initer)

        if func.parameters is not None:
            for param in func.parameters:
                if isinstance(param, Expression):
                    found |= self.visit(param)

        return found

    def _visit_int_literal(self, literal: int) -> Set[Expression]:
        return set()

    def _visit_entity(self, entity: Entity) -> Set[Expression]:
        if isinstance(entity, self.exp_type):
            return set([entity])
        return set()

    def _visit_relationship(self, relationship: Relationship) -> Set[Expression]:
        if isinstance(relationship, self.exp_type):
            return set([relationship])
        elif isinstance(relationship.lhs, self.exp_type):
            return set([relationship.lhs, relationship.rhs])

        return set()

    def _visit_join(self, join: Join) -> Set[Expression]:
        if isinstance(join, self.exp_type):
            return set([join])
        elif isinstance(join.relationships[0], self.exp_type):
            return set(join.relationships)

        return set()

    def _visit_condition(self, cond: Condition) -> Set[Expression]:
        found = self.visit(cond.lhs)
        if not is_unary(cond.op):
            if isinstance(cond.rhs, Expression):
                found |= self.visit(cond.rhs)
        return found

    def _visit_boolean_condition(self, cond: BooleanCondition) -> Set[Expression]:
        found = set()
        for c in cond.conditions:
            found |= self.visit(c)
        return found

    def _visit_orderby(self, orderby: OrderBy) -> Set[Expression]:
        if isinstance(orderby, self.exp_type):
            return set([orderby])

        return self.visit(orderby.exp)

    def _visit_limitby(self, limitby: LimitBy) -> Set[Expression]:
        if isinstance(limitby, self.exp_type):
            return set([limitby])
        return self.visit(limitby.column)

    def _visit_totals(self, totals: Totals) -> Set[Expression]:
        if isinstance(totals, self.exp_type):
            return set([totals])
        return set()

    def _visit_consistent(self, consistent: Consistent) -> Set[Expression]:
        if isinstance(consistent, self.exp_type):
            return set([consistent])
        return set()

    def _visit_turbo(self, turbo: Turbo) -> Set[Expression]:
        if isinstance(turbo, self.exp_type):
            return set([turbo])
        return set()

    def _visit_debug(self, debug: Debug) -> Set[Expression]:
        if isinstance(debug, self.exp_type):
            return set([debug])
        return set()

    def _visit_dry_run(self, dry_run: DryRun) -> Set[Expression]:
        if isinstance(dry_run, self.exp_type):
            return set([dry_run])
        return set()

    def _visit_legacy(self, legacy: Legacy) -> Set[Expression]:
        if isinstance(legacy, self.exp_type):
            return set([legacy])
        return set()

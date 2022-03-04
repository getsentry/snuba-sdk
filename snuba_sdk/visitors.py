from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import contextmanager
from datetime import date, datetime
from typing import Any, Generator, Generic, Set, TypeVar

from snuba_sdk.aliased_expression import AliasedExpression
from snuba_sdk.column import Column
from snuba_sdk.conditions import BooleanCondition, Condition, is_unary
from snuba_sdk.entity import Entity
from snuba_sdk.expressions import (
    Expression,
    Granularity,
    InvalidExpressionError,
    Limit,
    Offset,
    Scalar,
    ScalarType,
    is_scalar,
)
from snuba_sdk.flags import BooleanFlag, StringFlag
from snuba_sdk.function import CurriedFunction, Function, Identifier, Lambda
from snuba_sdk.orderby import LimitBy, OrderBy
from snuba_sdk.relationships import Join, Relationship

TVisited = TypeVar("TVisited")


class ExpressionVisitor(ABC, Generic[TVisited]):
    def visit(self, node: Expression) -> TVisited:
        if isinstance(node, AliasedExpression):
            return self._visit_aliased_expression(node)
        if isinstance(node, Column):
            return self._visit_column(node)
        elif isinstance(node, (CurriedFunction, Function)):
            return self._visit_curried_function(node)
        elif isinstance(node, Identifier):
            return self._visit_identifier(node)
        elif isinstance(node, Lambda):
            return self._visit_lambda(node)
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
        elif isinstance(node, BooleanFlag):
            return self._visit_boolean_flag(node)
        elif isinstance(node, StringFlag):
            return self._visit_string_flag(node)

        assert False, f"Unhandled Expression: {node}"

    @abstractmethod
    def _visit_aliased_expression(self, aliased: AliasedExpression) -> TVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_column(self, column: Column) -> TVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_curried_function(self, func: CurriedFunction) -> TVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_identifier(self, ident: Identifier) -> TVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_lambda(self, lambda_fn: Lambda) -> TVisited:
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
    def _visit_boolean_flag(self, boolean_flag: BooleanFlag) -> TVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_string_flag(self, string_flag: StringFlag) -> TVisited:
        raise NotImplementedError


class Translation(ExpressionVisitor[str]):
    def __init__(self, use_entity_aliases: bool = False):
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

            # The ' and \ character are escaped in the string to ensure
            # the query is valid. They are de-escaped in the SnQL parser.
            # Also escape newlines since they break the SnQL grammar.
            decoded = (
                decoded.replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n")
            )
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

        raise InvalidExpressionError(f"'{value}' is not a valid scalar")

    def _visit_aliased_expression(self, aliased: AliasedExpression) -> str:
        alias_clause = ""
        if aliased.alias is not None:
            alias_clause = f" AS `{aliased.alias}`"

        return f"{self.visit(aliased.exp)}{alias_clause}"

    def _visit_column(self, column: Column) -> str:
        entity_alias_clause = ""
        if column.entity is not None and self.use_entity_aliases:
            entity_alias_clause = f"{column.entity.alias}."

        return f"{entity_alias_clause}{column.name}"

    def _visit_curried_function(self, func: CurriedFunction) -> str:
        alias = "" if func.alias is None else f" AS `{func.alias}`"
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
                if isinstance(
                    param, (Column, CurriedFunction, Function, Identifier, Lambda)
                ):
                    params.append(self.visit(param))
                elif is_scalar(param):
                    params.append(self._stringify_scalar(param))

            param_clause = f"({', '.join(params)})"

        return f"{func.function}{initialize_clause}{param_clause}{alias}"

    def _visit_identifier(self, ident: Identifier) -> str:
        return f"`{ident.name}`"

    def _visit_lambda(self, lambda_fn: Lambda) -> str:
        identifiers = ", ".join(f"`{i}`" for i in lambda_fn.identifiers)
        return f"({identifiers}) -> {self.visit(lambda_fn.transformation)}"

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
        return f"{limitby.count} BY {','.join(self.visit(column) for column in limitby.columns)}"

    def _visit_boolean_flag(self, boolean_flag: BooleanFlag) -> str:
        return str(boolean_flag)

    def _visit_string_flag(self, string_flag: StringFlag) -> str:
        return string_flag.value


@contextmanager
def entity_aliases(translator: Translation) -> Generator[None, None, None]:
    translator.use_entity_aliases = True
    yield
    translator.use_entity_aliases = False


class ExpressionFinder(ExpressionVisitor[Set[Expression]]):
    def __init__(self, exp_type: Any) -> None:
        self.exp_type = exp_type

    def _visit_aliased_expression(self, aliased: AliasedExpression) -> set[Expression]:
        if isinstance(aliased, self.exp_type):
            return set([aliased])

        return self.visit(aliased.exp)

    def _visit_column(self, column: Column) -> set[Expression]:
        if isinstance(column, self.exp_type):
            return set([column])
        return set()

    def _visit_curried_function(self, func: CurriedFunction) -> set[Expression]:
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

    def _visit_identifier(self, ident: Identifier) -> set[Expression]:
        if isinstance(ident, self.exp_type):
            return set([ident])

        return set()

    def _visit_lambda(self, lambda_fn: Lambda) -> set[Expression]:
        if isinstance(lambda_fn, self.exp_type):
            return set([lambda_fn])

        if self.exp_type == Identifier:
            return set([Identifier(i) for i in lambda_fn.identifiers])

        return self.visit(lambda_fn.transformation)

    def _visit_int_literal(self, literal: int) -> set[Expression]:
        return set()

    def _visit_entity(self, entity: Entity) -> set[Expression]:
        if isinstance(entity, self.exp_type):
            return set([entity])
        return set()

    def _visit_relationship(self, relationship: Relationship) -> set[Expression]:
        if isinstance(relationship, self.exp_type):
            return set([relationship])
        elif isinstance(relationship.lhs, self.exp_type):
            return set([relationship.lhs, relationship.rhs])

        return set()

    def _visit_join(self, join: Join) -> set[Expression]:
        if isinstance(join, self.exp_type):
            return set([join])
        elif isinstance(join.relationships[0], self.exp_type):
            return set(join.relationships)

        return set()

    def _visit_condition(self, cond: Condition) -> set[Expression]:
        found = self.visit(cond.lhs)
        if not is_unary(cond.op):
            if isinstance(cond.rhs, Expression):
                found |= self.visit(cond.rhs)
        return found

    def _visit_boolean_condition(self, cond: BooleanCondition) -> set[Expression]:
        found = set()
        for c in cond.conditions:
            found |= self.visit(c)
        return found

    def _visit_orderby(self, orderby: OrderBy) -> set[Expression]:
        if isinstance(orderby, self.exp_type):
            return set([orderby])

        return self.visit(orderby.exp)

    def _visit_limitby(self, limitby: LimitBy) -> set[Expression]:
        if isinstance(limitby, self.exp_type):
            return set([limitby])

        return set.union(*[self.visit(column) for column in limitby.columns])

    def _visit_boolean_flag(self, boolean_flag: BooleanFlag) -> set[Expression]:
        if isinstance(boolean_flag, self.exp_type):
            return set([boolean_flag])
        return set()

    def _visit_string_flag(self, string_flag: StringFlag) -> set[Expression]:
        if isinstance(string_flag, self.exp_type):
            return set([string_flag])
        return set()

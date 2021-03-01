import re
from abc import ABC, abstractmethod
from datetime import date, datetime
from typing import Generic, TypeVar

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
    InvalidExpression,
    is_scalar,
    Limit,
    LimitBy,
    Offset,
    OrderBy,
    Scalar,
    ScalarType,
    Totals,
    Turbo,
)


# validation regexes
unescaped_quotes = re.compile(r"(?<!\\)'")
unescaped_newline = re.compile(r"(?<!\\)\n")


def _stringify_scalar(value: ScalarType) -> str:
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
    elif isinstance(value, list):
        is_scalar(value)  # Throws on an invalid array
        return f"array({', '.join([_stringify_scalar(v) for v in value])})"
    elif isinstance(value, tuple):
        is_scalar(value)  # Throws on an invalid tuple
        return f"tuple({', '.join([_stringify_scalar(v) for v in value])})"

    raise InvalidExpression(f"'{value}' is not a valid scalar")


TVisited = TypeVar("TVisited")


class ExpressionVisitor(ABC, Generic[TVisited]):
    def visit(self, node: Expression) -> TVisited:
        if isinstance(node, Column):
            return self._visit_column(node)
        elif isinstance(node, (CurriedFunction, Function)):
            return self._visit_curried_function(node)
        elif isinstance(node, Entity):
            return self._visit_entity(node)
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


class Translation(ExpressionVisitor[str]):
    def _visit_column(self, column: Column) -> str:
        return column.name

    def _visit_curried_function(self, func: CurriedFunction) -> str:
        alias = "" if func.alias is None else f" AS {func.alias}"
        initialize_clause = ""
        if func.initializers is not None:
            initializers = []
            for initer in func.initializers:
                if isinstance(initer, Column):
                    initializers.append(self.visit(initer))
                elif isinstance(initer, tuple(Scalar)):
                    initializers.append(_stringify_scalar(initer))

            initialize_clause = f"({', '.join(initializers)})"

        param_clause = ""
        if func.parameters is not None:
            params = []
            for param in func.parameters:
                if isinstance(param, (Column, CurriedFunction, Function)):
                    params.append(self.visit(param))
                elif is_scalar(param):
                    params.append(_stringify_scalar(param))

            param_clause = f"({', '.join(params)})"

        return f"{func.function}{initialize_clause}{param_clause}{alias}"

    def _visit_int_literal(self, literal: int) -> str:
        return f"{literal:d}"

    def _visit_entity(self, entity: Entity) -> str:
        sample_clause = ""
        if entity.sample is not None:
            if isinstance(entity.sample, int):
                sample_clause = f" SAMPLE {entity.sample:d}"
            else:
                sample_clause = f" SAMPLE {entity.sample:f}"
        return f"({entity.name}{sample_clause})"

    def _visit_condition(self, cond: Condition) -> str:
        rhs = None
        if cond.is_unary():
            rhs = ""
        elif isinstance(cond.rhs, (Column, CurriedFunction, Function)):
            rhs = f" {self.visit(cond.rhs)}"
        elif is_scalar(cond.rhs):
            rhs = f" {_stringify_scalar(cond.rhs)}"

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

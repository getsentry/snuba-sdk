from __future__ import annotations

import re
from abc import ABC, abstractmethod
from datetime import date, datetime
from typing import Generic, TypeVar

from snuba_sdk.expressions import (
    Column,
    Expression,
    Function,
    Granularity,
    InvalidExpression,
    Limit,
    Offset,
    Scalar,
    ScalarType,
)
from snuba_sdk.entity import Entity
from snuba_sdk.conditions import Condition


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

    raise InvalidExpression(f"'{value}' is not a valid scalar")


TVisited = TypeVar("TVisited")


class Visitor(ABC, Generic[TVisited]):
    def visit(self, node: Expression) -> TVisited:
        if isinstance(node, Column):
            return self._visit_column(node)
        elif isinstance(node, Function):
            return self._visit_function(node)
        elif isinstance(node, Entity):
            return self._visit_entity(node)
        elif isinstance(node, Condition):
            return self._visit_condition(node)
        elif isinstance(node, Limit):
            return self._visit_int_literal(node.limit)
        elif isinstance(node, Offset):
            return self._visit_int_literal(node.offset)
        elif isinstance(node, Granularity):
            return self._visit_int_literal(node.granularity)

        assert False, f"Unhandled Expression: {node}"

    @abstractmethod
    def _visit_column(self, column: Column) -> TVisited:
        raise NotImplementedError

    @abstractmethod
    def _visit_function(self, func: Function) -> TVisited:
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


class Translation(Visitor[str]):
    def _visit_column(self, column: Column) -> str:
        return column.name

    def _visit_function(self, func: Function) -> str:
        alias = "" if func.alias is None else f" AS {func.alias}"
        params = []
        for param in func.parameters:
            if isinstance(param, (Column, Function)):
                params.append(self.visit(param))
            elif isinstance(param, tuple(Scalar)):
                params.append(_stringify_scalar(param))

        return f"{func.function}({', '.join(params)}){alias}"

    def _visit_int_literal(self, literal: int) -> str:
        return f"{literal:d}"

    def _visit_entity(self, entity: Entity) -> str:
        return f"({entity.name})"

    def _visit_condition(self, cond: Condition) -> str:
        if isinstance(cond.rhs, (Column, Function)):
            rhs = self.visit(cond.rhs)
        elif isinstance(cond.rhs, tuple(Scalar)):
            rhs = _stringify_scalar(cond.rhs)

        return f"{self.visit(cond.lhs)} {cond.op.value} {rhs}"

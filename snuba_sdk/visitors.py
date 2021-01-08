from __future__ import annotations

import re
from datetime import datetime, date
from typing import Optional

from snuba_sdk.expressions import (
    Column,
    Function,
    Entity,
    Condition,
    InvalidExpression,
    Scalar,
    ScalarType,
    Visitor,
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

    raise InvalidExpression(f"'{value}' is not a valid scalar")


class Translation(Visitor[str]):
    def visit_column(self, column: Column) -> str:
        return column.name

    def visit_function(self, func: Function) -> str:
        alias = "" if func.alias is None else f" AS {func.alias}"
        params = []
        for param in func.parameters:
            if isinstance(param, (Column, Function)):
                params.append(param.accept(self))
            elif isinstance(param, tuple(Scalar)):
                params.append(_stringify_scalar(param))

        return f"{func.function}({', '.join(params)}){alias}"

    def visit_int_literal(
        self, literal: int, minn: Optional[int], maxn: Optional[int], name: str
    ) -> str:
        return f"{literal:d}"

    def visit_entity(self, entity: Entity) -> str:
        return f"({entity.name})"

    def visit_condition(self, cond: Condition) -> str:
        if isinstance(cond.rhs, (Column, Function)):
            rhs = cond.rhs.accept(self)
        elif isinstance(cond.rhs, tuple(Scalar)):
            rhs = _stringify_scalar(cond.rhs)

        return f"{cond.lhs.accept(self)} {cond.op.value} {rhs}"

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import List, Union

from snuba_sdk import Expression

ScalarType = Union[None, bool, str, float, int, date, datetime]


class InvalidExpression(Exception):
    pass


@dataclass(frozen=True)
class Limit(Expression):
    limit: int

    def validate(self) -> None:
        if self.limit < 0 or self.limit > 10000:
            raise InvalidExpression(f"limit {self.limit} is invalid")

    def translate(self) -> str:
        return str(self)

    def __repr__(self) -> str:
        return f"{self.limit:d}"


@dataclass(frozen=True)
class Offset(Expression):
    offset: int

    def validate(self) -> None:
        if self.offset < 0 or self.offset > 10000:
            raise InvalidExpression(f"offset {self.offset} is invalid")

    def translate(self) -> str:
        return str(self)

    def __repr__(self) -> str:
        return f"{self.offset:d}"


@dataclass(frozen=True)
class Granularity(Expression):
    granularity: int

    def validate(self) -> None:
        if self.granularity < 0:
            raise InvalidExpression(f"granularity {self.granularity} is invalid")

    def translate(self) -> str:
        return str(self)

    def __repr__(self) -> str:
        return f"{self.granularity:d}"


column_name_re = re.compile(r"[a-zA-Z_][a-zA-Z0-9_\.]*")


@dataclass(frozen=True)
class Column(Expression):
    name: str

    def validate(self) -> None:
        if not column_name_re.match(self.name):
            raise InvalidExpression(f"'{self}' contains invalid characters")

    def translate(self) -> str:
        return str(self)

    def __repr__(self) -> str:
        return self.name


@dataclass(frozen=True)
class Function(Expression):
    function: str
    parameters: List[Union[ScalarType, Column, Function]]
    alias: str

    def validate(self) -> None:
        raise InvalidExpression(f"'{self}' is not a valid column")

    def translate(self) -> str:
        return str(self)

    def __repr__(self) -> str:
        return f"{self.function}({','.join(map(str, self.parameters))}) AS {self.alias}"


@dataclass(frozen=True)
class Condition(Expression):
    lhs: Union[Column, Function]
    op: str  # "<", ">", "in" etc.
    rhs: Union[Column, Function, ScalarType]

    def validate(self) -> None:
        raise InvalidExpression(f"'{self}' is not a valid condition")

    def translate(self) -> str:
        return str(self)

    def __repr__(self) -> str:
        return f"{self.lhs} {self.op} {self.rhs}"

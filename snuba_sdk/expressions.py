from __future__ import annotations

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import Any, List, Optional, Sequence, Set, Union

from snuba_sdk.snuba import check_array_type, is_aggregation_function


class InvalidExpression(Exception):
    pass


class Expression(ABC):
    def __post_init__(self) -> None:
        self.validate()

    @abstractmethod
    def validate(self) -> None:
        raise NotImplementedError


# For type hinting
ScalarLiteralType = Union[None, bool, str, bytes, float, int, date, datetime]
ScalarSequenceType = Sequence[ScalarLiteralType]
ScalarType = Union[ScalarLiteralType, ScalarSequenceType]

# For type checking
Scalar: Set[type] = {
    type(None),
    bool,
    str,
    bytes,
    float,
    int,
    date,
    datetime,
}


class InvalidArray(Exception):
    def __init__(self, value: List[Any]) -> None:
        value_str = f"{value}"
        if len(value_str) > 10:
            value_str = f"{value_str[:10]}...]"

        super().__init__(
            f"invalid array {value_str}: arrays must have the same data type or None, perhaps use a tuple instead"
        )


def is_scalar(value: Any) -> bool:
    if isinstance(value, tuple(Scalar)):
        return True
    elif isinstance(value, tuple):
        if not all(is_scalar(v) for v in value):
            raise InvalidExpression("tuple must contain only scalar values")
        return True
    elif isinstance(value, list):
        if not check_array_type(value):
            raise InvalidArray(value)

        return True

    return False


column_name_re = re.compile(r"^[a-zA-Z][a-zA-Z_.]+$")


def _validate_int_literal(
    name: str, literal: int, minn: Optional[int], maxn: Optional[int]
) -> None:
    if not isinstance(literal, int):
        raise InvalidExpression(f"{name} '{literal}' must be an integer")
    if minn is not None and literal < minn:
        raise InvalidExpression(f"{name} '{literal}' must be at least {minn:,}")
    elif maxn is not None and literal > maxn:
        raise InvalidExpression(f"{name} '{literal}' is capped at {maxn:,}")


@dataclass(frozen=True)
class Limit(Expression):
    limit: int

    def validate(self) -> None:
        _validate_int_literal("limit", self.limit, 1, 10000)


@dataclass(frozen=True)
class Offset(Expression):
    offset: int

    def validate(self) -> None:
        _validate_int_literal("offset", self.offset, 0, None)


@dataclass(frozen=True)
class Granularity(Expression):
    granularity: int

    def validate(self) -> None:
        _validate_int_literal("granularity", self.granularity, 1, None)


@dataclass(frozen=True)
class Totals(Expression):
    totals: bool = False

    def validate(self) -> None:
        if not isinstance(self.totals, bool):
            raise InvalidExpression("totals must be a boolean")


@dataclass(frozen=True)
class Column(Expression):
    name: str

    def validate(self) -> None:
        if not isinstance(self.name, str):
            raise InvalidExpression(f"column '{self.name}' must be a string")
            self.name = str(self.name)
        if not column_name_re.match(self.name):
            raise InvalidExpression(
                f"column '{self.name}' is empty or contains invalid characters"
            )


@dataclass(frozen=True)
class Function(Expression):
    function: str
    parameters: Sequence[Union[ScalarType, Column, Function]]
    alias: Optional[str] = None

    def is_aggregate(self) -> bool:
        if is_aggregation_function(self.function):
            return True

        for param in self.parameters:
            if isinstance(param, Function) and param.is_aggregate():
                return True

        return False

    def validate(self) -> None:
        if not isinstance(self.function, str):
            raise InvalidExpression(f"function '{self.function}' must be a string")
        if self.function == "":
            # TODO: Have a whitelist of valid functions to check, maybe even with more
            # specific parameter type checking
            raise InvalidExpression("function cannot be empty")
        if not column_name_re.match(self.function):
            raise InvalidExpression(
                f"function '{self.function}' contains invalid characters"
            )

        if self.alias is not None:
            if not isinstance(self.alias, str) or self.alias == "":
                raise InvalidExpression(
                    f"alias '{self.alias}' of function {self.function} must be None or a non-empty string"
                )
            if not column_name_re.match(self.alias):
                raise InvalidExpression(
                    f"alias '{self.alias}' of function {self.function} contains invalid characters"
                )

        for param in self.parameters:
            if not isinstance(param, (Column, Function, *Scalar)):
                assert not isinstance(param, bytes)  # mypy
                raise InvalidExpression(
                    f"parameter '{param}' of function {self.function} is an invalid type"
                )

    def __eq__(self, other: object) -> bool:
        # Don't use the alias to compare equality
        if not isinstance(other, Function):
            return False

        return self.function == other.function and self.parameters == other.parameters


class Direction(Enum):
    ASC = "ASC"
    DESC = "DESC"


@dataclass(frozen=True)
class OrderBy(Expression):
    exp: Union[Column, Function]
    direction: Direction

    def validate(self) -> None:
        if not isinstance(self.exp, (Column, Function)):
            raise InvalidExpression("OrderBy expression must be a Column or Function")
        if not isinstance(self.direction, Direction):
            raise InvalidExpression("OrderBy direction must be a Direction")


@dataclass(frozen=True)
class LimitBy(Expression):
    column: Column
    count: int

    def validate(self) -> None:
        if not isinstance(self.column, Column):
            raise InvalidExpression("LimitBy can only be used on a Column")
        if not isinstance(self.count, int) or self.count <= 0 or self.count > 10000:
            raise InvalidExpression(
                "LimitBy count must be a positive integer (max 10,000)"
            )

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Optional, Sequence, Union


class InvalidExpressionError(Exception):
    pass


class Expression(ABC):
    def __post_init__(self) -> None:
        self.validate()

    @abstractmethod
    def validate(self) -> None:
        raise NotImplementedError


ALIAS_RE = re.compile(r"^[a-zA-Z0-9_\.\+\*\/:\-\[\]\(\)\@]*$")


# For type hinting
ScalarLiteralType = Union[None, bool, str, bytes, float, int, date, datetime]
ScalarSequenceType = Sequence[Union[Expression, ScalarLiteralType]]
ScalarType = Union[ScalarLiteralType, ScalarSequenceType]

# For type checking
Scalar: set[type] = {
    type(None),
    bool,
    str,
    bytes,
    float,
    int,
    date,
    datetime,
}


class InvalidArrayError(Exception):
    def __init__(self, value: list[Any]) -> None:
        value_str = f"{value}"
        if len(value_str) > 10:
            value_str = f"{value_str[:10]}...]"

        super().__init__(
            f"invalid array {value_str}: arrays must have the same data type or None, perhaps use a tuple instead"
        )


def is_literal(value: Any) -> bool:
    """
    Allow simple scalar types but not lists/tuples.
    """
    return isinstance(value, tuple(Scalar))


def is_scalar(value: Any) -> bool:
    if isinstance(value, tuple(Scalar)):
        return True
    elif isinstance(value, (tuple, list)):
        if not all(is_scalar(v) or isinstance(v, Expression) for v in value):
            raise InvalidExpressionError("tuple/array must contain only scalar values")
        return True

    return False


def _validate_int_literal(
    name: str, literal: int, minn: Optional[int], maxn: Optional[int]
) -> None:
    if not isinstance(literal, int):
        raise InvalidExpressionError(f"{name} '{literal}' must be an integer")
    if minn is not None and literal < minn:
        raise InvalidExpressionError(f"{name} '{literal}' must be at least {minn:,}")
    elif maxn is not None and literal > maxn:
        raise InvalidExpressionError(f"{name} '{literal}' is capped at {maxn:,}")


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

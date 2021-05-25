from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, List, Optional, Sequence, Set, Union


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
ScalarSequenceType = Sequence[Union[Expression, ScalarLiteralType]]
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
            raise InvalidExpression("tuple/array must contain only scalar values")
        return True

    return False


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
class BooleanFlag(Expression):
    value: bool = False
    name: str = ""

    def validate(self) -> None:
        if not isinstance(self.value, bool):
            raise InvalidExpression(f"{self.name} must be a boolean")

    def __bool__(self) -> bool:
        return self.value

    def __str__(self) -> str:
        return str(self.value)


@dataclass(frozen=True)
class Totals(BooleanFlag):
    name: str = "totals"


@dataclass(frozen=True)
class Consistent(BooleanFlag):
    name: str = "consistent"


@dataclass(frozen=True)
class Turbo(BooleanFlag):
    name: str = "turbo"


@dataclass(frozen=True)
class Debug(BooleanFlag):
    name: str = "debug"


@dataclass(frozen=True)
class DryRun(BooleanFlag):
    name: str = "dry_run"


@dataclass(frozen=True)
class Legacy(BooleanFlag):
    name: str = "legacy"

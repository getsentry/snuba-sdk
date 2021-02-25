import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
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


def is_literal(value: Any) -> bool:
    """
    Allow simple scalar types but not lists/tuples.
    """
    return isinstance(value, tuple(Scalar))


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


alias_re = re.compile(r"^[a-zA-Z](\w|\.)+$")

column_name_re = re.compile(r"^[a-zA-Z](\w|\.|:)*(\[([a-zA-Z](\w|\.|:)*)\])?$")
# In theory the function matcher should be the same as the column one.
# However legacy API sends curried functions as raw strings, and it
# wasn't worth it to import an entire parsing grammar into the SDK
# just to accomodate that one case. Instead, allow it for now and
# once that use case is eliminated we can remove this.
function_name_re = re.compile(r"^[a-zA-Z](\w|[().,]| |\[|\])+$")


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
class Column(Expression):
    """
    A representation of a single column in the database. Columns are
    expected to be alpha-numeric, with '.', '_`, and `:` allowed as well.
    If the column is subscriptable then you can specify the column in the
    form `column[key]`. The `column` attribute will contain the outer
    column and `key` will contain the inner key.

    :param name: The column name.
    :type name: str

    :raises InvalidExpression: If the column name is not a string or has an
        invalid format.

    """

    name: str
    subscriptable: Optional[str] = field(init=False, default=None)
    key: Optional[str] = field(init=False, default=None)

    def validate(self) -> None:
        if not isinstance(self.name, str):
            raise InvalidExpression(f"column '{self.name}' must be a string")
            self.name = str(self.name)
        if not column_name_re.match(self.name):
            raise InvalidExpression(
                f"column '{self.name}' is empty or contains invalid characters"
            )

        # If this is a subscriptable set these values to help with debugging etc.
        # Because this is frozen we can't set the value directly.
        if "[" in self.name:
            subscriptable, key = self.name.split("[", 1)
            key = key.strip("]")
            super().__setattr__("subscriptable", subscriptable)
            super().__setattr__("key", key)


@dataclass(frozen=True)
class CurriedFunction(Expression):
    function: str
    initializers: Optional[Sequence[Union[ScalarLiteralType, Column]]] = None
    parameters: Optional[
        Sequence[Union[ScalarType, Column, "CurriedFunction", "Function"]]
    ] = None
    alias: Optional[str] = None

    def is_aggregate(self) -> bool:
        if is_aggregation_function(self.function):
            return True

        if self.parameters is not None:
            for param in self.parameters:
                if (
                    isinstance(param, (CurriedFunction, Function))
                    and param.is_aggregate()
                ):
                    return True

        return False

    def validate(self) -> None:
        if not isinstance(self.function, str):
            raise InvalidExpression(f"function '{self.function}' must be a string")
        if self.function == "":
            # TODO: Have a whitelist of valid functions to check, maybe even with more
            # specific parameter type checking
            raise InvalidExpression("function cannot be empty")
        if not function_name_re.match(self.function):
            raise InvalidExpression(
                f"function '{self.function}' contains invalid characters"
            )

        if self.initializers is not None:
            if not isinstance(self.initializers, Sequence):
                raise InvalidExpression(
                    f"initializers of function {self.function} must be a Sequence"
                )
            elif not all(
                isinstance(param, Column) or is_literal(param)
                for param in self.initializers
            ):
                raise InvalidExpression(
                    f"initializers to function {self.function} must be a scalar or column"
                )

        if self.alias is not None:
            if not isinstance(self.alias, str) or self.alias == "":
                raise InvalidExpression(
                    f"alias '{self.alias}' of function {self.function} must be None or a non-empty string"
                )
            if not alias_re.match(self.alias):
                raise InvalidExpression(
                    f"alias '{self.alias}' of function {self.function} contains invalid characters"
                )

        if self.parameters is not None:
            if not isinstance(self.parameters, Sequence):
                raise InvalidExpression(
                    f"parameters of function {self.function} must be a Sequence"
                )
            for param in self.parameters:
                if not isinstance(
                    param, (Column, CurriedFunction, Function)
                ) and not is_scalar(param):
                    assert not isinstance(param, bytes)  # mypy
                    raise InvalidExpression(
                        f"parameter '{param}' of function {self.function} is an invalid type"
                    )

    def __eq__(self, other: object) -> bool:
        # Don't use the alias to compare equality
        if not isinstance(other, CurriedFunction):
            return False

        return (
            self.function == other.function
            and self.initializers == other.initializers
            and self.parameters == other.parameters
        )


@dataclass(frozen=True)
class Function(CurriedFunction):
    initializers: Optional[Sequence[Union[ScalarLiteralType, Column]]] = field(
        init=False, default=None
    )


class Direction(Enum):
    ASC = "ASC"
    DESC = "DESC"


@dataclass(frozen=True)
class OrderBy(Expression):
    exp: Union[Column, CurriedFunction, Function]
    direction: Direction

    def validate(self) -> None:
        if not isinstance(self.exp, (Column, CurriedFunction, Function)):
            raise InvalidExpression(
                "OrderBy expression must be a Column, CurriedFunction or Function"
            )
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

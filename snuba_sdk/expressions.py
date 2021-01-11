from __future__ import annotations

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime
from typing import List, Optional, Set, Union

from snuba_sdk.clickhouse import is_aggregation_function


class InvalidExpression(Exception):
    pass


class Expression(ABC):
    def __post_init__(self) -> None:
        self.validate()

    @abstractmethod
    def validate(self) -> None:
        raise NotImplementedError


# For type hinting
ScalarType = Union[None, bool, str, bytes, float, int, date, datetime]
# For type checking
Scalar: Set[type] = {type(None), bool, str, bytes, float, int, date, datetime}

column_name_re = re.compile(r"[a-zA-Z_]+")


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
    parameters: List[Union[ScalarType, Column, Function]]
    alias: Optional[str] = None

    def is_aggregate(self) -> bool:
        return is_aggregation_function(self.function)

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

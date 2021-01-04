from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import List, Optional, Set, Union

from snuba_sdk import Expression


class InvalidExpression(Exception):
    pass


# For type hinting
ScalarType = Union[None, bool, str, bytes, float, int, date, datetime]
# For type checking
Scalar: Set[type] = {type(None), bool, str, bytes, float, int, date, datetime}

# validation regexes
unescaped_quotes = re.compile(r"(?<!\\)'")
unescaped_newline = re.compile(r"(?<!\\)\n")
column_name_re = re.compile(r"[a-zA-Z_][a-zA-Z0-9_\.]*")


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


@dataclass(frozen=True)
class Limit(Expression):
    limit: int

    def validate(self) -> None:
        if not isinstance(self.limit, int):
            raise InvalidExpression(f"limit '{self.limit}' must be an integer")
        if self.limit < 0:
            raise InvalidExpression(f"limit '{self.limit}' cannot be negative")
        elif self.limit > 10000:
            raise InvalidExpression(f"limit '{self.limit}' is capped at 10,000")

    def translate(self) -> str:
        return f"{self.limit:d}"


@dataclass(frozen=True)
class Offset(Expression):
    offset: int

    def validate(self) -> None:
        if not isinstance(self.offset, int):
            raise InvalidExpression(f"offset '{self.offset}' must be an integer")
        if self.offset < 0:
            raise InvalidExpression(f"offset '{self.offset}' cannot be negative")
        elif self.offset > 10000:
            raise InvalidExpression(f"offset '{self.offset}' is capped at 10,000")

    def translate(self) -> str:
        return f"{self.offset:d}"


@dataclass(frozen=True)
class Granularity(Expression):
    granularity: int

    def validate(self) -> None:
        if not isinstance(self.granularity, int):
            raise InvalidExpression(
                f"granularity '{self.granularity}' must be an integer"
            )
        if self.granularity <= 0:
            raise InvalidExpression(
                f"granularity '{self.granularity}' must be greater than 0"
            )

    def translate(self) -> str:
        return f"{self.granularity:d}"


@dataclass(frozen=True)
class Column(Expression):
    name: str

    def validate(self) -> None:
        if not isinstance(self.name, str):
            raise InvalidExpression(f"column '{self.name}' must be a string")
            self.name = str(self.name)
        if not column_name_re.match(self.name):
            raise InvalidExpression(
                f"column '{self}' is empty or contains invalid characters"
            )

    def translate(self) -> str:
        return self.name


@dataclass(frozen=True)
class Function(Expression):
    function: str
    parameters: List[Union[ScalarType, Column, Function]]
    alias: Optional[str] = None

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
            if isinstance(param, (Column, Function, *Scalar)):
                continue
            else:
                assert not isinstance(param, bytes)  # mypy
                raise InvalidExpression(
                    f"parameter '{param}' of function {self.function} is an invalid type"
                )

    def translate(self) -> str:
        alias = "" if self.alias is None else f" AS {self.alias}"
        params = []
        for param in self.parameters:
            if isinstance(param, (Column, Function)):
                params.append(param.translate())
            elif isinstance(param, tuple(Scalar)):
                params.append(_stringify_scalar(param))

        return f"{self.function}({', '.join(params)}){alias}"

from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional, Sequence, Union

from snuba_sdk.column import Column
from snuba_sdk.expressions import Expression, InvalidExpressionError
from snuba_sdk.function import CurriedFunction, Function


class Direction(Enum):
    ASC = "ASC"
    DESC = "DESC"


@dataclass(frozen=True)
class OrderBy(Expression):
    exp: Union[Column, CurriedFunction, Function]
    direction: Direction

    def validate(self) -> None:
        if not isinstance(self.exp, (Column, CurriedFunction, Function)):
            raise InvalidExpressionError(
                "OrderBy expression must be a Column, CurriedFunction or Function"
            )
        if not isinstance(self.direction, Direction):
            raise InvalidExpressionError("OrderBy direction must be a Direction")


def validate_sequence_of_type(
    name: str, val: Any, type: type, minimum_length: Optional[int]
) -> None:
    if not isinstance(val, Sequence):
        raise InvalidExpressionError(f"{name}: '{val}' must be a sequence of {type}")
    if minimum_length is not None and len(val) < minimum_length:
        raise InvalidExpressionError(
            f"{name}: '{val}' must be contain at least {minimum_length} elements"
        )
    for el in val:
        if not isinstance(el, type):
            raise InvalidExpressionError(
                f"{name}: Invalid element '{el}' which must be a list composed entirely of {type}"
            )


@dataclass(frozen=True)
class LimitBy(Expression):
    columns: Sequence[Column]
    count: int

    def validate(self) -> None:
        validate_sequence_of_type("LimitBy columns", self.columns, Column, 1)
        if not isinstance(self.count, int) or self.count <= 0 or self.count > 10000:
            raise InvalidExpressionError(
                "LimitBy count must be a positive integer (max 10,000)"
            )

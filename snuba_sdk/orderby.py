from dataclasses import dataclass
from enum import Enum
from typing import Sequence, Union

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


@dataclass(frozen=True)
class LimitBy(Expression):
    columns: Sequence[Column]
    count: int

    def validate(self) -> None:
        if (
            not isinstance(self.columns, Sequence)
            or len(self.columns) < 1
            or not isinstance(self.columns[0], Column)
        ):
            raise InvalidExpressionError(
                "LimitBy can only be used on a Column or multiple Columns"
            )
        if not isinstance(self.count, int) or self.count <= 0 or self.count > 10000:
            raise InvalidExpressionError(
                "LimitBy count must be a positive integer (max 10,000)"
            )

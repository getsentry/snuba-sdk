from dataclasses import dataclass
from enum import Enum
from typing import Union

from snuba_sdk.column import Column
from snuba_sdk.function import CurriedFunction, Function
from snuba_sdk.expressions import Expression, InvalidExpression


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

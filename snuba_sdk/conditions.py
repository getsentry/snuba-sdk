from dataclasses import dataclass
from enum import Enum
from typing import Union

from snuba_sdk import Expression
from snuba_sdk.expressions import (
    Column,
    Function,
    InvalidExpression,
    Scalar,
    ScalarType,
    _stringify_scalar,
)


class Op(Enum):
    GT = ">"
    LT = "<"
    GTE = ">="
    LTE = "<="
    EQ = "="
    NEQ = "!="
    IN = "IN"
    NOT_IN = "NOT IN"
    LIKE = "LIKE"
    NOT_LIKE = "NOT LIKE"


@dataclass(frozen=True)
class Condition(Expression):
    lhs: Union[Column, Function]
    op: Op
    rhs: Union[Column, Function, ScalarType]

    def validate(self) -> None:
        if not isinstance(self.lhs, (Column, Function)):
            raise InvalidExpression(
                f"invalid condition: LHS of a condition must be a Column or Function, not {type(self.lhs)}"
            )
        if not isinstance(self.rhs, (Column, Function, *Scalar)):
            raise InvalidExpression(
                f"invalid condition: RHS of a condition must be a Column, Function or Scalar not {type(self.rhs)}"
            )
        if not isinstance(self.op, Op):
            raise InvalidExpression(
                "invalid condition: operator of a condition must be an Op"
            )

    def translate(self) -> str:
        return str(self)

    def __str__(self) -> str:
        if isinstance(self.rhs, (Column, Function)):
            rhs = self.rhs.translate()
        elif isinstance(self.rhs, *Scalar):
            rhs = _stringify_scalar(self.rhs)

        return f"{self.lhs.translate()} {self.op.value} {rhs}"

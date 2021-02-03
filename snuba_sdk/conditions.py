from dataclasses import dataclass
from enum import Enum
from typing import Union

from snuba_sdk.expressions import (
    Column,
    CurriedFunction,
    Expression,
    Function,
    InvalidExpression,
    is_scalar,
    ScalarType,
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
    lhs: Union[Column, CurriedFunction, Function]
    op: Op
    rhs: Union[Column, CurriedFunction, Function, ScalarType]

    def validate(self) -> None:
        if not isinstance(self.lhs, (Column, CurriedFunction, Function)):
            raise InvalidExpression(
                f"invalid condition: LHS of a condition must be a Column, CurriedFunction or Function, not {type(self.lhs)}"
            )
        if not isinstance(
            self.rhs, (Column, CurriedFunction, Function)
        ) and not is_scalar(self.rhs):
            raise InvalidExpression(
                f"invalid condition: RHS of a condition must be a Column, CurriedFunction, Function or Scalar not {type(self.rhs)}"
            )
        if not isinstance(self.op, Op):
            raise InvalidExpression(
                "invalid condition: operator of a condition must be an Op"
            )

from dataclasses import dataclass
from enum import Enum
from typing import Sequence, Union

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


class BooleanOp(Enum):
    AND = "AND"
    OR = "OR"


@dataclass(frozen=True)
class BooleanCondition(Expression):
    op: BooleanOp
    conditions: Sequence[Union["BooleanCondition", Condition]]

    def validate(self) -> None:
        if not isinstance(self.op, BooleanOp):
            raise InvalidExpression(
                "invalid boolean: operator of a boolean must be a BooleanOp"
            )

        if not isinstance(self.conditions, (list, tuple)):
            raise InvalidExpression(
                "invalid boolean: conditions must be a list of other conditions"
            )
        elif len(self.conditions) < 2:
            raise InvalidExpression(
                "invalid boolean: must supply at least two conditions"
            )

        for con in self.conditions:
            if not isinstance(con, (Condition, BooleanCondition)):
                raise InvalidExpression(
                    f"invalid boolean: {con} is not a valid condition"
                )

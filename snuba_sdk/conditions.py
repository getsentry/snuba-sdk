from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Sequence, Union

from snuba_sdk.column import Column
from snuba_sdk.function import CurriedFunction, Function
from snuba_sdk.expressions import (
    Expression,
    InvalidExpression,
    is_scalar,
    ScalarType,
)


class InvalidCondition(InvalidExpression):
    pass


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
    IS_NULL = "IS NULL"
    IS_NOT_NULL = "IS NOT NULL"


def is_unary(op: Op) -> bool:
    return op in [Op.IS_NULL, Op.IS_NOT_NULL]


@dataclass(frozen=True)
class Condition(Expression):
    lhs: Union[Column, CurriedFunction, Function]
    op: Op
    rhs: Optional[Union[Column, CurriedFunction, Function, ScalarType]] = None

    def validate(self) -> None:
        if not isinstance(self.lhs, (Column, CurriedFunction, Function)):
            raise InvalidCondition(
                f"invalid condition: LHS of a condition must be a Column, CurriedFunction or Function, not {type(self.lhs)}"
            )
        if not isinstance(self.op, Op):
            raise InvalidCondition(
                "invalid condition: operator of a condition must be an Op"
            )

        if is_unary(self.op):
            if self.rhs is not None:
                raise InvalidCondition(
                    "invalid condition: unary operators don't have rhs conditions"
                )

        if not isinstance(
            self.rhs, (Column, CurriedFunction, Function)
        ) and not is_scalar(self.rhs):
            raise InvalidCondition(
                f"invalid condition: RHS of a condition must be a Column, CurriedFunction, Function or Scalar not {type(self.rhs)}"
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
            raise InvalidCondition(
                "invalid boolean: operator of a boolean must be a BooleanOp"
            )

        if not isinstance(self.conditions, (list, tuple)):
            raise InvalidCondition(
                "invalid boolean: conditions must be a list of other conditions"
            )
        elif len(self.conditions) < 2:
            raise InvalidCondition(
                "invalid boolean: must supply at least two conditions"
            )

        for con in self.conditions:
            if not isinstance(con, (Condition, BooleanCondition)):
                raise InvalidCondition(
                    f"invalid boolean: {con} is not a valid condition"
                )


@dataclass(frozen=True)
class And(BooleanCondition):
    op: BooleanOp = field(init=False, default=BooleanOp.AND)
    conditions: Sequence[Union[BooleanCondition, Condition]] = field(
        default_factory=list
    )


@dataclass(frozen=True)
class Or(BooleanCondition):
    op: BooleanOp = field(init=False, default=BooleanOp.OR)
    conditions: Sequence[Union[BooleanCondition, Condition]] = field(
        default_factory=list
    )

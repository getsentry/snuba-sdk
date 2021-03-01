from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Sequence, Union

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
    IS_NULL = "IS NULL"
    IS_NOT_NULL = "IS NOT NULL"


@dataclass(frozen=True)
class Condition(Expression):
    lhs: Union[Column, CurriedFunction, Function]
    op: Op
    rhs: Optional[Union[Column, CurriedFunction, Function, ScalarType]] = None

    def is_unary(self) -> bool:
        return self.op in set([Op.IS_NULL, Op.IS_NOT_NULL])

    def validate(self) -> None:
        if not isinstance(self.lhs, (Column, CurriedFunction, Function)):
            raise InvalidExpression(
                f"invalid condition: LHS of a condition must be a Column, CurriedFunction or Function, not {type(self.lhs)}"
            )
        if not isinstance(self.op, Op):
            raise InvalidExpression(
                "invalid condition: operator of a condition must be an Op"
            )

        if self.is_unary():
            if self.rhs is not None:
                raise InvalidExpression(
                    "invalid condition: unary operators don't have rhs conditions"
                )

        if not isinstance(
            self.rhs, (Column, CurriedFunction, Function)
        ) and not is_scalar(self.rhs):
            raise InvalidExpression(
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

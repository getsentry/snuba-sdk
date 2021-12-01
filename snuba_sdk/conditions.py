from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Mapping, Optional, Sequence, Union

from snuba_sdk.column import Column
from snuba_sdk.expressions import (
    Expression,
    InvalidExpressionError,
    ScalarType,
    is_scalar,
)
from snuba_sdk.function import CurriedFunction, Function


class InvalidConditionError(InvalidExpressionError):
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


class ConditionFunction(Enum):
    EQ = "equals"
    NEQ = "notEquals"
    LTE = "lessOrEquals"
    GTE = "greaterOrEquals"
    LT = "less"
    GT = "greater"
    IS_NULL = "isNull"
    IS_NOT_NULL = "isNotNull"
    LIKE = "like"
    NOT_LIKE = "notLike"
    IN = "in"
    NOT_IN = "notIn"


OPERATOR_TO_FUNCTION: Mapping[Op, ConditionFunction] = {
    Op.GT: ConditionFunction.GT,
    Op.LT: ConditionFunction.LT,
    Op.GTE: ConditionFunction.GTE,
    Op.LTE: ConditionFunction.LTE,
    Op.EQ: ConditionFunction.EQ,
    Op.NEQ: ConditionFunction.NEQ,
    Op.IN: ConditionFunction.IN,
    Op.NOT_IN: ConditionFunction.NOT_IN,
    Op.IS_NULL: ConditionFunction.IS_NULL,
    Op.IS_NOT_NULL: ConditionFunction.IS_NOT_NULL,
    Op.LIKE: ConditionFunction.LIKE,
    Op.NOT_LIKE: ConditionFunction.NOT_LIKE,
}


def is_unary(op: Op) -> bool:
    return op in [Op.IS_NULL, Op.IS_NOT_NULL]


@dataclass(frozen=True)
class Condition(Expression):
    lhs: Union[Column, CurriedFunction, Function]
    op: Op
    rhs: Optional[Union[Column, CurriedFunction, Function, ScalarType]] = None

    def validate(self) -> None:
        if not isinstance(self.lhs, (Column, CurriedFunction, Function)):
            raise InvalidConditionError(
                f"invalid condition: LHS of a condition must be a Column, CurriedFunction or Function, not {type(self.lhs)}"
            )
        if not isinstance(self.op, Op):
            raise InvalidConditionError(
                "invalid condition: operator of a condition must be an Op"
            )

        if is_unary(self.op):
            if self.rhs is not None:
                raise InvalidConditionError(
                    "invalid condition: unary operators don't have rhs conditions"
                )

        if not isinstance(
            self.rhs, (Column, CurriedFunction, Function)
        ) and not is_scalar(self.rhs):
            raise InvalidConditionError(
                f"invalid condition: RHS of a condition must be a Column, CurriedFunction, Function or Scalar not {type(self.rhs)}"
            )


class BooleanOp(Enum):
    AND = "AND"
    OR = "OR"


@dataclass(frozen=True)
class BooleanCondition(Expression):
    op: BooleanOp
    conditions: ConditionGroup

    def validate(self) -> None:
        if not isinstance(self.op, BooleanOp):
            raise InvalidConditionError(
                "invalid boolean: operator of a boolean must be a BooleanOp"
            )

        if not isinstance(self.conditions, (list, tuple)):
            raise InvalidConditionError(
                "invalid boolean: conditions must be a list of other conditions"
            )
        elif len(self.conditions) < 2:
            raise InvalidConditionError(
                "invalid boolean: must supply at least two conditions"
            )

        for con in self.conditions:
            if not isinstance(con, (Condition, BooleanCondition)):
                raise InvalidConditionError(
                    f"invalid boolean: {con} is not a valid condition"
                )


@dataclass(frozen=True)
class And(BooleanCondition):
    op: BooleanOp = field(init=False, default=BooleanOp.AND)
    conditions: ConditionGroup = field(default_factory=list)


@dataclass(frozen=True)
class Or(BooleanCondition):
    op: BooleanOp = field(init=False, default=BooleanOp.OR)
    conditions: ConditionGroup = field(default_factory=list)


ConditionGroup = Sequence[Union[BooleanCondition, Condition]]


def get_first_level_and_conditions(conditions: ConditionGroup) -> ConditionGroup:
    flattened = []
    for cond in conditions:
        if isinstance(cond, And):
            top_level = get_first_level_and_conditions(cond.conditions)
            flattened += [*top_level]
        else:
            flattened.append(cond)

    return flattened

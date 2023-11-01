from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum
from typing import Any, Optional, Sequence, Union

from snuba_sdk.aliased_expression import AliasedExpression
from snuba_sdk.column import Column
from snuba_sdk.conditions import BooleanCondition, Condition, ConditionGroup
from snuba_sdk.expressions import InvalidExpressionError, list_type
from snuba_sdk.timeseries import Timeseries


class InvalidArithmeticError(InvalidExpressionError):
    pass


class InvalidFormulaError(InvalidExpressionError):
    pass


class ArithmeticOperator(Enum):
    PLUS = "plus"
    MINUS = "minus"
    MULTIPLY = "multiply"
    DIVIDE = "divide"


@dataclass(frozen=True)
class Formula:
    operator: ArithmeticOperator
    parameters: Optional[Sequence[FormulaParameterGroup]] = None
    filters: Optional[ConditionGroup] = None
    groupby: Optional[list[Column | AliasedExpression]] = None

    def validate(self) -> None:
        if not isinstance(self.operator, ArithmeticOperator):
            raise InvalidFormulaError(
                f"formula '{self.operator}' must be a ArithmeticOperator"
            )
        if self.parameters is not None:
            if not isinstance(self.parameters, Sequence):
                raise InvalidFormulaError(
                    f"parameters of formula {self.operator.value} must be a Sequence"
                )
            for param in self.parameters:
                if not isinstance(param, (Formula, Timeseries, float, int)):
                    raise InvalidFormulaError(
                        f"parameter '{param}' of formula {self.operator.value} is an invalid type"
                    )

    def _replace(self, field: str, value: Any) -> Formula:
        new = replace(self, **{field: value})
        return new

    def set_parameters(self, parameters: Sequence[FormulaParameterGroup]) -> Formula:
        if parameters is not None and not list_type(
            parameters, (Formula, Timeseries, float, int)
        ):
            raise InvalidFormulaError(
                "parameters must be a list of either Formulas, Timeseries, floats, or ints"
            )
        return self._replace("parameters", parameters)

    def set_filters(self, filters: ConditionGroup | None) -> Formula:
        if filters is not None and not list_type(
            filters, (BooleanCondition, Condition)
        ):
            raise InvalidFormulaError("filters must be a list of Conditions")
        return self._replace("filters", filters)

    def set_groupby(self, groupby: list[Column | AliasedExpression] | None) -> Formula:
        if groupby is not None and not list_type(groupby, (Column, AliasedExpression)):
            raise InvalidFormulaError("groupby must be a list of Columns")
        return self._replace("groupby", groupby)


FormulaParameterGroup = Union[Formula, Timeseries, float, int]

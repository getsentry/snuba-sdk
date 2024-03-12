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


PREFIX_TO_INFIX: dict[str, str] = {
    ArithmeticOperator.PLUS.value: "+",
    ArithmeticOperator.MINUS.value: "-",
    ArithmeticOperator.MULTIPLY.value: "*",
    ArithmeticOperator.DIVIDE.value: "/",
}

PREFIX_ALIASES: dict[str, str] = {"negate": "-"}


@dataclass(frozen=True)
class Formula:
    function_name: str
    parameters: Optional[Sequence[FormulaParameterGroup]] = None
    aggregate_params: list[Any] | None = None
    filters: Optional[ConditionGroup] = None
    groupby: Optional[list[Column | AliasedExpression]] = None

    def __validate_consistency(self) -> None:
        """
        Ensure that the groupby columns are consistent across all Timeseries
        and Formulas within this Formula."""
        if self.parameters is None:
            raise InvalidFormulaError("Formula must have parameters")

        groupbys = set()
        has_timeseries = False

        stack: list[FormulaParameterGroup] = [self]
        while stack:
            param = stack.pop()
            if isinstance(param, Formula):
                if param.groupby is not None:
                    groupbys.add(tuple(param.groupby))

                if param.parameters:
                    stack.extend(param.parameters)
            elif isinstance(param, Timeseries):
                has_timeseries = True
                if param.groupby is not None:
                    groupbys.add(tuple(param.groupby))

        if not has_timeseries:
            raise InvalidFormulaError(
                "Formulas must operate on at least one Timeseries"
            )
        if len(set(groupbys)) > 1:
            raise InvalidFormulaError(
                "Formula parameters must group by the same columns"
            )

    def validate(self) -> None:
        if not isinstance(self.function_name, str):
            raise InvalidFormulaError(f"formula '{self.function_name}' must be a str")
        if self.parameters is None:
            raise InvalidFormulaError("Formula must have parameters")
        elif not isinstance(self.parameters, Sequence):
            raise InvalidFormulaError(
                f"parameters of formula {self.function_name} must be a Sequence"
            )

        for param in self.parameters:
            if not isinstance(param, tuple(FormulaParameter)):
                raise InvalidFormulaError(
                    f"parameter '{param}' of formula {self.function_name} is an invalid type"
                )
        self.__validate_consistency()

    def _replace(self, field: str, value: Any) -> Formula:
        new = replace(self, **{field: value})
        return new

    def set_parameters(self, parameters: Sequence[FormulaParameterGroup]) -> Formula:
        if parameters is not None and not list_type(
            parameters, (Formula, Timeseries, float, int)
        ):
            raise InvalidFormulaError(
                "parameters must be a list of either Timeseries, floats, or ints"
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


FormulaParameterGroup = Union[Formula, Timeseries, float, int, str]
FormulaParameter = {Formula, Timeseries, float, int}

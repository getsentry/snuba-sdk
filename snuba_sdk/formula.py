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


PREFIX_TO_INFIX: dict[ArithmeticOperator, str] = {
    ArithmeticOperator.PLUS: "+",
    ArithmeticOperator.MINUS: "-",
    ArithmeticOperator.MULTIPLY: "*",
    ArithmeticOperator.DIVIDE: "/",
}


@dataclass(frozen=True)
class Formula:
    operator: ArithmeticOperator
    parameters: Optional[Sequence[FormulaParameterGroup]] = None
    filters: Optional[ConditionGroup] = None
    groupby: Optional[list[Column | AliasedExpression]] = None

    def __validate_consistency(self) -> tuple[str, list[Column | AliasedExpression]]:
        """
        Ensure that the entity and groupby columns are consistent across all Timeseries
        and Formulas within this Formula."""
        if self.parameters is None:
            raise InvalidFormulaError("Formula must have parameters")

        entities = set()
        groupbys = []
        groupbys.append(
            tuple(g for g in self.groupby) if self.groupby is not None else tuple()
        )
        for param in self.parameters:
            if isinstance(param, Formula):
                entity, found_gpby = param.__validate_consistency()
                entities.add(entity)
                groupbys.append(tuple(found_gpby))
            elif isinstance(param, Timeseries):
                if param.metric.entity is not None:
                    entities.add(param.metric.entity)
                if param.groupby is not None:
                    groupbys.append(tuple(param.groupby))

        if len(entities) != 1:
            raise InvalidFormulaError("Formulas must operate on a single entity")
        if len(set(groupbys)) != 1:
            raise InvalidFormulaError(
                "Formula parameters must group by the same columns"
            )

        return entities.pop(), list(groupbys[0])

    def validate(self) -> None:
        if not isinstance(self.operator, ArithmeticOperator):
            raise InvalidFormulaError(
                f"formula '{self.operator}' must be a ArithmeticOperator"
            )
        if self.parameters is None:
            raise InvalidFormulaError("Formula must have parameters")
        elif not isinstance(self.parameters, Sequence):
            raise InvalidFormulaError(
                f"parameters of formula {self.operator.value} must be a Sequence"
            )

        for param in self.parameters:
            if not isinstance(param, tuple(FormulaParameter)):
                raise InvalidFormulaError(
                    f"parameter '{param}' of formula {self.operator.value} is an invalid type"
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


FormulaParameterGroup = Union[Formula, Timeseries, float, int]
FormulaParameter = {Formula, Timeseries, float, int}

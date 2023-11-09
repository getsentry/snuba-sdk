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

            # - Must have the same entities, and must have an entity (no formulas on just literals)
            # - Must have the same groupbys
            entity = None
            groupby = None
            for param in self.parameters:
                if not isinstance(param, (Timeseries, float, int)):
                    raise InvalidFormulaError(
                        f"parameter '{param}' of formula {self.operator.value} is an invalid type"
                    )

                if isinstance(param, Timeseries):
                    if entity is None:
                        entity = param.metric.entity
                    elif entity != param.metric.entity:
                        raise InvalidFormulaError(
                            "Formulas can only operate on a single entity"
                        )

                    to_check = set(param.groupby) if param.groupby is not None else None
                    if groupby is None:
                        groupby = to_check
                    elif groupby != to_check:
                        raise InvalidFormulaError(
                            "Formula parameters must group by the same columns"
                        )

            if not entity:
                raise InvalidFormulaError("Formulas must have an an entity")

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


FormulaParameterGroup = Union[Timeseries, float, int]

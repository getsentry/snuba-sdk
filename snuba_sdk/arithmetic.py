from __future__ import annotations

from enum import Enum

from snuba_sdk.expressions import InvalidExpressionError


class InvalidArithmeticError(InvalidExpressionError):
    pass

class ArithmeticFunction(Enum):
    PLUS = "plus"
    MINUS = "minus"
    MULTIPLY = "multiply"
    DIVIDE = "divide"

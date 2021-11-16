from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional, Sequence, Union

from snuba_sdk.column import Column
from snuba_sdk.expressions import (
    ALIAS_RE,
    Expression,
    InvalidExpressionError,
    ScalarLiteralType,
    ScalarType,
    is_literal,
    is_scalar,
)


class InvalidFunctionError(InvalidExpressionError):
    pass


# In theory the function matcher should be the same as the column one.
# However legacy API sends curried functions as raw strings, and it
# wasn't worth it to import an entire parsing grammar into the SDK
# just to accomodate that one case. Instead, allow it for now and
# once that use case is eliminated we can remove this.
function_name_re = re.compile(r"""^[a-zA-Z](\w|[().,+'":]| |\[|\]|\-)+$""")


@dataclass(frozen=True)
class CurriedFunction(Expression):
    function: str
    initializers: Optional[Sequence[Union[ScalarLiteralType, Column]]] = None
    parameters: Optional[
        Sequence[Union[ScalarType, Column, CurriedFunction, Function]]
    ] = None
    alias: Optional[str] = None

    def validate(self) -> None:
        if not isinstance(self.function, str):
            raise InvalidFunctionError(f"function '{self.function}' must be a string")
        if self.function == "":
            # TODO: Have a whitelist of valid functions to check, maybe even with more
            # specific parameter type checking
            raise InvalidFunctionError("function cannot be empty")
        if not function_name_re.match(self.function):
            raise InvalidFunctionError(
                f"function '{self.function}' contains invalid characters"
            )

        if self.initializers is not None:
            if not isinstance(self.initializers, Sequence):
                raise InvalidFunctionError(
                    f"initializers of function {self.function} must be a Sequence"
                )
            elif not all(
                isinstance(param, Column) or is_literal(param)
                for param in self.initializers
            ):
                raise InvalidFunctionError(
                    f"initializers to function {self.function} must be a scalar or column"
                )

        if self.alias is not None:
            if not isinstance(self.alias, str) or self.alias == "":
                raise InvalidFunctionError(
                    f"alias '{self.alias}' of function {self.function} must be None or a non-empty string"
                )
            if not ALIAS_RE.match(self.alias):
                raise InvalidFunctionError(
                    f"alias '{self.alias}' of function {self.function} contains invalid characters"
                )

        if self.parameters is not None:
            if not isinstance(self.parameters, Sequence):
                raise InvalidFunctionError(
                    f"parameters of function {self.function} must be a Sequence"
                )
            for param in self.parameters:
                if not isinstance(
                    param, (Column, CurriedFunction, Function)
                ) and not is_scalar(param):
                    assert not isinstance(param, bytes)  # mypy
                    raise InvalidFunctionError(
                        f"parameter '{param}' of function {self.function} is an invalid type"
                    )

    def __eq__(self, other: object) -> bool:
        # Don't use the alias to compare equality
        if not isinstance(other, CurriedFunction):
            return False

        return (
            self.function == other.function
            and self.initializers == other.initializers
            and self.parameters == other.parameters
        )


@dataclass(frozen=True)
class Function(CurriedFunction):
    initializers: Optional[Sequence[Union[ScalarLiteralType, Column]]] = field(
        init=False, default=None
    )

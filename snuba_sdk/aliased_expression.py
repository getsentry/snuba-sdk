from dataclasses import dataclass
from typing import Optional

from snuba_sdk.column import Column
from snuba_sdk.expressions import ALIAS_RE, Expression, InvalidExpressionError


@dataclass(frozen=True)
class AliasedExpression(Expression):
    """
    Used to alias the name of an expression in the results of a query. It is not used
    anywhere in Snuba except to change the names in the results set. Right now this is
    limited to Columns only because Functions have a separate alias. Eventually the
    two will be combined.

    :param Expression: The expression to alias.
    :type Expression: Column
    :raises InvalidExpressionError: If the expression or alias is invalid.
    """

    # TODO: We should eventually allow Functions here as well, once we think through
    # how this should work with functions that already have aliases.
    exp: Column
    alias: Optional[str] = None

    def validate(self) -> None:
        if not isinstance(self.exp, Column):
            raise InvalidExpressionError(
                "aliased expressions can only contain a Column"
            )

        if self.alias is not None:
            if not isinstance(self.alias, str) or self.alias == "":
                raise InvalidExpressionError(
                    f"alias '{self.alias}' of expression must be None or a non-empty string"
                )
            if not ALIAS_RE.match(self.alias):
                raise InvalidExpressionError(
                    f"alias '{self.alias}' of expression contains invalid characters"
                )

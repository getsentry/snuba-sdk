import re
from typing import Optional

import pytest

from snuba_sdk.aliased_expression import AliasedExpression
from snuba_sdk.column import Column
from snuba_sdk.expressions import InvalidExpression
from snuba_sdk.visitors import Translation

tests = [
    pytest.param(Column("stuff"), "things", "stuff AS things", None, id="simple"),
    pytest.param(
        Column("stuff"),
        "things[1.c-2:3_]",
        "stuff AS things[1.c-2:3_]",
        None,
        id="complex alias",
    ),
    pytest.param(
        "stuff",
        "things[1.c-2:3]",
        None,
        InvalidExpression("aliased expressions can only contain a Column"),
        id="exp must be a Column",
    ),
    pytest.param(
        Column("stuff"),
        "",
        None,
        InvalidExpression("alias '' of expression must be None or a non-empty string"),
        id="alias can't be empty string",
    ),
    pytest.param(
        Column("stuff"),
        1,
        None,
        InvalidExpression("alias '1' of expression must be None or a non-empty string"),
        id="alias must be string",
    ),
    pytest.param(
        Column("stuff"),
        "what???||things!!",
        None,
        InvalidExpression(
            "alias 'what???||things!!' of expression contains invalid characters"
        ),
        id="alias has invalid characters",
    ),
]


TRANSLATOR = Translation(use_entity_aliases=True)


@pytest.mark.parametrize("exp, alias, translated, exception", tests)
def test_aliased_expression(
    exp: Column,
    alias: Optional[str],
    translated: str,
    exception: Optional[Exception],
) -> None:
    if exception is not None:
        with pytest.raises(type(exception), match=re.escape(str(exception))):
            AliasedExpression(exp, alias)
    else:
        aliased = AliasedExpression(exp, alias)
        assert TRANSLATOR.visit(aliased) == translated

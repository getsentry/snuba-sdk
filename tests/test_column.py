import pytest
from typing import Optional

from snuba_sdk.expressions import (
    Column,
    Expression,
    InvalidExpression,
)
from snuba_sdk.visitors import Translation

tests = [
    pytest.param("valid", Column("valid"), "valid", None, id="basic column test"),
    pytest.param(
        "_valid", Column("_valid"), "_valid", None, id="underscore column test"
    ),
    pytest.param(
        "_valid.stuff",
        Column("_valid.stuff"),
        "_valid.stuff",
        None,
        id="dot column test",
    ),
    pytest.param(
        "..valid",
        None,
        None,
        InvalidExpression("column '..valid' is empty or contains invalid characters"),
        id="invalid column",
    ),
    pytest.param(
        10,
        None,
        None,
        InvalidExpression("column '10' must be a string"),
        id="invalid column type",
    ),
    pytest.param(
        "",
        None,
        None,
        InvalidExpression("column '' is empty or contains invalid characters"),
        id="empty column",
    ),
]


TRANSLATOR = Translation()


@pytest.mark.parametrize("column_name, valid, translated, exception", tests)
def test_columns(
    column_name: str,
    valid: Expression,
    translated: str,
    exception: Optional[Exception],
) -> None:
    def verify() -> None:
        exp = Column(column_name)
        assert exp == valid
        assert TRANSLATOR.visit(exp) == translated

    if exception is not None:
        with pytest.raises(type(exception), match=str(exception)):
            verify()
    else:
        verify()

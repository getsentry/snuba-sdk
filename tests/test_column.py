import pytest  # type: ignore
from typing import Optional

from snuba_sdk import Expression
from snuba_sdk.expressions import Column, InvalidExpression

tests = [
    pytest.param("valid", Column("valid"), "valid", None, id="basic column test"),
    pytest.param(
        "..valid",
        None,
        None,
        InvalidExpression("'..valid' contains invalid characters"),
        id="invalid column",
    ),
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
]


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
        assert exp.translate() == translated

    if exception is not None:
        with pytest.raises(type(exception), match=str(exception)):
            verify()
    else:
        verify()

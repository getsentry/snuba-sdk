import pytest
from typing import Optional

from snuba_sdk import Expression
from snuba_sdk.expressions import Column

tests = [pytest.param("valid", Column("valid"), "valid", None, id="basic column test")]


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

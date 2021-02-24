import pytest
import re
from typing import Optional, Tuple

from snuba_sdk.expressions import (
    Column,
    InvalidExpression,
)
from snuba_sdk.visitors import Translation

tests = [
    pytest.param("valid", ("valid", None, None), "valid", None, id="basic column test"),
    pytest.param(
        "valid_", ("valid_", None, None), "valid_", None, id="underscore column test"
    ),
    pytest.param(
        "va_lid.stuff",
        ("va_lid.stuff", None, None),
        "va_lid.stuff",
        None,
        id="dot column test",
    ),
    pytest.param(
        "va_lid:stuff",
        ("va_lid:stuff", None, None),
        "va_lid:stuff",
        None,
        id="colon column test",
    ),
    pytest.param(
        "a[b]", ("a[b]", "a", "b"), "a[b]", None, id="single char subscriptable"
    ),
    pytest.param(
        "a1[a1]", ("a1[a1]", "a1", "a1"), "a1[a1]", None, id="number subscriptable"
    ),
    pytest.param(
        "a1[a.2]", ("a1[a.2]", "a1", "a.2"), "a1[a.2]", None, id="dot subscriptable"
    ),
    pytest.param(
        "a1.2[a:bdsd]",
        ("a1.2[a:bdsd]", "a1.2", "a:bdsd"),
        "a1.2[a:bdsd]",
        None,
        id="colon subscriptable",
    ),
    pytest.param(
        "a1[a:b][aasdc]",
        None,
        None,
        InvalidExpression(
            "column 'a1[a:b][aasdc]' is empty or contains invalid characters"
        ),
        id="only one subscriptable",
    ),
    pytest.param(
        "a1[a?]",
        None,
        None,
        InvalidExpression("column 'a1[a?]' is empty or contains invalid characters"),
        id="invalid subscriptable",
    ),
    pytest.param(
        "_valid",
        None,
        None,
        InvalidExpression("column '_valid' is empty or contains invalid characters"),
        id="underscore column test",
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
    valid: Tuple[str, Optional[str], Optional[str]],
    translated: str,
    exception: Optional[Exception],
) -> None:
    def verify() -> None:
        exp = Column(column_name)
        assert exp.name == valid[0]
        assert exp.column == valid[1]
        assert exp.key == valid[2]
        assert TRANSLATOR.visit(exp) == translated

    if exception is not None:
        with pytest.raises(type(exception), match=re.escape(str(exception))):
            verify()
    else:
        verify()

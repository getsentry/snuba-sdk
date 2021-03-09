import pytest
import re
from typing import Any, Optional, Tuple

from snuba_sdk.column import Column, InvalidColumn
from snuba_sdk.entity import Entity
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
        InvalidColumn(
            "column 'a1[a:b][aasdc]' is empty or contains invalid characters"
        ),
        id="only one subscriptable",
    ),
    pytest.param(
        "a1[a?]",
        None,
        None,
        InvalidColumn("column 'a1[a?]' is empty or contains invalid characters"),
        id="invalid subscriptable",
    ),
    pytest.param(
        "_valid",
        None,
        None,
        InvalidColumn("column '_valid' is empty or contains invalid characters"),
        id="underscore column test",
    ),
    pytest.param(
        "..valid",
        None,
        None,
        InvalidColumn("column '..valid' is empty or contains invalid characters"),
        id="invalid column",
    ),
    pytest.param(
        10,
        None,
        None,
        InvalidColumn("column '10' must be a string"),
        id="invalid column type",
    ),
    pytest.param(
        "",
        None,
        None,
        InvalidColumn("column '' is empty or contains invalid characters"),
        id="empty column",
    ),
]


TRANSLATOR = Translation(use_entity_aliases=True)


@pytest.mark.parametrize("column_name, valid, translated, exception", tests)
def test_columns(
    column_name: str,
    valid: Tuple[str, Optional[str], Optional[str]],
    translated: str,
    exception: Optional[Exception],
) -> None:
    if exception is not None:
        with pytest.raises(type(exception), match=re.escape(str(exception))):
            Column(column_name)
    else:
        exp = Column(column_name)
        assert exp.name == valid[0]
        assert exp.subscriptable == valid[1]
        assert exp.key == valid[2]
        assert TRANSLATOR.visit(exp) == translated


entity_tests = [
    pytest.param("foo", Entity("events", "e"), "e.foo", None, id="column with entity"),
    pytest.param(
        "foo",
        Entity("events"),
        None,
        InvalidColumn("column foo expects an Entity with an alias"),
        id="column with entity but no alias",
    ),
    pytest.param(
        "foo",
        "events",
        None,
        InvalidColumn("column foo expects an Entity"),
        id="column with non-entity",
    ),
]


@pytest.mark.parametrize("column_name, entity, translated, exception", entity_tests)
def test_columns_with_entities(
    column_name: str,
    entity: Any,
    translated: str,
    exception: Optional[Exception],
) -> None:
    if exception is not None:
        with pytest.raises(type(exception), match=re.escape(str(exception))):
            Column(column_name, entity)
    else:
        exp = Column(column_name, entity)
        assert exp.name == column_name
        assert exp.entity == entity
        assert TRANSLATOR.visit(exp) == translated

import re
from typing import Any, Optional, Tuple

import pytest

from snuba_sdk.column import Column, InvalidColumn
from snuba_sdk.entity import Entity
from snuba_sdk.visitors import Translation, output_aliases

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
        ":_valid",
        None,
        None,
        InvalidColumn("column ':_valid' is empty or contains invalid characters"),
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
    valid: Tuple[str, Optional[str], Optional[str], Optional[str]],
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


aliased_tests = [
    pytest.param(
        "valid",
        None,
        ("valid", None, None, None),
        "valid",
        None,
        id="basic column test",
    ),
    pytest.param(
        "valid",
        "v",
        ("valid", None, None, "v"),
        "valid AS v",
        None,
        id="basic alias test",
    ),
    pytest.param(
        "va_lid.stuff",
        "v.s",
        ("va_lid.stuff", None, None, "v.s"),
        "va_lid.stuff AS v.s",
        None,
        id="dot column test",
    ),
    pytest.param(
        "a[b]",
        "a_b",
        ("a[b]", "a", "b", "a_b"),
        "a[b] AS a_b",
        None,
        id="single char subscriptable",
    ),
    pytest.param(
        "a1.2[a:bdsd]",
        "complex",
        ("a1.2[a:bdsd]", "a1.2", "a:bdsd", "complex"),
        "a1.2[a:bdsd] AS complex",
        None,
        id="colon subscriptable",
    ),
    pytest.param(
        "a1",
        1,
        None,
        None,
        InvalidColumn(
            "output_alias '1' of column a1 must be None or a non-empty string"
        ),
        id="alias must be string",
    ),
    pytest.param(
        "a1",
        "",
        None,
        None,
        InvalidColumn(
            "output_alias '' of column a1 must be None or a non-empty string"
        ),
        id="alias must be non-empty string",
    ),
    pytest.param(
        "a1",
        "___invalid**",
        None,
        None,
        InvalidColumn(
            "output_alias '___invalid**' of column a1 contains invalid characters"
        ),
        id="alias must be string",
    ),
]


TRANSLATOR = Translation(use_entity_aliases=True)


@pytest.mark.parametrize(
    "column_name, alias, valid, translated, exception", aliased_tests
)
def test_aliased_columns(
    column_name: str,
    alias: Optional[str],
    valid: Tuple[str, Optional[str], Optional[str], Optional[str]],
    translated: str,
    exception: Optional[Exception],
) -> None:
    if exception is not None:
        with pytest.raises(type(exception), match=re.escape(str(exception))):
            Column(column_name, output_alias=alias)
    else:
        exp = Column(column_name, output_alias=alias)
        assert exp.name == valid[0]
        assert exp.subscriptable == valid[1]
        assert exp.key == valid[2]
        assert exp.output_alias == valid[3]
        with output_aliases(TRANSLATOR):
            assert TRANSLATOR.visit(exp) == translated

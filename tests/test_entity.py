import pytest
import re
from typing import Any, Optional

from snuba_sdk.entity import Entity, InvalidEntity
from snuba_sdk.visitors import Translation


TRANSLATOR = Translation(use_entity_aliases=True)

tests = [
    pytest.param("sessions", None, None, "(sessions)", None),
    pytest.param("sessions", None, 0.1, "(sessions SAMPLE 0.100000)", None),
    pytest.param("sessions", None, 10.0, "(sessions SAMPLE 10.0)", None),
    pytest.param("sessions", "s", None, "(s: sessions)", None),
    pytest.param("sessions", "s", 10.0, "(s: sessions SAMPLE 10.0)", None),
    pytest.param("", "s", None, None, InvalidEntity("'' is not a valid entity name")),
    pytest.param(1, None, None, None, InvalidEntity("'1' is not a valid entity name")),
    pytest.param("sessions", "", None, None, InvalidEntity("'' is not a valid alias")),
    pytest.param("sessions", 1, None, None, InvalidEntity("'1' is not a valid alias")),
    pytest.param(
        "sessions",
        None,
        "0.1",
        None,
        InvalidEntity("sample must be a float"),
    ),
    pytest.param(
        "sessions",
        "s",
        -0.1,
        None,
        InvalidEntity("samples must be greater than 0.0"),
    ),
]


@pytest.mark.parametrize("name, alias, sample, formatted, exception", tests)
def test_entity(
    name: Any,
    alias: Any,
    sample: Any,
    formatted: Optional[str],
    exception: Optional[Exception],
) -> None:
    if exception is not None:
        with pytest.raises(type(exception), match=re.escape(str(exception))):
            Entity(name, alias, sample)
    else:
        entity = Entity(name, alias, sample)
        assert entity.name == name
        assert entity.sample == sample
        if formatted is not None:
            assert TRANSLATOR.visit(entity) == formatted

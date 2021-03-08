import pytest
import re
from typing import Any, Optional

from snuba_sdk.entity import Entity, InvalidEntity
from snuba_sdk.visitors import Translation


TRANSLATOR = Translation()

tests = [
    pytest.param("sessions", None, None, "(sessions)", None),
    pytest.param("sessions", None, 0.1, "(sessions SAMPLE 0.100000)", None),
    pytest.param("sessions", None, 10, "(sessions SAMPLE 10)", None),
    pytest.param("sessions", "s", None, "(s: sessions)", None),
    pytest.param("sessions", "s", 10, "(s: sessions SAMPLE 10)", None),
    pytest.param("", "s", None, None, InvalidEntity("'' is not a valid entity name")),
    pytest.param(1, None, None, None, InvalidEntity("'1' is not a valid entity name")),
    pytest.param("sessions", "", None, None, InvalidEntity("'' is not a valid alias")),
    pytest.param("sessions", 1, None, None, InvalidEntity("'1' is not a valid alias")),
    pytest.param(
        "sessions",
        None,
        "0.1",
        None,
        InvalidEntity(
            "sample must be a float between 0 and 1 or an integer greater than 1"
        ),
    ),
    pytest.param(
        "sessions",
        "s",
        -0.1,
        None,
        InvalidEntity("float samples must be between 0.0 and 1.0 (%age of rows)"),
    ),
    pytest.param(
        "sessions",
        None,
        1.1,
        None,
        InvalidEntity("float samples must be between 0.0 and 1.0 (%age of rows)"),
    ),
    pytest.param(
        "sessions",
        None,
        0,
        None,
        InvalidEntity("int samples must be at least 1 (# of rows)"),
    ),
    pytest.param(
        "sessions",
        "s",
        -1,
        None,
        InvalidEntity("int samples must be at least 1 (# of rows)"),
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

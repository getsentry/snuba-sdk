import re

import pytest

from snuba_sdk.request import Flags, InvalidFlagError

flag_tests = [
    pytest.param("totals", True, None),
    pytest.param("consistent", False, None),
    pytest.param("turbo", 1, InvalidFlagError("turbo must be a boolean")),
    pytest.param("dry_run", "string", InvalidFlagError("dry_run must be a boolean")),
]


@pytest.mark.parametrize("name, flag, exception", flag_tests)
def test_flags(name: str, flag: bool, exception: Exception) -> None:
    if exception is None:
        f = Flags(**{name: flag})
        assert getattr(f, name) == flag
        f.validate()
        assert f.to_dict() == {name: flag}
    else:
        with pytest.raises(type(exception), match=re.escape(str(exception))):
            f = Flags(**{name: flag})
            f.validate()


def test_flag_with_none() -> None:
    f = Flags(debug=None, totals=False, dry_run=True)
    f.validate()
    assert f.to_dict() == {"totals": False, "dry_run": True}

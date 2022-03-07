import re
from typing import Any

import pytest

from snuba_sdk.expressions import InvalidExpressionError
from snuba_sdk.flags import (
    Consistent,
    Debug,
    DryRun,
    Feature,
    Legacy,
    ParentAPI,
    Team,
    Totals,
    Turbo,
)

boolean_tests = [
    pytest.param("totals", Totals),
    pytest.param("consistent", Consistent),
    pytest.param("turbo", Turbo),
    pytest.param("debug", Debug),
    pytest.param("dry_run", DryRun),
    pytest.param("legacy", Legacy),
]


@pytest.mark.parametrize("name, flag", boolean_tests)
def test_boolean_flags(name: str, flag: Any) -> None:
    assert flag(True) is not None
    assert flag(False) is not None
    with pytest.raises(
        InvalidExpressionError, match=re.escape(f"{name} must be a boolean")
    ):
        flag(0)


string_tests = [
    pytest.param("parent_api", ParentAPI),
    pytest.param("team", Team),
    pytest.param("feature", Feature),
]


@pytest.mark.parametrize("name, flag", string_tests)
def test_string_flags(name: str, flag: Any) -> None:
    assert (
        flag("/api/0/issues_groups/_issue_id_/integrations/_integration_id_/")
        is not None
    )
    assert flag("sentry.tasks") is not None
    with pytest.raises(
        InvalidExpressionError, match=re.escape(f"{name} must be a non-empty string")
    ):
        flag(0)

    if flag != ParentAPI:
        with pytest.raises(
            InvalidExpressionError,
            match=re.escape(f"{name} contains invalid characters"),
        ):
            flag("`````")

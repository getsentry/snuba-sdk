import re
from typing import Any, Callable, Optional

import pytest

from snuba_sdk.expressions import InvalidExpressionError
from snuba_sdk.timeseries import Metric, Timeseries
from tests import timeseries

metric_tests = [
    pytest.param(
        "transaction.duration", "d:transactions/duration@millisecond", 123, None
    ),
    pytest.param(
        "transaction.duration", "d:transactions/duration@millisecond", None, None
    ),
    pytest.param("transaction.duration", None, 123, None),
    pytest.param(None, "d:transactions/duration@millisecond", 123, None),
    pytest.param("transaction.duration", None, None, None),
    pytest.param(None, "d:transactions/duration@millisecond", None, None),
    pytest.param(None, None, 123, None),
    pytest.param(
        None,
        None,
        None,
        InvalidExpressionError(
            "Metric must have at least one of public_name, mri or id"
        ),
    ),
    pytest.param(
        123,
        "d:transactions/duration@millisecond",
        123,
        InvalidExpressionError("public_name must be a string"),
    ),
    pytest.param(
        "transaction.duration",
        123,
        123,
        InvalidExpressionError("mri must be a string"),
    ),
    pytest.param(
        "transaction.duration",
        "d:transactions/duration@millisecond",
        "wrong",
        InvalidExpressionError("id must be an integer"),
    ),
]


@pytest.mark.parametrize("public_name, mri, mid, exception", metric_tests)
def test_metric(
    public_name: Any, mri: Any, mid: Any, exception: Optional[Exception]
) -> None:
    if exception is not None:
        with pytest.raises(type(exception), match=re.escape(str(exception))):
            Metric(public_name, mri, mid)
    else:
        assert Metric(public_name, mri, mid).id == mid


timeseries_tests = [
    pytest.param(
        timeseries(Metric(id=123), "count", None, None, None),
        Timeseries(Metric(id=123), "count", None, None, None),
        "",
        None,
        id="simple test",
    ),
    pytest.param(
        timeseries(Metric(id=123), 456, None, None, None),
        None,
        "",
        InvalidExpressionError("aggregate must be a string"),
        id="invalid aggregate",
    ),
]

# TODO: Add this back when we have a proper translator
# TRANSLATOR = Translation()


@pytest.mark.parametrize("func_wrapper, valid, translated, exception", timeseries_tests)
def test_timeseries(
    func_wrapper: Callable[[], Any],
    valid: Optional[Timeseries],
    translated: str,
    exception: Optional[Exception],
) -> None:
    def verify() -> None:
        exp = func_wrapper()
        assert exp == valid
        # assert TRANSLATOR.visit(exp) == translated # TODO: Translator doesn't exist yet

    if exception is not None:
        with pytest.raises(type(exception), match=re.escape(str(exception))):
            verify()
    else:
        verify()

import re
from typing import Any, Optional

import pytest

from snuba_sdk.storage import Storage, InvalidStorageError
from snuba_sdk.visitors import Translation

TRANSLATOR = Translation()

tests = [
    pytest.param("metric_summaries", None, "STORAGE(metric_summaries)", None),
    pytest.param(
        "metric_summaries", 0.1, "STORAGE(metric_summaries SAMPLE 0.100000)", None
    ),
    pytest.param(
        "metric_summaries", 10.0, "STORAGE(metric_summaries SAMPLE 10.0)", None
    ),
    pytest.param("", None, None, InvalidStorageError("'' is not a valid storage name")),
    pytest.param(1, None, None, InvalidStorageError("'1' is not a valid storage name")),
    pytest.param(
        "metric_summaries",
        "0.1",
        None,
        InvalidStorageError("sample must be a float"),
    ),
    pytest.param(
        "metric_summaries",
        -0.1,
        None,
        InvalidStorageError("samples must be greater than 0.0"),
    ),
]


@pytest.mark.parametrize("name, sample, formatted, exception", tests)
def test_storage(
    name: Any,
    sample: Any,
    formatted: Optional[str],
    exception: Optional[Exception],
) -> None:
    if exception is not None:
        with pytest.raises(type(exception), match=re.escape(str(exception))):
            Storage(name, sample)
    else:
        storage = Storage(name, sample)
        assert storage.name == name
        assert storage.sample == sample
        if formatted is not None:
            assert TRANSLATOR.visit(storage) == formatted

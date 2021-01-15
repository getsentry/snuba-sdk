import pytest
from typing import Any, List

from snuba_sdk.snuba import check_array_type

tests = [
    pytest.param([1, 2, 3], True),
    pytest.param([1, 2, None], True),
    pytest.param([None, 2, None], True),
    pytest.param([None, None, None], True),
    pytest.param([], True),
    pytest.param([1.0, 2, 3 / 4], True),
    pytest.param([(1.0, 2, 3 / 4)], True),
    pytest.param([(1.0, 2, 3 / 4), ("a",)], True),
    pytest.param([[[]]], True),
    pytest.param([[[None]]], True),
    pytest.param([[[None], [1]]], True),
    pytest.param([[None], [1]], True),
    pytest.param([[[1]]], True),
    pytest.param([[[1]], [[2.0]]], True),
    pytest.param([1, 2, "a"], False),
    pytest.param([1, "a", None], False),
    pytest.param([None, 2, "a"], False),
    pytest.param([(1.0, 2, 3 / 4), [1]], False),
    pytest.param([[1.0, 2, 3 / 4], ["a"]], False),
    pytest.param([[[1], ["a"]]], False),
    pytest.param([[[None], [1], ["a"]]], False),
    pytest.param([[[1]], [["a"]]], False),
    pytest.param([[[1]], [2.0]], False),
    pytest.param([[[None]], [2.0]], False),
]


@pytest.mark.parametrize("value, expected", tests)
def test_check_array_type(value: List[Any], expected: bool) -> None:
    assert check_array_type(value) == expected, value

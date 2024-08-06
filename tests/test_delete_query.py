import json

import pytest

from snuba_sdk.delete_query import DeleteQuery

queries = [
    pytest.param(
        DeleteQuery(
            storage_name="search_issues",
            column_conditions={"project_id": [1], "occurrence_id": ["1234"]},
        ),
        '{"columns":{"project_id": [1], "occurrence_id": ["1234"]}}',
    ),
    pytest.param(
        DeleteQuery(
            storage_name="non-real-storage",
            column_conditions={},
        ),
        '{"columns":{}}',
    ),
]


@pytest.mark.parametrize("query, expected", queries)
def test_serializes_properly(query: DeleteQuery, expected: str) -> None:
    # im doing the json.loads to ignore things like formatting
    actual = query.serialize()
    if not isinstance(actual, str):
        actual = json.dumps(actual)
    assert json.loads(actual) == json.loads(expected)

import json

import pytest

from snuba_sdk.delete_query import DeleteQuery, InvalidDeleteQueryError


def test_serialize() -> None:
    # im doing the json.loads to ignore things like formatting
    query = DeleteQuery(
        storage_name="non-real-storage",
        column_conditions={"project_id": [1], "occurrence_id": ["1234"]},
    )
    expected = '{"columns":{"project_id": [1], "occurrence_id": ["1234"]}}'
    serialize = query.serialize()
    assert isinstance(serialize, str)
    # json.loads is so whitespace and stuff is ignored
    assert json.loads(serialize) == json.loads(expected)


def test_empty_column_conditions() -> None:
    query = DeleteQuery(
        storage_name="search_issues",
        column_conditions={},
    )
    with pytest.raises(
        InvalidDeleteQueryError,
        match="column conditions cannot be empty",
    ):
        query.serialize()


def test_single_empty_column_condition() -> None:
    query = DeleteQuery(
        storage_name="search_issues",
        column_conditions={"project_id": []},
    )
    with pytest.raises(
        InvalidDeleteQueryError, match="column condition 'project_id' cannot be empty"
    ):
        query.serialize()

import json

import pytest

from snuba_sdk.delete_query import DeleteQuery, InvalidDeleteQueryError
from snuba_sdk.request import Request


def test_serialize() -> None:
    # im doing the json.loads to ignore things like formatting
    query = DeleteQuery(
        storage_name="non-real-storage",
        column_conditions={"project_id": [1], "occurrence_id": ["1234"]},
    )
    expected = {"columns": {"project_id": [1], "occurrence_id": ["1234"]}}
    serialize = query.serialize()
    assert serialize == expected


def test_serialize_request() -> None:
    req = Request(
        dataset="search_issues",
        app_id="myapp",
        query=DeleteQuery(
            storage_name="search_issues",
            column_conditions={"project_id": [1], "occurrence_id": ["1234"]},
        ),
    )
    expected = {
        "query": {"columns": {"project_id": [1], "occurrence_id": ["1234"]}},
        "dataset": "search_issues",
        "app_id": "myapp",
        "tenant_ids": {},
        "parent_api": "<unknown>",
    }
    assert json.loads(req.serialize()) == expected


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

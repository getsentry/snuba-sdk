import pytest
import re

from snuba_sdk.entity import Entity
from snuba_sdk.expressions import InvalidExpression
from snuba_sdk.query import Query, InvalidQuery


def test_invalid_query() -> None:
    with pytest.raises(
        InvalidQuery, match=re.escape("queries must have a valid dataset")
    ):
        Query(dataset=1, match=Entity("events"))  # type: ignore

    with pytest.raises(
        InvalidQuery, match=re.escape("queries must have a valid dataset")
    ):
        Query(dataset="", match=Entity("events"))

    with pytest.raises(
        InvalidQuery, match=re.escape("queries must have a valid Entity")
    ):
        Query(dataset="discover", match="events")  # type: ignore


def test_invalid_query_set() -> None:
    query = Query("discover", Entity("events"))

    tests = {
        "match": (0, "0 must be a valid Entity"),
        "select": (
            (0, [], [0]),
            "select clause must be a non-empty list of Column and/or Function",
        ),
        "groupby": (
            [0, [0]],
            "groupby clause must be a list of Column and/or Function",
        ),
        "where": ([0, [0]], "where clause must be a list of Condition"),
        "limit": (100000, "limit '100000' is capped at 10,000"),
        "offset": ("", "offset '' must be an integer"),
        "granularity": (-1, "granularity '-1' must be at least 1"),
    }

    match, err = tests["match"]
    with pytest.raises(InvalidQuery, match=re.escape(err)):
        query.set_match(match)  # type: ignore

    for val in tests["select"]:
        with pytest.raises(InvalidQuery, match=re.escape(tests["select"][1])):
            query.set_select(val)  # type: ignore

    for val in tests["groupby"]:
        with pytest.raises(InvalidQuery, match=re.escape(tests["groupby"][1])):
            query.set_groupby(val)  # type: ignore

    for val in tests["where"]:
        with pytest.raises(InvalidQuery, match=re.escape(tests["where"][1])):
            query.set_where(val)  # type: ignore

    with pytest.raises(InvalidExpression, match=re.escape(tests["limit"][1])):
        query.set_limit(tests["limit"][0])  # type: ignore

    with pytest.raises(InvalidExpression, match=re.escape(tests["offset"][1])):
        query.set_offset(tests["offset"][0])  # type: ignore

    with pytest.raises(InvalidExpression, match=re.escape(tests["granularity"][1])):
        query.set_granularity(tests["granularity"][0])  # type: ignore

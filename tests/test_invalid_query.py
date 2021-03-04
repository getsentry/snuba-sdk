import pytest
import re
from typing import Any, Mapping, Sequence

from snuba_sdk.entity import Entity
from snuba_sdk.expressions import Column, Function, InvalidExpression
from snuba_sdk.query import Query
from snuba_sdk.query_visitors import InvalidQuery


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

    tests: Mapping[str, Sequence[Any]] = {
        "match": (0, "0 must be a valid Entity"),
        "select": (
            (0, [], [0]),
            "select clause must be a non-empty list of Column and/or Function",
        ),
        "groupby": (
            [0, [0]],
            "groupby clause must be a list of Column and/or Function",
        ),
        "where": ([0, [0]], "where clause must be a list of conditions"),
        "having": ([0, [0]], "having clause must be a list of conditions"),
        "orderby": ([0, [0]], "orderby clause must be a list of OrderBy"),
        "limitby": ("a", "limitby clause must be a LimitBy"),
        "limit": (100000, "limit '100000' is capped at 10,000"),
        "offset": ("", "offset '' must be an integer"),
        "granularity": (-1, "granularity '-1' must be at least 1"),
    }

    match, err = tests["match"]
    with pytest.raises(InvalidQuery, match=re.escape(err)):
        query.set_match(match)

    for val in tests["select"][0]:
        with pytest.raises(InvalidQuery, match=re.escape(tests["select"][1])):
            query.set_select(val)

    for val in tests["groupby"][0]:
        with pytest.raises(InvalidQuery, match=re.escape(tests["groupby"][1])):
            query.set_groupby(val)

    for val in tests["where"][0]:
        with pytest.raises(InvalidQuery, match=re.escape(tests["where"][1])):
            query.set_where(val)

    for val in tests["having"][0]:
        with pytest.raises(InvalidQuery, match=re.escape(tests["having"][1])):
            query.set_having(val)

    for val in tests["orderby"][0]:
        with pytest.raises(InvalidQuery, match=re.escape(tests["orderby"][1])):
            query.set_orderby(val)

    with pytest.raises(InvalidQuery, match=re.escape(tests["limitby"][1])):
        query.set_limitby(tests["limitby"][0])

    with pytest.raises(InvalidExpression, match=re.escape(tests["limit"][1])):
        query.set_limit(tests["limit"][0])

    with pytest.raises(InvalidExpression, match=re.escape(tests["offset"][1])):
        query.set_offset(tests["offset"][0])

    with pytest.raises(InvalidExpression, match=re.escape(tests["granularity"][1])):
        query.set_granularity(tests["granularity"][0])


def test_invalid_subquery() -> None:
    with pytest.raises(
        InvalidQuery,
        match=re.escape(
            "inner query is invalid: query must have at least one column in select"
        ),
    ):
        Query("discover", Query(dataset="discover", match=Entity("events"))).set_select(
            [Column("event_id"), Column("title")]
        )

    with pytest.raises(
        InvalidQuery,
        match=re.escape(
            "inner query is invalid: query must have at least one column in select"
        ),
    ):
        Query(
            "discover",
            Query(
                dataset="discover",
                match=Entity("events"),
                select=[Column("title"), Column("timestamp")],
            ),
        ).set_match(Query(dataset="discover", match=Entity("events"))).set_select(
            [Function("uniq", [Column("new_event")], "uniq_event"), Column("title")]
        )

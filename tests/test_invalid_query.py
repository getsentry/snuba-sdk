import re
from typing import Any, Mapping, Sequence

import pytest

from snuba_sdk.aliased_expression import AliasedExpression
from snuba_sdk.column import Column
from snuba_sdk.conditions import Condition, InvalidConditionError, Op
from snuba_sdk.entity import Entity
from snuba_sdk.expressions import InvalidExpressionError
from snuba_sdk.function import Function
from snuba_sdk.query import Query
from snuba_sdk.query_visitors import InvalidQueryError


def test_invalid_query() -> None:
    with pytest.raises(
        InvalidQueryError, match=re.escape("queries must have a valid Entity")
    ):
        Query(match="events")  # type: ignore

    with pytest.raises(
        InvalidConditionError,
        match=re.escape(
            "invalid condition: LHS of a condition must be a Column, CurriedFunction or Function, not <class 'snuba_sdk.aliased_expression.AliasedExpression'>"
        ),
    ):
        (
            Query(Entity("events"))
            .set_select([AliasedExpression(Column("transaction"), "tn")])
            .set_where(
                [Condition(AliasedExpression(Column("project_id"), "pi"), Op.IN, (1,))]  # type: ignore
            )
        )


def test_invalid_query_set() -> None:
    query = Query(Entity("events"))

    tests: Mapping[str, Sequence[Any]] = {
        "match": (0, "0 must be a valid Entity"),
        "select": (
            (0, [], [0]),
            "select clause must be a non-empty list of SelectableExpression",
        ),
        "groupby": ([0, [0]], "groupby clause must be a list of SelectableExpression"),
        "where": ([0, [0]], "where clause must be a list of conditions"),
        "having": ([0, [0]], "having clause must be a list of conditions"),
        "orderby": ([0, [0]], "orderby clause must be a list of OrderBy"),
        "limitby": ("a", "limitby clause must be a LimitBy"),
        "limit": (100000, "limit '100000' is capped at 10,000"),
        "offset": ("", "offset '' must be an integer"),
        "granularity": (-1, "granularity '-1' must be at least 1"),
    }

    match, err = tests["match"]
    with pytest.raises(InvalidQueryError, match=re.escape(err)):
        query.set_match(match)

    for val in tests["select"][0]:
        with pytest.raises(InvalidQueryError, match=re.escape(tests["select"][1])):
            query.set_select(val)

    for val in tests["groupby"][0]:
        with pytest.raises(InvalidQueryError, match=re.escape(tests["groupby"][1])):
            query.set_groupby(val)

    for val in tests["where"][0]:
        with pytest.raises(InvalidQueryError, match=re.escape(tests["where"][1])):
            query.set_where(val)

    for val in tests["having"][0]:
        with pytest.raises(InvalidQueryError, match=re.escape(tests["having"][1])):
            query.set_having(val)

    for val in tests["orderby"][0]:
        with pytest.raises(InvalidQueryError, match=re.escape(tests["orderby"][1])):
            query.set_orderby(val)

    with pytest.raises(InvalidQueryError, match=re.escape(tests["limitby"][1])):
        query.set_limitby(tests["limitby"][0])

    with pytest.raises(InvalidExpressionError, match=re.escape(tests["limit"][1])):
        query.set_limit(tests["limit"][0])

    with pytest.raises(InvalidExpressionError, match=re.escape(tests["offset"][1])):
        query.set_offset(tests["offset"][0])

    with pytest.raises(
        InvalidExpressionError, match=re.escape(tests["granularity"][1])
    ):
        query.set_granularity(tests["granularity"][0])


def test_invalid_subquery() -> None:
    with pytest.raises(
        InvalidQueryError,
        match=re.escape(
            "inner query is invalid: query must have at least one expression in select"
        ),
    ):
        Query(Query(match=Entity("events"))).set_select(
            [Column("event_id"), Column("title")]
        )

    with pytest.raises(
        InvalidQueryError,
        match=re.escape(
            "inner query is invalid: query must have at least one expression in select"
        ),
    ):
        Query(
            Query(
                match=Entity("events"),
                select=[Column("title"), Column("timestamp")],
            ),
        ).set_match(Query(match=Entity("events"))).set_select(
            [Function("uniq", [Column("new_event")], "uniq_event"), Column("title")]
        )

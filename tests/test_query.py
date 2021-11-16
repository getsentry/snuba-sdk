import re
from datetime import datetime, timezone

import pytest

from snuba_sdk import (
    BooleanCondition,
    BooleanOp,
    Column,
    Condition,
    CurriedFunction,
    Debug,
    Direction,
    Entity,
    Function,
    Granularity,
    Limit,
    LimitBy,
    Offset,
    Op,
    OrderBy,
    Query,
)
from snuba_sdk.query_validation import InvalidMatchError
from snuba_sdk.query_visitors import InvalidQueryError

NOW = datetime(2021, 1, 2, 3, 4, 5, 6, timezone.utc)
tests = [
    pytest.param(
        Query(
            dataset="discover",
            match=Entity("events"),
            select=[Column("event_id")],
            groupby=None,
            where=[Condition(Column("timestamp"), Op.GT, NOW)],
            limit=Limit(10),
            offset=Offset(1),
            granularity=Granularity(3600),
        ),
        id="basic query",
    ),
    pytest.param(
        Query(
            dataset="discover",
            match=Entity("events", "ev", 0.2),
            select=[
                Column("title"),
                Column("tags[release:1]"),
                Function("uniq", [Column("event_id")], "uniq_events"),
            ],
            groupby=[Column("title"), Column("tags[release:1]")],
            where=[
                Condition(Column("timestamp"), Op.GT, NOW),
                Condition(Function("toHour", [Column("timestamp")]), Op.LTE, NOW),
                Condition(Column("project_id"), Op.IN, Function("tuple", [1, 2, 3])),
            ],
            having=[Condition(Function("uniq", [Column("event_id")]), Op.GT, 1)],
            orderby=[OrderBy(Column("title"), Direction.ASC)],
            limitby=LimitBy(Column("title"), 5),
            limit=Limit(10),
            offset=Offset(1),
            granularity=Granularity(3600),
            debug=Debug(True),
        ),
        id="complex query",
    ),
    pytest.param(
        Query("discover", Entity("events", None, 0.2))
        .set_select([Column("event_id")])
        .set_where([Condition(Column("timestamp"), Op.GT, NOW)])
        .set_limit(10)
        .set_offset(1)
        .set_granularity(3600),
        id="basic query with replace",
    ),
    pytest.param(
        Query("discover", Entity("events"))
        .set_select(
            [
                Column("title"),
                Function("uniq", [Column("event_id")], "uniq_events"),
                CurriedFunction("quantile", [0.5], [Column("duration")], "p50"),
            ]
        )
        .set_groupby([Column("title")])
        .set_where(
            [
                Condition(Column("timestamp"), Op.GT, NOW),
                Condition(Function("toHour", [Column("timestamp")]), Op.LTE, NOW),
                Condition(Column("project_id"), Op.IN, Function("tuple", [1, 2, 3])),
                BooleanCondition(
                    BooleanOp.OR,
                    [
                        Condition(Column("event_id"), Op.EQ, "abc"),
                        Condition(Column("duration"), Op.GT, 10),
                    ],
                ),
            ],
        )
        .set_having(
            [
                Condition(Function("uniq", [Column("event_id")]), Op.GT, 1),
                BooleanCondition(
                    BooleanOp.OR,
                    [
                        Condition(Function("uniq", [Column("event_id")]), Op.GTE, 10),
                        Condition(
                            CurriedFunction("quantile", [0.5], [Column("duration")]),
                            Op.GTE,
                            99,
                        ),
                    ],
                ),
            ],
        )
        .set_orderby([OrderBy(Column("title"), Direction.ASC)])
        .set_limitby(LimitBy(Column("title"), 5))
        .set_limit(10)
        .set_offset(1)
        .set_granularity(3600)
        .set_debug(True),
        id="complex query with replace",
    ),
    pytest.param(
        Query("discover", Entity("events"))
        .set_select([Column("event_id")])
        .set_where(
            [
                Condition(Column("project_id"), Op.IN, [1, 2, 3]),
                Condition(Column("group_id"), Op.NOT_IN, (1, "2", 3)),
            ]
        )
        .set_limit(10)
        .set_offset(1)
        .set_granularity(3600),
        id="lists and tuples are allowed",
    ),
    pytest.param(
        Query("discover", Entity("events"))
        .set_select([Column("event_id"), Column("title")])
        .set_where([Condition(Column("timestamp"), Op.GT, NOW)])
        .set_orderby(
            [
                OrderBy(Column("event_id"), Direction.ASC),
                OrderBy(Column("title"), Direction.DESC),
            ]
        )
        .set_limit(10)
        .set_offset(1)
        .set_granularity(3600),
        id="multiple ORDER BY",
    ),
    pytest.param(
        Query("discover", Entity("events"))
        .set_select([Column("event_id"), Column("title")])
        .set_where([Condition(Column("timestamp"), Op.IS_NOT_NULL)])
        .set_limit(10)
        .set_offset(1)
        .set_granularity(3600),
        id="unary condition",
    ),
    pytest.param(
        Query("discover", Entity("events"))
        .set_select([Column("event_id"), Column("title")])
        .set_array_join(Column("exception_stacks"))
        .set_where([Condition(Column("timestamp"), Op.IS_NOT_NULL)])
        .set_limit(10)
        .set_offset(1)
        .set_granularity(3600),
        id="array join",
    ),
    pytest.param(
        Query(
            "discover",
            Query(
                dataset="discover",
                match=Entity("events"),
                select=[Column("event_id"), Column("title"), Column("timestamp")],
            ),
        )
        .set_select([Column("event_id"), Column("title")])
        .set_where([Condition(Column("timestamp"), Op.IS_NOT_NULL)])
        .set_limit(10)
        .set_offset(1)
        .set_granularity(3600),
        id="simple subquery",
    ),
    pytest.param(
        Query(
            "discover",
            Query(
                dataset="discover",
                match=Entity("events"),
                select=[
                    Function("toString", [Column("event_id")], "new_event"),
                    Column("title"),
                    Column("timestamp"),
                ],
            ),
        )
        .set_select(
            [Function("uniq", [Column("new_event")], "uniq_event"), Column("title")]
        )
        .set_groupby([Column("title")])
        .set_where([Condition(Column("timestamp"), Op.IS_NOT_NULL)])
        .set_limit(10)
        .set_offset(1)
        .set_granularity(3600),
        id="subquery with functions",
    ),
    pytest.param(
        Query(
            "discover",
            Query(
                dataset="discover",
                match=Query(
                    dataset="discover",
                    match=Entity("events"),
                    select=[Column("event_id"), Column("title"), Column("timestamp")],
                ),
                select=[
                    Function("toString", [Column("event_id")], "uniq_event"),
                    Column("timestamp"),
                ],
            ),
        )
        .set_select([Function("avg", [Column("uniq_event")], "avg_event")])
        .set_where([Condition(Column("timestamp"), Op.IS_NOT_NULL)])
        .set_limit(10)
        .set_offset(1)
        .set_granularity(3600),
        id="multiple nested",
    ),
    pytest.param(
        Query("discover", Entity("discover"))
        .set_select(
            [
                Function(
                    "arrayMax",
                    [[1, Function("indexOf", ["a", Column("hierarchical_hashes")])]],
                )
            ]
        )
        .set_where(
            [
                Condition(
                    Column("event_id"),
                    Op.IN,
                    (Column("group_id"), Column("primary_hash")),
                )
            ]
        )
        .set_limit(10)
        .set_offset(1)
        .set_granularity(3600),
        id="sequences can mix expressions with literals",
    ),
]


@pytest.mark.parametrize("query", tests)
def test_query(query: Query) -> None:
    query.validate()


invalid_tests = [
    pytest.param(
        Query(
            dataset="discover",
            match=Entity("events"),
            select=None,
            groupby=None,
            where=[Condition(Column("timestamp"), Op.GT, NOW)],
            limit=Limit(10),
            offset=Offset(1),
            granularity=Granularity(3600),
        ),
        InvalidQueryError("query must have at least one expression in select"),
        id="missing select",
    ),
    pytest.param(
        Query(
            dataset="discover",
            match=Entity("events"),
            select=[Column("title")],
            where=[Condition(Column("timestamp"), Op.GT, NOW)],
            limit=Limit(10),
            offset=Offset(1),
            granularity=Granularity(3600),
        ).set_totals(True),
        InvalidQueryError("totals is only valid with a groupby"),
        id="Totals must have a groupby",
    ),
    pytest.param(
        Query(
            "discover",
            Query(
                dataset="discover",
                match=Entity("events"),
                select=[Column("event_id"), Column("title"), Column("timestamp")],
            ),
        )
        .set_select([Column("event_id"), Column("group_id")])
        .set_where([Condition(Column("timestamp"), Op.IS_NOT_NULL)])
        .set_limit(10)
        .set_offset(1)
        .set_granularity(3600),
        InvalidMatchError(
            "outer query is referencing column 'group_id' that does not exist in subquery"
        ),
        id="invalid column reference in outer query",
    ),
    pytest.param(
        Query(
            "discover",
            Query(
                dataset="discover",
                match=Entity("events"),
                select=[
                    Function("toString", [Column("event_id")], "new_event"),
                    Column("title"),
                    Column("timestamp"),
                ],
            ),
        )
        .set_select(
            [Function("uniq", [Column("event_id")], "uniq_event"), Column("title")]
        )
        .set_groupby([Column("title")])
        .set_where([Condition(Column("timestamp"), Op.IS_NOT_NULL)])
        .set_limit(10)
        .set_offset(1)
        .set_granularity(3600),
        InvalidMatchError(
            "outer query is referencing column 'event_id' that does not exist in subquery"
        ),
        id="outer query is referencing column not alias",
    ),
]


@pytest.mark.parametrize("query, exception", invalid_tests)
def test_invalid_query(query: Query, exception: Exception) -> None:
    with pytest.raises(type(exception), match=re.escape(str(exception))):
        query.validate()

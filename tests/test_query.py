import pytest
import re
from datetime import datetime, timezone
from typing import Optional

from snuba_sdk.conditions import BooleanCondition, BooleanOp, Condition, Op
from snuba_sdk.entity import Entity
from snuba_sdk.expressions import (
    Column,
    CurriedFunction,
    Debug,
    Direction,
    Function,
    Granularity,
    Limit,
    LimitBy,
    Offset,
    OrderBy,
)
from snuba_sdk.query import Query
from snuba_sdk.query_visitors import InvalidQuery


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
        None,
        id="basic query",
    ),
    pytest.param(
        Query(
            dataset="discover",
            match=Entity("events", 0.2),
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
        None,
        id="complex query",
    ),
    pytest.param(
        Query("discover", Entity("events", 0.2))
        .set_select([Column("event_id")])
        .set_where([Condition(Column("timestamp"), Op.GT, NOW)])
        .set_limit(10)
        .set_offset(1)
        .set_granularity(3600),
        None,
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
                        Condition(
                            Function("uniq", [Column("event_id")]),
                            Op.GTE,
                            10,
                        ),
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
        None,
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
        None,
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
        None,
        id="multiple ORDER BY",
    ),
    pytest.param(
        Query("discover", Entity("events"))
        .set_select([Column("event_id"), Column("title")])
        .set_where([Condition(Column("timestamp"), Op.IS_NOT_NULL)])
        .set_limit(10)
        .set_offset(1)
        .set_granularity(3600),
        None,
        id="unary condition",
    ),
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
        InvalidQuery("query must have at least one column in select"),
        id="missing select",
    ),
    pytest.param(
        Query(
            dataset="discover",
            match=Entity("events"),
            select=[Function("count", [])],
            groupby=[Column("title")],
            where=[Condition(Column("timestamp"), Op.GT, NOW)],
            limit=Limit(10),
            offset=Offset(1),
            granularity=Granularity(3600),
        ),
        InvalidQuery(
            "Function(function='count', initializers=None, parameters=[], alias=None) must have an alias in the select"
        ),
        id="functions in the select must have an alias",
    ),
    pytest.param(
        Query(
            dataset="discover",
            match=Entity("events"),
            select=[Column("title"), Function("count", [], "count")],
            groupby=None,
            where=[Condition(Column("timestamp"), Op.GT, NOW)],
            limit=Limit(10),
            offset=Offset(1),
            granularity=Granularity(3600),
        ),
        InvalidQuery(
            "groupby must be included if there are aggregations in the select"
        ),
        id="groupby can't be None with aggregate",
    ),
    pytest.param(
        Query(
            dataset="discover",
            match=Entity("events"),
            select=[
                Column("title"),
                Function(
                    "plus", [Function("count", []), Function("count", [])], "added"
                ),
            ],
            groupby=None,
            where=[Condition(Column("timestamp"), Op.GT, NOW)],
            limit=Limit(10),
            offset=Offset(1),
            granularity=Granularity(3600),
        ),
        InvalidQuery(
            "groupby must be included if there are aggregations in the select"
        ),
        id="groupby can't be None with nested aggregate",
    ),
    pytest.param(
        Query(
            dataset="discover",
            match=Entity("events"),
            select=[Column("title"), Function("count", [], "count")],
            groupby=[Column("day")],
            where=[Condition(Column("timestamp"), Op.GT, NOW)],
            limit=Limit(10),
            offset=Offset(1),
            granularity=Granularity(3600),
        ),
        InvalidQuery(
            "Column(name='title', subscriptable=None, key=None) missing from the groupby"
        ),
        id="groupby must include all non aggregates",
    ),
    pytest.param(
        Query(
            dataset="discover",
            match=Entity("events"),
            select=[Column("title"), Function("count", [], "count")],
            groupby=[Column("day")],
            where=[Condition(Column("timestamp"), Op.GT, NOW)],
            limitby=LimitBy(Column("event_id"), 5),
            limit=Limit(10),
            offset=Offset(1),
            granularity=Granularity(3600),
        ),
        InvalidQuery(
            "Column(name='event_id', subscriptable=None, key=None) in limitby clause is missing from select clause"
        ),
        id="LimitBy must be in the select",
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
        InvalidQuery("totals is only valid with a groupby"),
        id="Totals must have a groupby",
    ),
]


@pytest.mark.parametrize("query, exception", tests)
def test_query(query: Query, exception: Optional[Exception]) -> None:
    if exception is not None:
        with pytest.raises(type(exception), match=re.escape(str(exception))):
            query.validate()
        return

    query.validate()

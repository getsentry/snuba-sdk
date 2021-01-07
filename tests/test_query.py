import pytest
import re
from datetime import datetime, timezone
from typing import Optional

from snuba_sdk.expressions import (
    Column,
    Condition,
    Entity,
    Function,
    Granularity,
    Limit,
    Offset,
    Op,
)
from snuba_sdk.query import Query, InvalidQuery


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
        (
            "MATCH (events) "
            "SELECT event_id "
            "WHERE timestamp > toDateTime('2021-01-02T03:04:05.000006') "
            "LIMIT 10 "
            "OFFSET 1 "
            "GRANULARITY 3600"
        ),
        id="basic query",
    ),
    pytest.param(
        Query(
            dataset="discover",
            match=Entity("events"),
            select=[
                Column("title"),
                Function("uniq", [Column("event_id")], "uniq_events"),
            ],
            groupby=[Column("title")],
            where=[
                Condition(Column("timestamp"), Op.GT, NOW),
                Condition(Function("toHour", [Column("timestamp")]), Op.LTE, NOW),
                Condition(Column("project_id"), Op.IN, Function("tuple", [1, 2, 3])),
            ],
            limit=Limit(10),
            offset=Offset(1),
            granularity=Granularity(3600),
        ),
        None,
        (
            "MATCH (events) "
            "SELECT title, uniq(event_id) AS uniq_events "
            "BY title "
            "WHERE timestamp > toDateTime('2021-01-02T03:04:05.000006') "
            "AND toHour(timestamp) <= toDateTime('2021-01-02T03:04:05.000006') "
            "AND project_id IN tuple(1, 2, 3) "
            "LIMIT 10 "
            "OFFSET 1 "
            "GRANULARITY 3600"
        ),
        id="complex query",
    ),
    pytest.param(
        Query("discover", Entity("events"))
        .set_select([Column("event_id")])
        .set_where([Condition(Column("timestamp"), Op.GT, NOW)])
        .set_limit(10)
        .set_offset(1)
        .set_granularity(3600),
        None,
        (
            "MATCH (events) "
            "SELECT event_id "
            "WHERE timestamp > toDateTime('2021-01-02T03:04:05.000006') "
            "LIMIT 10 "
            "OFFSET 1 "
            "GRANULARITY 3600"
        ),
        id="basic query with replace",
    ),
    pytest.param(
        Query("discover", Entity("events"))
        .set_select(
            [Column("title"), Function("uniq", [Column("event_id")], "uniq_events")]
        )
        .set_groupby([Column("title")])
        .set_where(
            [
                Condition(Column("timestamp"), Op.GT, NOW),
                Condition(Function("toHour", [Column("timestamp")]), Op.LTE, NOW),
                Condition(Column("project_id"), Op.IN, Function("tuple", [1, 2, 3])),
            ]
        )
        .set_limit(10)
        .set_offset(1)
        .set_granularity(3600),
        None,
        (
            "MATCH (events) "
            "SELECT title, uniq(event_id) AS uniq_events "
            "BY title "
            "WHERE timestamp > toDateTime('2021-01-02T03:04:05.000006') "
            "AND toHour(timestamp) <= toDateTime('2021-01-02T03:04:05.000006') "
            "AND project_id IN tuple(1, 2, 3) "
            "LIMIT 10 "
            "OFFSET 1 "
            "GRANULARITY 3600"
        ),
        id="complex query with replace",
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
        None,
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
        InvalidQuery("count() must have an alias in the select"),
        None,
        id="functions must have alias",
    ),
    pytest.param(
        Query(
            dataset="discover",
            match=Entity("events"),
            select=[Column("title"), Function("count", [])],
            groupby=None,
            where=[Condition(Column("timestamp"), Op.GT, NOW)],
            limit=Limit(10),
            offset=Offset(1),
            granularity=Granularity(3600),
        ),
        InvalidQuery("count() must have an alias in the select"),
        None,
        id="groupby can't be None with aggregate",
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
        InvalidQuery("title missing from the groupby"),
        None,
        id="groupby must include all non aggregates",
    ),
]


@pytest.mark.parametrize("query, exception, expected", tests)
def test_query(
    query: Query, exception: Optional[Exception], expected: Optional[str]
) -> None:
    if exception is not None:
        with pytest.raises(type(exception), match=re.escape(str(exception))):
            query.translate()
        return

    assert query.translate() == expected

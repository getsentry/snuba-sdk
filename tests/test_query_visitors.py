import json
import pytest
from datetime import datetime, timezone
from typing import Any, MutableMapping, Optional, Sequence, Tuple

from snuba_sdk.conditions import Condition, Op
from snuba_sdk.entity import Entity
from snuba_sdk.expressions import (
    Column,
    CurriedFunction,
    Direction,
    Function,
    Granularity,
    Limit,
    LimitBy,
    Offset,
    OrderBy,
    Totals,
)
from snuba_sdk.query import Query


NOW = datetime(2021, 1, 2, 3, 4, 5, 6, timezone.utc)
tests = [
    pytest.param(
        Query("discover", Entity("events"))
        .set_select([Column("event_id")])
        .set_where([Condition(Column("timestamp"), Op.GT, NOW)])
        .set_limit(10)
        .set_offset(1)
        .set_granularity(3600),
        (
            "MATCH (events)",
            "SELECT event_id",
            "WHERE timestamp > toDateTime('2021-01-02T03:04:05.000006')",
            "LIMIT 10",
            "OFFSET 1",
            "GRANULARITY 3600",
        ),
        None,
        id="basic query",
    ),
    pytest.param(
        Query(
            dataset="discover",
            match=Entity("events", 1000),
            select=[
                Column("title"),
                Function("uniq", [Column("event_id")], "uniq_events"),
                CurriedFunction("quantile", [0.5], [Column("duration")], "p50"),
            ],
            groupby=[Column("title")],
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
            totals=Totals(True),
        ),
        (
            "MATCH (events SAMPLE 1000)",
            "SELECT title, uniq(event_id) AS uniq_events, quantile(0.5)(duration) AS p50",
            "BY title",
            (
                "WHERE timestamp > toDateTime('2021-01-02T03:04:05.000006') "
                "AND toHour(timestamp) <= toDateTime('2021-01-02T03:04:05.000006') "
                "AND project_id IN tuple(1, 2, 3)"
            ),
            "HAVING uniq(event_id) > 1",
            "ORDER BY title ASC",
            "LIMIT 5 BY title",
            "LIMIT 10",
            "OFFSET 1",
            "GRANULARITY 3600",
            "TOTALS True",
        ),
        None,
        id="query with all clauses",
    ),
    pytest.param(
        Query("discover", Entity("events", 0.2))
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
        .set_granularity(3600)
        .set_totals(False)
        .set_consistent(True)
        .set_turbo(True)
        .set_debug(True),
        (
            "MATCH (events SAMPLE 0.200000)",
            "SELECT event_id, title",
            "WHERE timestamp > toDateTime('2021-01-02T03:04:05.000006')",
            "ORDER BY event_id ASC, title DESC",
            "LIMIT 10",
            "OFFSET 1",
            "GRANULARITY 3600",
        ),
        [("consistent", True), ("turbo", True), ("debug", True)],
        id="multiple ORDER BY",
    ),
    pytest.param(
        Query("discover", Entity("events"))
        .set_select([Column("event_id"), Column("title")])
        .set_where([Condition(Column("timestamp"), Op.GT, NOW)])
        .set_having(
            [
                Condition(Function("uniq", [Column("users")]), Op.GT, 1),
                Condition(Function("count", []), Op.LTE, 1000),
            ]
        )
        .set_orderby(
            [
                OrderBy(Column("event_id"), Direction.ASC),
                OrderBy(Column("title"), Direction.DESC),
            ]
        )
        .set_limit(10)
        .set_offset(1)
        .set_granularity(3600),
        (
            "MATCH (events)",
            "SELECT event_id, title",
            "WHERE timestamp > toDateTime('2021-01-02T03:04:05.000006')",
            "HAVING uniq(users) > 1 AND count() <= 1000",
            "ORDER BY event_id ASC, title DESC",
            "LIMIT 10",
            "OFFSET 1",
            "GRANULARITY 3600",
        ),
        None,
        id="multiple HAVING",
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
        (
            "MATCH (events)",
            "SELECT event_id",
            "WHERE project_id IN array(1, 2, 3) AND group_id NOT IN tuple(1, '2', 3)",
            "LIMIT 10",
            "OFFSET 1",
            "GRANULARITY 3600",
        ),
        None,
        id="lists and tuples are allowed",
    ),
]


@pytest.mark.parametrize("query, clauses, extras", tests)
def test_print_query(
    query: Query, clauses: Sequence[str], extras: Optional[Sequence[Tuple[str, bool]]]
) -> None:
    expected = " ".join(clauses)
    assert str(query) == expected


@pytest.mark.parametrize("query, clauses, extras", tests)
def test_pretty_print_query(
    query: Query, clauses: Sequence[str], extras: Optional[Sequence[Tuple[str, bool]]]
) -> None:
    joined = "\n".join(clauses)
    prefix = "-- DATASET: discover\n"
    if extras:
        for key, value in extras:
            prefix += f"-- {key.upper()}: {value}\n"

    expected = f"{prefix}{joined}"
    assert query.print() == expected


@pytest.mark.parametrize("query, clauses, extras", tests)
def test_translate_query(
    query: Query, clauses: Sequence[str], extras: Optional[Sequence[Tuple[str, bool]]]
) -> None:
    joined = " ".join(clauses)
    body: MutableMapping[str, Any] = {"dataset": "discover", "query": joined}
    if extras:
        body.update({k: v for k, v in extras})
    assert query.snuba() == json.dumps(body)

from __future__ import annotations

from datetime import datetime, timezone
from typing import Sequence

import pytest

from snuba_sdk.aliased_expression import AliasedExpression
from snuba_sdk.column import Column
from snuba_sdk.conditions import BooleanCondition, BooleanOp, Condition, Op
from snuba_sdk.entity import Entity
from snuba_sdk.expressions import Granularity, Limit, Offset, Totals
from snuba_sdk.function import CurriedFunction, Function
from snuba_sdk.orderby import Direction, LimitBy, OrderBy
from snuba_sdk.query import Query
from snuba_sdk.storage import Storage

NOW = datetime(2021, 1, 2, 3, 4, 5, 6, timezone.utc)
tests = [
    pytest.param(
        Query(Entity("events"))
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
        id="basic query",
    ),
    pytest.param(
        Query(Storage("events"))
        .set_select([Column("event_id")])
        .set_where([Condition(Column("timestamp"), Op.GT, NOW)])
        .set_limit(10)
        .set_offset(1)
        .set_granularity(3600),
        (
            "MATCH STORAGE(events)",
            "SELECT event_id",
            "WHERE timestamp > toDateTime('2021-01-02T03:04:05.000006')",
            "LIMIT 10",
            "OFFSET 1",
            "GRANULARITY 3600",
        ),
        id="basic storage query",
    ),
    pytest.param(
        Query(
            match=Entity("events", "e", 1000.0),
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
                BooleanCondition(
                    BooleanOp.OR,
                    [
                        Condition(Column("event_id"), Op.EQ, "abc"),
                        Condition(Column("duration"), Op.GT, 10),
                    ],
                ),
            ],
            having=[
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
            orderby=[OrderBy(Column("title"), Direction.ASC)],
            limitby=LimitBy([Column("title")], 5),
            limit=Limit(10),
            offset=Offset(1),
            granularity=Granularity(3600),
            totals=Totals(True),
        ),
        (
            "MATCH (events SAMPLE 1000.0)",
            "SELECT title, uniq(event_id) AS `uniq_events`, quantile(0.5)(duration) AS `p50`",
            "BY title",
            (
                "WHERE timestamp > toDateTime('2021-01-02T03:04:05.000006') "
                "AND toHour(timestamp) <= toDateTime('2021-01-02T03:04:05.000006') "
                "AND project_id IN tuple(1, 2, 3) "
                "AND (event_id = 'abc' OR duration > 10)"
            ),
            "HAVING uniq(event_id) > 1 AND (uniq(event_id) >= 10 OR quantile(0.5)(duration) >= 99)",
            "ORDER BY title ASC",
            "LIMIT 5 BY title",
            "LIMIT 10",
            "OFFSET 1",
            "GRANULARITY 3600",
            "TOTALS True",
        ),
        id="query with all clauses",
    ),
    pytest.param(
        Query(
            match=Storage("events", 1000.0),
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
                BooleanCondition(
                    BooleanOp.OR,
                    [
                        Condition(Column("event_id"), Op.EQ, "abc"),
                        Condition(Column("duration"), Op.GT, 10),
                    ],
                ),
            ],
            having=[
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
            orderby=[OrderBy(Column("title"), Direction.ASC)],
            limitby=LimitBy([Column("title")], 5),
            limit=Limit(10),
            offset=Offset(1),
            granularity=Granularity(3600),
            totals=Totals(True),
        ),
        (
            "MATCH STORAGE(events SAMPLE 1000.0)",
            "SELECT title, uniq(event_id) AS `uniq_events`, quantile(0.5)(duration) AS `p50`",
            "BY title",
            (
                "WHERE timestamp > toDateTime('2021-01-02T03:04:05.000006') "
                "AND toHour(timestamp) <= toDateTime('2021-01-02T03:04:05.000006') "
                "AND project_id IN tuple(1, 2, 3) "
                "AND (event_id = 'abc' OR duration > 10)"
            ),
            "HAVING uniq(event_id) > 1 AND (uniq(event_id) >= 10 OR quantile(0.5)(duration) >= 99)",
            "ORDER BY title ASC",
            "LIMIT 5 BY title",
            "LIMIT 10",
            "OFFSET 1",
            "GRANULARITY 3600",
            "TOTALS True",
        ),
        id="storage query with all clauses",
    ),
    pytest.param(
        Query(Entity("events", None, 0.2))
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
        (
            "MATCH (events SAMPLE 0.200000)",
            "SELECT event_id, title",
            "WHERE timestamp > toDateTime('2021-01-02T03:04:05.000006')",
            "ORDER BY event_id ASC, title DESC",
            "LIMIT 10",
            "OFFSET 1",
            "GRANULARITY 3600",
        ),
        id="multiple ORDER BY",
    ),
    pytest.param(
        Query(Entity("events"))
        .set_select([Column("event_id"), Column("title")])
        .set_where([Condition(Column("timestamp"), Op.IS_NOT_NULL)])
        .set_limit(10)
        .set_offset(1)
        .set_granularity(3600),
        (
            "MATCH (events)",
            "SELECT event_id, title",
            "WHERE timestamp IS NOT NULL",
            "LIMIT 10",
            "OFFSET 1",
            "GRANULARITY 3600",
        ),
        id="unary condition",
    ),
    pytest.param(
        Query(Entity("events"))
        .set_select([Column("event_id"), Column("title")])
        .set_array_join([Column("exception_stacks[stuff]")])
        .set_where([Condition(Column("timestamp"), Op.IS_NOT_NULL)])
        .set_limit(10)
        .set_offset(1)
        .set_granularity(3600),
        (
            "MATCH (events)",
            "SELECT event_id, title",
            "ARRAY JOIN exception_stacks[stuff]",
            "WHERE timestamp IS NOT NULL",
            "LIMIT 10",
            "OFFSET 1",
            "GRANULARITY 3600",
        ),
        id="array join",
    ),
    pytest.param(
        Query(Entity("events"))
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
        id="multiple HAVING",
    ),
    pytest.param(
        Query(Entity("events"))
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
        id="lists and tuples are allowed",
    ),
    pytest.param(
        Query(
            Query(
                match=Entity("events"),
                select=[Column("event_id"), Column("title"), Column("timestamp")],
            ),
        )
        .set_select([Column("event_id"), Column("title")])
        .set_where([Condition(Column("timestamp"), Op.IS_NOT_NULL)])
        .set_limit(10)
        .set_offset(1)
        .set_granularity(3600),
        (
            "MATCH { MATCH (events) SELECT event_id, title, timestamp }",
            "SELECT event_id, title",
            "WHERE timestamp IS NOT NULL",
            "LIMIT 10",
            "OFFSET 1",
            "GRANULARITY 3600",
        ),
        id="simple subquery",
    ),
    pytest.param(
        Query(
            Query(
                match=Entity("events", "ev"),
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
        (
            "MATCH { MATCH (events) SELECT toString(event_id) AS `new_event`, title, timestamp }",
            "SELECT uniq(new_event) AS `uniq_event`, title",
            "BY title",
            "WHERE timestamp IS NOT NULL",
            "LIMIT 10",
            "OFFSET 1",
            "GRANULARITY 3600",
        ),
        id="subquery with functions",
    ),
    pytest.param(
        Query(
            Query(
                match=Query(
                    match=Entity("events"),
                    select=[Column("event_id"), Column("title"), Column("timestamp")],
                ),
                select=[
                    Function("toString", [Column("event_id")], "new_event"),
                    Column("timestamp"),
                ],
            ),
        )
        .set_select([Function("avg", [Column("new_event")], "avg_event")])
        .set_where([Condition(Column("timestamp"), Op.IS_NOT_NULL)])
        .set_limit(10)
        .set_offset(1)
        .set_granularity(3600),
        (
            "MATCH { MATCH { MATCH (events) SELECT event_id, title, timestamp } SELECT toString(event_id) AS `new_event`, timestamp }",
            "SELECT avg(new_event) AS `avg_event`",
            "WHERE timestamp IS NOT NULL",
            "LIMIT 10",
            "OFFSET 1",
            "GRANULARITY 3600",
        ),
        id="multiple nested",
    ),
    pytest.param(
        Query(Entity("discover"))
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
        (
            "MATCH (discover)",
            "SELECT arrayMax(array(1, indexOf('a', hierarchical_hashes)))",
            "WHERE event_id IN tuple(group_id, primary_hash)",
            "LIMIT 10",
            "OFFSET 1",
            "GRANULARITY 3600",
        ),
        id="sequences can mix expressions with literals",
    ),
    pytest.param(
        Query(Entity("events"))
        .set_select(
            [
                AliasedExpression(Column("transaction"), "tn"),
                Function("count", [], "equation[0]"),
            ]
        )
        .set_groupby(
            [
                AliasedExpression(Column("project_id"), "pi"),
                AliasedExpression(Column("transaction"), "tn"),
            ]
        )
        .set_where([Condition(Column("project_id"), Op.IN, (1,))]),
        (
            "MATCH (events)",
            "SELECT transaction AS `tn`, count() AS `equation[0]`",
            "BY project_id AS `pi`, transaction AS `tn`",
            "WHERE project_id IN tuple(1)",
        ),
        id="columns can have aliases",
    ),
]


@pytest.mark.parametrize("query, clauses", tests)
def test_print_query(query: Query, clauses: Sequence[str]) -> None:
    expected = " ".join(clauses)
    assert str(query) == expected


@pytest.mark.parametrize("query, clauses", tests)
def test_pretty_print_query(query: Query, clauses: Sequence[str]) -> None:
    expected = "\n".join(clauses)
    assert query.print() == expected


@pytest.mark.parametrize("query, clauses", tests)
def test_translate_query(query: Query, clauses: Sequence[str]) -> None:
    expected = " ".join(clauses)
    assert query.serialize() == expected

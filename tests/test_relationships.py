from __future__ import annotations

from typing import Sequence

import pytest

from snuba_sdk.column import Column
from snuba_sdk.conditions import Condition, Op, Or
from snuba_sdk.entity import Entity
from snuba_sdk.function import Function
from snuba_sdk.orderby import Direction, OrderBy
from snuba_sdk.query import Query
from snuba_sdk.relationships import Join, Relationship

tests = [
    pytest.param(
        Query(
            Join([Relationship(Entity("events", "e"), "has", Entity("sessions", "s"))]),
        )
        .set_select(
            [
                Column("group_id", Entity("events", "e")),
                Column("span_id", Entity("sessions", "s")),
            ]
        )
        .set_where(
            [Condition(Column("timestamp", Entity("events", "e")), Op.IS_NOT_NULL)]
        )
        .set_orderby(
            [OrderBy(Column("timestamp", Entity("events", "e")), Direction.DESC)]
        )
        .set_limit(10)
        .set_offset(1)
        .set_granularity(3600),
        (
            "MATCH (e: events) -[has]-> (s: sessions)",
            "SELECT e.group_id, s.span_id",
            "WHERE e.timestamp IS NOT NULL",
            "ORDER BY e.timestamp DESC",
            "LIMIT 10",
            "OFFSET 1",
            "GRANULARITY 3600",
        ),
        id="simple join",
    ),
    pytest.param(
        Query(
            Join(
                [
                    Relationship(Entity("events", "e"), "has", Entity("sessions", "s")),
                    Relationship(
                        Entity("events", "e"),
                        "hasnt",
                        Entity("transactions", "t", 10.0),
                    ),
                    Relationship(
                        Entity("events", "e"), "musnt", Entity("sessions", "s")
                    ),
                ]
            ),
        )
        .set_select(
            [
                Column("group_id", Entity("events", "e")),
                Column("span_id", Entity("sessions", "s")),
                Column("trace_id", Entity("transactions", "t")),
                Function("count", [], "count"),
            ]
        )
        .set_groupby(
            [
                Column("group_id", Entity("events", "e")),
                Column("span_id", Entity("sessions", "s")),
                Column("trace_id", Entity("transactions", "t")),
            ]
        )
        .set_where(
            [
                Or(
                    [
                        Condition(
                            Column("timestamp", Entity("events", "e")), Op.IS_NOT_NULL
                        ),
                        Condition(
                            Column("timestamp", Entity("sessions", "s")), Op.IS_NOT_NULL
                        ),
                        Condition(
                            Column("timestamp", Entity("transactions", "t")),
                            Op.IS_NOT_NULL,
                        ),
                    ]
                )
            ],
        )
        .set_orderby(
            [OrderBy(Column("timestamp", Entity("events", "e")), Direction.DESC)]
        )
        .set_limit(10)
        .set_offset(1)
        .set_granularity(3600),
        (
            "MATCH (e: events) -[has]-> (s: sessions), (e: events) -[hasnt]-> (t: transactions SAMPLE 10.0), (e: events) -[musnt]-> (s: sessions)",
            "SELECT e.group_id, s.span_id, t.trace_id, count() AS `count`",
            "BY e.group_id, s.span_id, t.trace_id",
            "WHERE (e.timestamp IS NOT NULL OR s.timestamp IS NOT NULL OR t.timestamp IS NOT NULL)",
            "ORDER BY e.timestamp DESC",
            "LIMIT 10",
            "OFFSET 1",
            "GRANULARITY 3600",
        ),
        id="complex join",
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

from snuba_sdk.column import Column
from snuba_sdk.conditions import BooleanCondition, BooleanOp, Condition, Op
from snuba_sdk.entity import Entity
from snuba_sdk.query import Query
from snuba_sdk.query_optimizers.or_optimizer import OrOptimizer


def test_basic() -> None:
    condition_group = [
        BooleanCondition(
            BooleanOp.OR,
            [
                Condition(Column("transaction"), Op.EQ, "a"),
                Condition(Column("transaction"), Op.EQ, "b"),
                Condition(Column("transaction"), Op.EQ, "c"),
            ],
        )
    ]
    expected = [Condition(Column("transaction"), Op.IN, ["a", "b", "c"])]
    actual = OrOptimizer().optimize(condition_group)
    assert actual == expected


def test_unsupported() -> None:
    condition_group = [
        BooleanCondition(
            BooleanOp.OR,
            [
                Condition(Column("transaction"), Op.EQ, "a"),
                BooleanCondition(
                    BooleanOp.OR,
                    [
                        Condition(Column("transaction"), Op.EQ, "b"),
                        Condition(Column("transaction"), Op.EQ, "c"),
                    ],
                ),
            ],
        )
    ]
    actual = OrOptimizer().optimize(condition_group)
    assert actual == None


def test_snql() -> None:
    query = (
        Query(Entity("events"))
        .set_select([Column("event_id")])
        .set_where(
            [
                BooleanCondition(
                    BooleanOp.OR,
                    [
                        Condition(Column("transaction"), Op.EQ, "a"),
                        Condition(Column("transaction"), Op.EQ, "b"),
                        Condition(Column("transaction"), Op.EQ, "c"),
                    ],
                ),
            ]
        )
        .set_limit(10)
        .set_offset(1)
        .set_granularity(3600)
    )
    expected = (
        "MATCH (events)",
        "SELECT event_id",
        "WHERE transaction IN array('a', 'b', 'c')",
        "LIMIT 10",
        "OFFSET 1",
        "GRANULARITY 3600",
    )

    actual = query.serialize()
    assert " ".join(expected) == actual

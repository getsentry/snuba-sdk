from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Optional

import pytest

from snuba_sdk.column import Column
from snuba_sdk.conditions import And, Condition, ConditionGroup, Op, Or
from snuba_sdk.entity import Entity
from snuba_sdk.query import Query
from snuba_sdk.query_validation import RequiredColumnError, validate_required_columns
from snuba_sdk.query_visitors import ExpressionSearcher
from snuba_sdk.relationships import Join, Relationship
from snuba_sdk.schema import Column as ColumnModel
from snuba_sdk.schema import DataModel

SCHEMA = DataModel(
    [
        ColumnModel("test1"),
        ColumnModel("test2"),
        ColumnModel("required1", required=True),
        ColumnModel("required2", required=True),
        ColumnModel("time"),
    ],
    required_time_column=ColumnModel("time"),
)
ENTITY = Entity("test", None, None, SCHEMA)
BEFORE = datetime(2021, 1, 2, 3, 4, 5, 5, timezone.utc)
AFTER = datetime(2021, 1, 2, 3, 4, 5, 6, timezone.utc)
SEARCHER = ExpressionSearcher(Column)

entity_match_tests = [
    pytest.param(
        [Condition(Column("test1"), Op.IN, [1, 2, 3])],
        Entity("test"),
        None,
        id="entity has no data model",
    ),
    pytest.param(
        [],
        ENTITY,
        RequiredColumnError("where clause is missing required columns"),
        id="query has no conditions",
    ),
    pytest.param(
        [
            Condition(Column("required1"), Op.IN, [1, 2, 3]),
            Condition(Column("required2"), Op.EQ, 1),
            Condition(Column("time"), Op.GTE, BEFORE),
            Condition(Column("time"), Op.LT, AFTER),
        ],
        ENTITY,
        None,
        id="all required columns",
    ),
    pytest.param(
        [
            And(
                [
                    Condition(Column("required1"), Op.IN, [1, 2, 3]),
                    Condition(Column("required2"), Op.EQ, 1),
                ]
            ),
            Condition(Column("time"), Op.GTE, BEFORE),
            Condition(Column("time"), Op.LT, AFTER),
        ],
        ENTITY,
        None,
        id="all required columns in nested And",
    ),
    pytest.param(
        [
            Condition(Column("required2"), Op.EQ, 1),
            Condition(Column("time"), Op.GTE, BEFORE),
            Condition(Column("time"), Op.LT, AFTER),
        ],
        ENTITY,
        RequiredColumnError(
            "where clause is missing required condition on column 'required1'"
        ),
        id="missing required columns",
    ),
    pytest.param(
        [
            Or(
                [
                    Condition(Column("required1"), Op.IN, [1, 2, 3]),
                    Condition(Column("required2"), Op.EQ, 1),
                ]
            ),
            Condition(Column("time"), Op.GTE, BEFORE),
            Condition(Column("time"), Op.LT, AFTER),
        ],
        ENTITY,
        RequiredColumnError(
            "where clause is missing required conditions on columns 'required1', 'required2'"
        ),
        id="all required columns in OR",
    ),
    pytest.param(
        [
            Condition(Column("required1"), Op.NOT_IN, [1, 2, 3]),
            Condition(Column("required2"), Op.EQ, 1),
            Condition(Column("time"), Op.GTE, BEFORE),
            Condition(Column("time"), Op.LT, AFTER),
        ],
        ENTITY,
        RequiredColumnError(
            "where clause is missing required condition on column 'required1'"
        ),
        id="wrong ops",
    ),
    pytest.param(
        [
            Condition(Column("required1"), Op.IN, [1, 2, 3]),
            Condition(Column("required2"), Op.EQ, 1),
            Condition(Column("time"), Op.GTE, BEFORE),
        ],
        ENTITY,
        RequiredColumnError(
            "where clause is missing required < condition on column 'time'"
        ),
        id="missing time op",
    ),
    pytest.param(
        [
            Condition(Column("required1"), Op.IN, [1, 2, 3]),
            Condition(Column("required2"), Op.EQ, 1),
            Condition(Column("time"), Op.GTE, BEFORE),
            Condition(Column("time"), Op.LTE, AFTER),
        ],
        ENTITY,
        RequiredColumnError(
            "where clause is missing required < condition on column 'time'"
        ),
        id="wrong time op",
    ),
]


@pytest.mark.parametrize("conditions, entity, exception", entity_match_tests)
def test_entity_validate_match(
    conditions: ConditionGroup,
    entity: Entity,
    exception: Optional[Exception],
) -> None:
    query = Query(match=entity, select=[Column("test1"), Column("required1")])
    query = query.set_where(conditions)

    if exception is not None:
        with pytest.raises(type(exception), match=re.escape(str(exception))):
            validate_required_columns(query)
    else:
        validate_required_columns(query)


JOIN_ENTITY = Entity("test_a", "ta", None, SCHEMA)
join_match_tests = [
    pytest.param(
        [Condition(Column("test1", Entity("test_a", "ta")), Op.IN, [1, 2, 3])],
        Entity("test_a", "ta"),
        None,
        id="entities have no data model",
    ),
    pytest.param(
        [],
        JOIN_ENTITY,
        RequiredColumnError(
            "where clause is missing required conditions on columns 'ta.required1', 'ta.required2'"
        ),
        id="one entity has no conditions",
    ),
    pytest.param(
        [
            Condition(Column("required1", JOIN_ENTITY), Op.IN, [1, 2, 3]),
            Condition(Column("required2", JOIN_ENTITY), Op.EQ, 1),
            Condition(Column("time", JOIN_ENTITY), Op.GTE, BEFORE),
            Condition(Column("time", JOIN_ENTITY), Op.LT, AFTER),
        ],
        JOIN_ENTITY,
        None,
        id="all required columns",
    ),
    pytest.param(
        [
            And(
                [
                    Condition(Column("required1", JOIN_ENTITY), Op.IN, [1, 2, 3]),
                    Condition(Column("required2", JOIN_ENTITY), Op.EQ, 1),
                ]
            ),
            Condition(Column("time", JOIN_ENTITY), Op.GTE, BEFORE),
            Condition(Column("time", JOIN_ENTITY), Op.LT, AFTER),
        ],
        JOIN_ENTITY,
        None,
        id="all required columns in nested And",
    ),
    pytest.param(
        [
            Condition(Column("required2", JOIN_ENTITY), Op.EQ, 1),
            Condition(Column("time", JOIN_ENTITY), Op.GTE, BEFORE),
            Condition(Column("time", JOIN_ENTITY), Op.LT, AFTER),
        ],
        JOIN_ENTITY,
        RequiredColumnError(
            "where clause is missing required condition on column 'ta.required1'"
        ),
        id="missing required columns",
    ),
    pytest.param(
        [
            Or(
                [
                    Condition(Column("required1", JOIN_ENTITY), Op.IN, [1, 2, 3]),
                    Condition(Column("required2", JOIN_ENTITY), Op.EQ, 1),
                ]
            ),
            Condition(Column("time", JOIN_ENTITY), Op.GTE, BEFORE),
            Condition(Column("time", JOIN_ENTITY), Op.LT, AFTER),
        ],
        JOIN_ENTITY,
        RequiredColumnError(
            "where clause is missing required conditions on columns 'ta.required1', 'ta.required2'"
        ),
        id="all required columns in OR",
    ),
    pytest.param(
        [
            Condition(Column("required1", JOIN_ENTITY), Op.NOT_IN, [1, 2, 3]),
            Condition(Column("required2", JOIN_ENTITY), Op.EQ, 1),
            Condition(Column("time", JOIN_ENTITY), Op.GTE, BEFORE),
            Condition(Column("time", JOIN_ENTITY), Op.LT, AFTER),
        ],
        JOIN_ENTITY,
        RequiredColumnError(
            "where clause is missing required condition on column 'ta.required1'"
        ),
        id="wrong ops",
    ),
    pytest.param(
        [
            Condition(Column("required1", JOIN_ENTITY), Op.IN, [1, 2, 3]),
            Condition(Column("required2", JOIN_ENTITY), Op.EQ, 1),
            Condition(Column("time", JOIN_ENTITY), Op.GTE, BEFORE),
        ],
        JOIN_ENTITY,
        RequiredColumnError(
            "where clause is missing required < condition on column 'ta.time'"
        ),
        id="missing time op",
    ),
    pytest.param(
        [
            Condition(Column("required1", JOIN_ENTITY), Op.IN, [1, 2, 3]),
            Condition(Column("required2", JOIN_ENTITY), Op.EQ, 1),
            Condition(Column("time", JOIN_ENTITY), Op.GTE, BEFORE),
            Condition(Column("time", JOIN_ENTITY), Op.LTE, AFTER),
        ],
        JOIN_ENTITY,
        RequiredColumnError(
            "where clause is missing required < condition on column 'ta.time'"
        ),
        id="wrong time op",
    ),
    pytest.param(
        [
            Condition(
                Column("required1", Entity("test_b", "tb", None, SCHEMA)),
                Op.IN,
                [1, 2, 3],
            ),
            Condition(Column("required2", JOIN_ENTITY), Op.EQ, 1),
            Condition(
                Column("time", Entity("test_b", "tb", None, SCHEMA)), Op.GTE, BEFORE
            ),
            Condition(Column("time", JOIN_ENTITY), Op.LTE, AFTER),
        ],
        JOIN_ENTITY,
        RequiredColumnError(
            "where clause is missing required condition on column 'ta.required1'"
        ),
        id="right column, wrong entity",
    ),
]


@pytest.mark.parametrize("conditions, entity, exception", join_match_tests)
def test_join_validate_match(
    conditions: ConditionGroup,
    entity: Entity,
    exception: Optional[Exception],
) -> None:
    other_join_entity = Entity("test_b", "tb", None, SCHEMA)
    join2_conditions = [
        Condition(Column("required1", other_join_entity), Op.IN, [1, 2, 3]),
        Condition(Column("required2", other_join_entity), Op.EQ, 1),
        Condition(Column("time", other_join_entity), Op.GTE, BEFORE),
        Condition(Column("time", other_join_entity), Op.LT, AFTER),
        *conditions,
    ]
    query = Query(
        match=Join([Relationship(entity, "has", other_join_entity)]),
        select=[Column("test1", entity), Column("required1", other_join_entity)],
        where=join2_conditions,
    )

    if exception is not None:
        with pytest.raises(type(exception), match=re.escape(str(exception))):
            validate_required_columns(query)
    else:
        validate_required_columns(query)

import re
from datetime import datetime, timezone
from typing import Optional

import pytest

from snuba_sdk.column import Column, InvalidColumnError
from snuba_sdk.conditions import Condition, Op
from snuba_sdk.entity import Entity
from snuba_sdk.function import Function
from snuba_sdk.query import Query
from snuba_sdk.query_validation import InvalidMatchError, validate_match
from snuba_sdk.query_visitors import ExpressionSearcher
from snuba_sdk.relationships import Join, Relationship
from snuba_sdk.schema import Column as ColumnModel
from snuba_sdk.schema import EntityModel

SCHEMA = EntityModel(
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
        Query(
            match=ENTITY,
            select=[Column("test1"), Column("required1")],
        ),
        None,
        id="all columns in data model",
    ),
    pytest.param(
        Query(
            match=ENTITY,
            select=[Column("test1"), Column("outside")],
        ),
        InvalidColumnError("entity 'test' does not support the column 'outside'"),
        id="some columns not in data model",
    ),
]


@pytest.mark.parametrize("query, exception", entity_match_tests)
def test_entity_validate_match(query: Query, exception: Optional[Exception]) -> None:
    query = query.set_where(
        [
            Condition(Column("required1"), Op.IN, [1, 2, 3]),
            Condition(Column("required2"), Op.EQ, 1),
            Condition(Column("time"), Op.GTE, BEFORE),
            Condition(Column("time"), Op.LT, AFTER),
        ],
    )

    if exception is not None:
        with pytest.raises(type(exception), match=re.escape(str(exception))):
            validate_match(query, SEARCHER)
    else:
        validate_match(query, SEARCHER)


subquery_match_tests = [
    pytest.param(
        Query(
            Query(
                match=ENTITY,
                select=[Column("test1"), Column("test2")],
                where=[
                    Condition(Column("required1"), Op.IN, [1, 2, 3]),
                    Condition(Column("required2"), Op.EQ, 1),
                    Condition(Column("time"), Op.GTE, BEFORE),
                    Condition(Column("time"), Op.LT, AFTER),
                ],
            ),
        )
        .set_select(
            [Function("uniq", [Column("test1")], "uniq_event"), Column("test2")]
        )
        .set_groupby([Column("test2")]),
        None,
        id="subquery referencing correct columns",
    ),
    pytest.param(
        Query(
            Query(
                match=ENTITY,
                select=[Column("test1"), Column("test2")],
                where=[
                    Condition(Column("required1"), Op.IN, [1, 2, 3]),
                    Condition(Column("required2"), Op.EQ, 1),
                    Condition(Column("time"), Op.GTE, BEFORE),
                    Condition(Column("time"), Op.LT, AFTER),
                ],
            ),
        )
        .set_select(
            [Function("uniq", [Column("test1")], "uniq_event"), Column("outside")]
        )
        .set_groupby([Column("outside")]),
        InvalidMatchError(
            "outer query is referencing column 'outside' that does not exist in subquery"
        ),
        id="subquery with incorrect columns",
    ),
    pytest.param(
        Query(
            match=Query(
                match=Query(
                    match=ENTITY,
                    select=[Column("test1"), Column("test2")],
                    where=[
                        Condition(Column("required1"), Op.IN, [1, 2, 3]),
                        Condition(Column("required2"), Op.EQ, 1),
                        Condition(Column("time"), Op.GTE, BEFORE),
                        Condition(Column("time"), Op.LT, AFTER),
                    ],
                ),
                select=[
                    Function("uniq", [Column("test1")], "uniq_event"),
                    Column("test1"),
                ],
                groupby=[Column("test1")],
            ),
        ).set_select([Function("toString", [Column("outside")])]),
        InvalidMatchError(
            "outer query is referencing column 'outside' that does not exist in subquery"
        ),
        id="sub sub query with incorrect columns",
    ),
]


@pytest.mark.parametrize("query, exception", subquery_match_tests)
def test_subquery_validate_match(query: Query, exception: Optional[Exception]) -> None:
    if exception is not None:
        with pytest.raises(type(exception), match=re.escape(str(exception))):
            validate_match(query, SEARCHER)
    else:
        validate_match(query, SEARCHER)


JOIN1 = Entity("test_a", "ta", None, SCHEMA)
JOIN2 = Entity("test_b", "tb", None, SCHEMA)
JOIN = Join([Relationship(JOIN1, "has", JOIN2)])
join_match_tests = [
    pytest.param(
        Query(JOIN)
        .set_select([Column("test1"), Column("test2", JOIN2)])
        .set_where([Condition(Column("time", JOIN1), Op.IS_NOT_NULL)]),
        InvalidMatchError("column 'test1' must have a qualifying entity"),
        id="all columns must be qualified",
    ),
    pytest.param(
        Query(JOIN)
        .set_select(
            [Column("test1", Entity("transactions", "t")), Column("test2", JOIN2)]
        )
        .set_where([Condition(Column("time", JOIN1), Op.IS_NOT_NULL)]),
        InvalidMatchError("column 'test1' has unknown entity alias t"),
        id="column with different entity",
    ),
    pytest.param(
        Query(JOIN)
        .set_select(
            [
                Column("test1", Entity("test_a", "other", None, SCHEMA)),
                Column("test2", JOIN2),
            ]
        )
        .set_where([Condition(Column("time", JOIN1), Op.IS_NOT_NULL)]),
        InvalidMatchError("column 'test1' has unknown entity alias other"),
        id="column with different entity alias",
    ),
    pytest.param(
        Query(JOIN)
        .set_select(
            [
                Column("test1", Entity("test_a", "tb", None, SCHEMA)),
                Column("test2", JOIN2),
            ]
        )
        .set_where([Condition(Column("time", JOIN1), Op.IS_NOT_NULL)]),
        InvalidMatchError(
            "column 'test1' has incorrect alias for entity Entity('test_a', alias='tb'): 'tb'"
        ),
        id="duplicate entity alias",
    ),
]


@pytest.mark.parametrize("query, exception", join_match_tests)
def test_invalid_join(query: Query, exception: Optional[Exception]) -> None:
    if exception is not None:
        with pytest.raises(type(exception), match=re.escape(str(exception))):
            validate_match(query, SEARCHER)
    else:
        validate_match(query, SEARCHER)

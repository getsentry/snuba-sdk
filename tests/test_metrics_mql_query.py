from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from snuba_sdk.column import Column
from snuba_sdk.conditions import And, BooleanCondition, BooleanOp, Condition, Op
from snuba_sdk.expressions import Limit, Offset
from snuba_sdk.formula import ArithmeticOperator, Formula
from snuba_sdk.metrics_query import MetricsQuery
from snuba_sdk.metrics_query_visitors import InvalidMetricsQueryError
from snuba_sdk.mql.mql import parse_mql
from snuba_sdk.orderby import Direction
from snuba_sdk.timeseries import Metric, MetricsScope, Rollup, Timeseries

NOW = datetime(2023, 1, 2, 3, 4, 5, 0, timezone.utc)


metrics_query_timeseries_to_mql_tests = [
    pytest.param(
        MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    mri="d:transactions/duration@millisecond",
                    entity="generic_metrics_distributions",
                ),
                aggregate="max",
                aggregate_params=None,
                filters=None,
                groupby=None,
            ),
            start=NOW,
            end=NOW + timedelta(days=14),
            rollup=Rollup(interval=3600, totals=None, granularity=3600),
            scope=MetricsScope(
                org_ids=[1], project_ids=[11], use_case_id="transactions"
            ),
            indexer_mappings={},
        ),
        {
            "mql": "max(d:transactions/duration@millisecond)",
            "mql_context": {
                "entity": {
                    "d:transactions/duration@millisecond": "generic_metrics_distributions"
                },
                "start": "2023-01-02T03:04:05+00:00",
                "end": "2023-01-16T03:04:05+00:00",
                "rollup": {
                    "orderby": None,
                    "granularity": 3600,
                    "interval": 3600,
                    "with_totals": None,
                },
                "scope": {
                    "org_ids": [1],
                    "project_ids": [11],
                    "use_case_id": "transactions",
                },
                "limit": None,
                "offset": None,
                "indexer_mappings": {},
            },
        },
        id="basic mri query",
    ),
    pytest.param(
        MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    mri="d:transactions/duration@millisecond",
                    entity="generic_metrics_distributions",
                ),
                aggregate="quantiles",
                aggregate_params=[0.5],
                filters=None,
                groupby=None,
            ),
            start=NOW,
            end=NOW + timedelta(days=14),
            rollup=Rollup(interval=3600, totals=None, granularity=3600),
            scope=MetricsScope(
                org_ids=[1], project_ids=[11], use_case_id="transactions"
            ),
            indexer_mappings={},
        ),
        {
            "mql": "quantiles(0.5)(d:transactions/duration@millisecond)",
            "mql_context": {
                "entity": {
                    "d:transactions/duration@millisecond": "generic_metrics_distributions"
                },
                "start": "2023-01-02T03:04:05+00:00",
                "end": "2023-01-16T03:04:05+00:00",
                "rollup": {
                    "orderby": None,
                    "granularity": 3600,
                    "interval": 3600,
                    "with_totals": None,
                },
                "scope": {
                    "org_ids": [1],
                    "project_ids": [11],
                    "use_case_id": "transactions",
                },
                "limit": None,
                "offset": None,
                "indexer_mappings": {},
            },
        },
        id="basic curried query",
    ),
    pytest.param(
        MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    mri="d:transactions/duration@millisecond",
                    entity="generic_metrics_distributions",
                ),
                aggregate="topK",
                aggregate_params=[10],
                filters=None,
                groupby=None,
            ),
            start=NOW,
            end=NOW + timedelta(days=14),
            rollup=Rollup(interval=3600, totals=None, granularity=3600),
            scope=MetricsScope(
                org_ids=[1], project_ids=[11], use_case_id="transactions"
            ),
            indexer_mappings={},
        ),
        {
            "mql": "topK(10)(d:transactions/duration@millisecond)",
            "mql_context": {
                "entity": {
                    "d:transactions/duration@millisecond": "generic_metrics_distributions"
                },
                "start": "2023-01-02T03:04:05+00:00",
                "end": "2023-01-16T03:04:05+00:00",
                "rollup": {
                    "orderby": None,
                    "granularity": 3600,
                    "interval": 3600,
                    "with_totals": None,
                },
                "scope": {
                    "org_ids": [1],
                    "project_ids": [11],
                    "use_case_id": "transactions",
                },
                "limit": None,
                "offset": None,
                "indexer_mappings": {},
            },
        },
        id="basic arbitrary curried query",
    ),
    pytest.param(
        MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    public_name="transactions.duration",
                    entity="generic_metrics_distributions",
                ),
                aggregate="max",
                aggregate_params=None,
                filters=None,
                groupby=None,
            ),
            start=NOW,
            end=NOW + timedelta(days=14),
            rollup=Rollup(totals=True, orderby=Direction.DESC, granularity=3600),
            scope=MetricsScope(
                org_ids=[1], project_ids=[11], use_case_id="transactions"
            ),
            indexer_mappings={},
        ),
        {
            "mql": "max(transactions.duration)",
            "mql_context": {
                "entity": {"transactions.duration": "generic_metrics_distributions"},
                "start": "2023-01-02T03:04:05+00:00",
                "end": "2023-01-16T03:04:05+00:00",
                "rollup": {
                    "orderby": "DESC",
                    "granularity": 3600,
                    "interval": None,
                    "with_totals": "True",
                },
                "scope": {
                    "org_ids": [1],
                    "project_ids": [11],
                    "use_case_id": "transactions",
                },
                "limit": None,
                "offset": None,
                "indexer_mappings": {},
            },
        },
        id="basic public name query",
    ),
    pytest.param(
        MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    mri="d:transactions/duration@millisecond",
                    entity="generic_metrics_distributions",
                ),
                aggregate="max",
                aggregate_params=None,
                filters=[Condition(Column("bar"), Op.EQ, "baz")],
                groupby=None,
            ),
            start=NOW,
            end=NOW + timedelta(days=14),
            rollup=Rollup(interval=3600, totals=None, granularity=3600),
            scope=MetricsScope(
                org_ids=[1], project_ids=[11], use_case_id="transactions"
            ),
            indexer_mappings={},
        ),
        {
            "mql": 'max(d:transactions/duration@millisecond){bar:"baz"}',
            "mql_context": {
                "entity": {
                    "d:transactions/duration@millisecond": "generic_metrics_distributions"
                },
                "start": "2023-01-02T03:04:05+00:00",
                "end": "2023-01-16T03:04:05+00:00",
                "rollup": {
                    "orderby": None,
                    "granularity": 3600,
                    "interval": 3600,
                    "with_totals": None,
                },
                "scope": {
                    "org_ids": [1],
                    "project_ids": [11],
                    "use_case_id": "transactions",
                },
                "limit": None,
                "offset": None,
                "indexer_mappings": {},
            },
        },
        id="filter query",
    ),
    pytest.param(
        MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    mri="d:transactions/duration@millisecond",
                    entity="generic_metrics_distributions",
                ),
                aggregate="max",
                aggregate_params=None,
                filters=[Condition(Column("bar"), Op.IN, ["baz", "bap"])],
                groupby=None,
            ),
            start=NOW,
            end=NOW + timedelta(days=14),
            rollup=Rollup(interval=3600, totals=None, granularity=3600),
            scope=MetricsScope(
                org_ids=[1], project_ids=[11], use_case_id="transactions"
            ),
            indexer_mappings={},
        ),
        {
            "mql": 'max(d:transactions/duration@millisecond){bar:["baz", "bap"]}',
            "mql_context": {
                "entity": {
                    "d:transactions/duration@millisecond": "generic_metrics_distributions"
                },
                "start": "2023-01-02T03:04:05+00:00",
                "end": "2023-01-16T03:04:05+00:00",
                "rollup": {
                    "orderby": None,
                    "granularity": 3600,
                    "interval": 3600,
                    "with_totals": None,
                },
                "scope": {
                    "org_ids": [1],
                    "project_ids": [11],
                    "use_case_id": "transactions",
                },
                "limit": None,
                "offset": None,
                "indexer_mappings": {},
            },
        },
        id="in filter query",
    ),
    pytest.param(
        MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    mri="d:transactions/duration@millisecond",
                    entity="generic_metrics_distributions",
                ),
                aggregate="max",
                aggregate_params=None,
                filters=[
                    Condition(Column("bar"), Op.EQ, "baz"),
                    Condition(Column("foo"), Op.EQ, "foz"),
                    BooleanCondition(
                        op=BooleanOp.OR,
                        conditions=[
                            Condition(Column("foo"), Op.EQ, "foz"),
                            Condition(Column("hee"), Op.EQ, "hez"),
                            # We test with both `BooleanCondition` and `And` to make sure the variants all work.
                            And(
                                conditions=[
                                    Condition(Column("foo"), Op.EQ, "foz"),
                                    Condition(Column("hee"), Op.EQ, "hez"),
                                ],
                            ),
                        ],
                    ),
                ],
                groupby=None,
            ),
            start=NOW,
            end=NOW + timedelta(days=14),
            rollup=Rollup(interval=3600, totals=None, granularity=3600),
            scope=MetricsScope(
                org_ids=[1], project_ids=[11], use_case_id="transactions"
            ),
            indexer_mappings={},
        ),
        {
            "mql": 'max(d:transactions/duration@millisecond){bar:"baz" AND foo:"foz" AND (foo:"foz" OR hee:"hez" OR (foo:"foz" AND hee:"hez"))}',
            "mql_context": {
                "entity": {
                    "d:transactions/duration@millisecond": "generic_metrics_distributions"
                },
                "start": "2023-01-02T03:04:05+00:00",
                "end": "2023-01-16T03:04:05+00:00",
                "rollup": {
                    "orderby": None,
                    "granularity": 3600,
                    "interval": 3600,
                    "with_totals": None,
                },
                "scope": {
                    "org_ids": [1],
                    "project_ids": [11],
                    "use_case_id": "transactions",
                },
                "limit": None,
                "offset": None,
                "indexer_mappings": {},
            },
        },
        id="multiple nested filters query",
    ),
    pytest.param(
        MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    mri="d:transactions/duration@millisecond",
                    entity="generic_metrics_distributions",
                ),
                aggregate="max",
                aggregate_params=None,
                filters=None,
                groupby=[Column("transaction")],
            ),
            start=NOW,
            end=NOW + timedelta(days=14),
            rollup=Rollup(interval=3600, totals=None, granularity=3600),
            scope=MetricsScope(
                org_ids=[1], project_ids=[11], use_case_id="transactions"
            ),
            indexer_mappings={},
        ),
        {
            "mql": "max(d:transactions/duration@millisecond) by (transaction)",
            "mql_context": {
                "entity": {
                    "d:transactions/duration@millisecond": "generic_metrics_distributions"
                },
                "start": "2023-01-02T03:04:05+00:00",
                "end": "2023-01-16T03:04:05+00:00",
                "rollup": {
                    "orderby": None,
                    "granularity": 3600,
                    "interval": 3600,
                    "with_totals": None,
                },
                "scope": {
                    "org_ids": [1],
                    "project_ids": [11],
                    "use_case_id": "transactions",
                },
                "limit": None,
                "offset": None,
                "indexer_mappings": {},
            },
        },
        id="groupby query",
    ),
    pytest.param(
        MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    mri="d:transactions/duration@millisecond",
                    entity="generic_metrics_distributions",
                ),
                aggregate="max",
                aggregate_params=None,
                filters=None,
                groupby=[Column("a"), Column("b")],
            ),
            start=NOW,
            end=NOW + timedelta(days=14),
            rollup=Rollup(interval=3600, totals=None, granularity=3600),
            scope=MetricsScope(
                org_ids=[1], project_ids=[11], use_case_id="transactions"
            ),
            indexer_mappings={"d:transactions/duration@millisecond": 11235813},
        ),
        {
            "mql": "max(d:transactions/duration@millisecond) by (a, b)",
            "mql_context": {
                "entity": {
                    "d:transactions/duration@millisecond": "generic_metrics_distributions"
                },
                "start": "2023-01-02T03:04:05+00:00",
                "end": "2023-01-16T03:04:05+00:00",
                "rollup": {
                    "orderby": None,
                    "granularity": 3600,
                    "interval": 3600,
                    "with_totals": None,
                },
                "scope": {
                    "org_ids": [1],
                    "project_ids": [11],
                    "use_case_id": "transactions",
                },
                "limit": None,
                "offset": None,
                "indexer_mappings": {"d:transactions/duration@millisecond": 11235813},
            },
        },
        id="multiple groupby query",
    ),
    pytest.param(
        MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    mri="d:transactions/duration@millisecond",
                    entity="generic_metrics_distributions",
                ),
                aggregate="max",
                aggregate_params=None,
                filters=[Condition(Column("bar"), Op.EQ, "baz")],
                groupby=[Column("transaction")],
            ),
            start=NOW,
            end=NOW + timedelta(days=14),
            rollup=Rollup(interval=3600, totals=None, granularity=3600),
            scope=MetricsScope(
                org_ids=[1], project_ids=[11], use_case_id="transactions"
            ),
            limit=Limit(100),
            offset=Offset(5),
            indexer_mappings={},
        ),
        {
            "mql": 'max(d:transactions/duration@millisecond){bar:"baz"} by (transaction)',
            "mql_context": {
                "entity": {
                    "d:transactions/duration@millisecond": "generic_metrics_distributions"
                },
                "start": "2023-01-02T03:04:05+00:00",
                "end": "2023-01-16T03:04:05+00:00",
                "rollup": {
                    "orderby": None,
                    "granularity": 3600,
                    "interval": 3600,
                    "with_totals": None,
                },
                "scope": {
                    "org_ids": [1],
                    "project_ids": [11],
                    "use_case_id": "transactions",
                },
                "limit": 100,
                "offset": 5,
                "indexer_mappings": {},
            },
        },
        id="complex single timeseries query",
    ),
    pytest.param(
        MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    mri="d:transactions/duration@millisecond",
                    entity="generic_metrics_distributions",
                ),
                aggregate="max",
                aggregate_params=None,
                filters=[
                    Condition(
                        Column("bar"),
                        Op.EQ,
                        " !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~",
                    )
                ],
                groupby=[Column("transaction")],
            ),
            start=NOW,
            end=NOW + timedelta(days=14),
            rollup=Rollup(interval=3600, totals=None, granularity=3600),
            scope=MetricsScope(
                org_ids=[1], project_ids=[11], use_case_id="transactions"
            ),
            limit=Limit(100),
            offset=Offset(5),
            indexer_mappings={},
        ),
        {
            "mql": 'max(d:transactions/duration@millisecond){bar:" !\\"#$%&\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"} by (transaction)',
            "mql_context": {
                "entity": {
                    "d:transactions/duration@millisecond": "generic_metrics_distributions"
                },
                "start": "2023-01-02T03:04:05+00:00",
                "end": "2023-01-16T03:04:05+00:00",
                "rollup": {
                    "orderby": None,
                    "granularity": 3600,
                    "interval": 3600,
                    "with_totals": None,
                },
                "scope": {
                    "org_ids": [1],
                    "project_ids": [11],
                    "use_case_id": "transactions",
                },
                "limit": 100,
                "offset": 5,
                "indexer_mappings": {},
            },
        },
        id="test_crazy_characters",
    ),
    pytest.param(
        MetricsQuery(
            query="sum(transaction.duration){status_code:500} by transaction",
            start=NOW,
            end=NOW + timedelta(days=14),
            rollup=Rollup(interval=3600, totals=None, granularity=3600),
            scope=MetricsScope(
                org_ids=[1], project_ids=[11], use_case_id="transactions"
            ),
            limit=Limit(100),
            offset=Offset(5),
            indexer_mappings={},
        ),
        {
            "mql": 'sum(transaction.duration){status_code:"500"} by (transaction)',
            "mql_context": {
                "entity": {
                    "transaction.duration": None,
                },
                "start": "2023-01-02T03:04:05+00:00",
                "end": "2023-01-16T03:04:05+00:00",
                "rollup": {
                    "orderby": None,
                    "granularity": 3600,
                    "interval": 3600,
                    "with_totals": None,
                },
                "scope": {
                    "org_ids": [1],
                    "project_ids": [11],
                    "use_case_id": "transactions",
                },
                "limit": 100,
                "offset": 5,
                "indexer_mappings": {},
            },
        },
        id="test_passing_string_directly",
    ),
]


@pytest.mark.parametrize("query, translated", metrics_query_timeseries_to_mql_tests)
def test_metrics_query_to_mql_timeseries(
    query: MetricsQuery, translated: dict[str, Any]
) -> None:
    query.validate()
    serialized = query.serialize()
    assert isinstance(serialized, dict)
    assert serialized["mql"] == translated["mql"]
    assert serialized["mql_context"] == translated["mql_context"]
    assert (
        parse_mql(str(serialized["mql"])) is not None
    )  # ensure we can parse our own encoding


invalid_metrics_query_to_mql_tests = [
    pytest.param(
        MetricsQuery(
            query=None,
            start=NOW,
            end=NOW + timedelta(days=14),
            rollup=Rollup(interval=3600, totals=None, granularity=3600),
            scope=MetricsScope(
                org_ids=[1], project_ids=[11], use_case_id="transactions"
            ),
        ),
        InvalidMetricsQueryError("query is required for a metrics query"),
        id="missing query",
    ),
]


@pytest.mark.parametrize("query, exception", invalid_metrics_query_to_mql_tests)
def test_invalid_metrics_query_to_mql_tests(
    query: MetricsQuery, exception: Exception
) -> None:
    with pytest.raises(type(exception), match=re.escape(str(exception))):
        query.validate()


metrics_query_formula_to_mql_tests = [
    pytest.param(
        MetricsQuery(
            query=Formula(
                ArithmeticOperator.DIVIDE.value,
                [
                    Timeseries(
                        metric=Metric(
                            public_name="foo", entity="generic_metrics_distributions"
                        ),
                        aggregate="sum",
                    ),
                    1000,
                ],
            ),
            start=NOW,
            end=NOW + timedelta(days=14),
            rollup=Rollup(interval=3600, totals=None, granularity=3600),
            scope=MetricsScope(
                org_ids=[1], project_ids=[11], use_case_id="transactions"
            ),
            limit=Limit(100),
            offset=Offset(5),
            indexer_mappings={},
        ),
        {
            "mql": "(sum(foo) / 1000)",
            "mql_context": {
                "entity": {"foo": "generic_metrics_distributions"},
                "start": "2023-01-02T03:04:05+00:00",
                "end": "2023-01-16T03:04:05+00:00",
                "rollup": {
                    "orderby": None,
                    "granularity": 3600,
                    "interval": 3600,
                    "with_totals": None,
                },
                "scope": {
                    "org_ids": [1],
                    "project_ids": [11],
                    "use_case_id": "transactions",
                },
                "limit": 100,
                "offset": 5,
                "indexer_mappings": {},
            },
        },
        id="test_terms",
    ),
    pytest.param(
        MetricsQuery(
            query=Formula(
                "apdex",
                [
                    Timeseries(
                        metric=Metric(
                            public_name="foo", entity="generic_metrics_distributions"
                        ),
                        aggregate="sum",
                    ),
                    1000,
                ],
            ),
            start=NOW,
            end=NOW + timedelta(days=14),
            rollup=Rollup(interval=3600, totals=None, granularity=3600),
            scope=MetricsScope(
                org_ids=[1], project_ids=[11], use_case_id="transactions"
            ),
            limit=Limit(100),
            offset=Offset(5),
            indexer_mappings={},
        ),
        {
            "mql": "apdex(sum(foo), 1000)",
            "mql_context": {
                "entity": {"foo": "generic_metrics_distributions"},
                "start": "2023-01-02T03:04:05+00:00",
                "end": "2023-01-16T03:04:05+00:00",
                "rollup": {
                    "orderby": None,
                    "granularity": 3600,
                    "interval": 3600,
                    "with_totals": None,
                },
                "scope": {
                    "org_ids": [1],
                    "project_ids": [11],
                    "use_case_id": "transactions",
                },
                "limit": 100,
                "offset": 5,
                "indexer_mappings": {},
            },
        },
        id="test arbitrary function",
    ),
    pytest.param(
        MetricsQuery(
            query=Formula(
                "apdex",
                [
                    Timeseries(
                        metric=Metric(
                            public_name="foo", entity="generic_metrics_distributions"
                        ),
                        aggregate="quantiles",
                        aggregate_params=[0.5],
                    ),
                    1000,
                ],
            ),
            start=NOW,
            end=NOW + timedelta(days=14),
            rollup=Rollup(interval=3600, totals=None, granularity=3600),
            scope=MetricsScope(
                org_ids=[1], project_ids=[11], use_case_id="transactions"
            ),
            limit=Limit(100),
            offset=Offset(5),
            indexer_mappings={},
        ),
        {
            "mql": "apdex(quantiles(0.5)(foo), 1000)",
            "mql_context": {
                "entity": {"foo": "generic_metrics_distributions"},
                "start": "2023-01-02T03:04:05+00:00",
                "end": "2023-01-16T03:04:05+00:00",
                "rollup": {
                    "orderby": None,
                    "granularity": 3600,
                    "interval": 3600,
                    "with_totals": None,
                },
                "scope": {
                    "org_ids": [1],
                    "project_ids": [11],
                    "use_case_id": "transactions",
                },
                "limit": 100,
                "offset": 5,
                "indexer_mappings": {},
            },
        },
        id="test arbitrary function with curried aggregate",
    ),
    pytest.param(
        MetricsQuery(
            query=Formula(
                "apdex",
                [
                    Formula(
                        function_name="failure_rate",
                        parameters=[
                            Timeseries(
                                metric=Metric(
                                    public_name="foo",
                                    entity="generic_metrics_distributions",
                                ),
                                aggregate="sum",
                            ),
                        ],
                    ),
                    1000,
                ],
            ),
            start=NOW,
            end=NOW + timedelta(days=14),
            rollup=Rollup(interval=3600, totals=None, granularity=3600),
            scope=MetricsScope(
                org_ids=[1], project_ids=[11], use_case_id="transactions"
            ),
            limit=Limit(100),
            offset=Offset(5),
            indexer_mappings={},
        ),
        {
            "mql": "apdex(failure_rate(sum(foo)), 1000)",
            "mql_context": {
                "entity": {"foo": "generic_metrics_distributions"},
                "start": "2023-01-02T03:04:05+00:00",
                "end": "2023-01-16T03:04:05+00:00",
                "rollup": {
                    "orderby": None,
                    "granularity": 3600,
                    "interval": 3600,
                    "with_totals": None,
                },
                "scope": {
                    "org_ids": [1],
                    "project_ids": [11],
                    "use_case_id": "transactions",
                },
                "limit": 100,
                "offset": 5,
                "indexer_mappings": {},
            },
        },
        id="test nested arbitrary function",
    ),
    pytest.param(
        MetricsQuery(
            query=Formula(
                function_name="apdex",
                parameters=[
                    Formula(
                        function_name=ArithmeticOperator.DIVIDE.value,
                        parameters=[
                            Timeseries(
                                metric=Metric(
                                    public_name="foo",
                                    entity="generic_metrics_distributions",
                                ),
                                aggregate="sum",
                            ),
                            Timeseries(
                                metric=Metric(
                                    public_name="bar",
                                    entity="generic_metrics_distributions",
                                ),
                                aggregate="sum",
                            ),
                        ],
                    ),
                    500,
                ],
                filters=[Condition(Column("tag"), Op.EQ, "tag_value")],
                groupby=[Column("transaction")],
            ),
            start=NOW,
            end=NOW + timedelta(days=14),
            rollup=Rollup(interval=3600, totals=None, granularity=3600),
            scope=MetricsScope(
                org_ids=[1], project_ids=[11], use_case_id="transactions"
            ),
            limit=Limit(100),
            offset=Offset(5),
            indexer_mappings={},
        ),
        {
            "mql": 'apdex((sum(foo) / sum(bar)), 500){tag:"tag_value"} by (transaction)',
            "mql_context": {
                "entity": {
                    "foo": "generic_metrics_distributions",
                    "bar": "generic_metrics_distributions",
                },
                "start": "2023-01-02T03:04:05+00:00",
                "end": "2023-01-16T03:04:05+00:00",
                "rollup": {
                    "orderby": None,
                    "granularity": 3600,
                    "interval": 3600,
                    "with_totals": None,
                },
                "scope": {
                    "org_ids": [1],
                    "project_ids": [11],
                    "use_case_id": "transactions",
                },
                "limit": 100,
                "offset": 5,
                "indexer_mappings": {},
            },
        },
        id="test arbitrary function with inner term",
    ),
    pytest.param(
        MetricsQuery(
            query=Formula(
                function_name="topK",
                aggregate_params=[10],
                parameters=[
                    Formula(
                        function_name="divide",
                        parameters=[
                            Timeseries(
                                metric=Metric(
                                    public_name="transaction.duration",
                                    entity="generic_metrics_distributions",
                                ),
                                aggregate="sum",
                            ),
                            Timeseries(
                                metric=Metric(
                                    public_name="transaction.duration",
                                    entity="generic_metrics_distributions",
                                ),
                                aggregate="count",
                            ),
                        ],
                    ),
                ],
            ),
            start=NOW,
            end=NOW + timedelta(days=14),
            rollup=Rollup(interval=3600, totals=None, granularity=3600),
            scope=MetricsScope(
                org_ids=[1], project_ids=[11], use_case_id="transactions"
            ),
            limit=Limit(100),
            offset=Offset(5),
            indexer_mappings={},
        ),
        {
            "mql": "topK(10)((sum(transaction.duration) / count(transaction.duration)))",
            "mql_context": {
                "entity": {"transaction.duration": "generic_metrics_distributions"},
                "start": "2023-01-02T03:04:05+00:00",
                "end": "2023-01-16T03:04:05+00:00",
                "rollup": {
                    "orderby": None,
                    "granularity": 3600,
                    "interval": 3600,
                    "with_totals": None,
                },
                "scope": {
                    "org_ids": [1],
                    "project_ids": [11],
                    "use_case_id": "transactions",
                },
                "limit": 100,
                "offset": 5,
                "indexer_mappings": {},
            },
        },
        id="test curried arbitrary function with inner aggregate and terms",
    ),
    pytest.param(
        MetricsQuery(
            query=Formula(
                function_name="topK",
                aggregate_params=[10],
                parameters=[
                    Formula(
                        function_name="apdex",
                        parameters=[
                            Timeseries(
                                metric=Metric(
                                    public_name="transaction.duration",
                                    entity="generic_metrics_distributions",
                                ),
                                aggregate="sum",
                            ),
                            500,
                        ],
                        filters=[Condition(Column("bar"), Op.EQ, "baz")],
                    ),
                ],
            ),
            start=NOW,
            end=NOW + timedelta(days=14),
            rollup=Rollup(interval=3600, totals=None, granularity=3600),
            scope=MetricsScope(
                org_ids=[1], project_ids=[11], use_case_id="transactions"
            ),
            limit=Limit(100),
            offset=Offset(5),
            indexer_mappings={},
        ),
        {
            "mql": 'topK(10)(apdex(sum(transaction.duration), 500){bar:"baz"})',
            "mql_context": {
                "entity": {"transaction.duration": "generic_metrics_distributions"},
                "start": "2023-01-02T03:04:05+00:00",
                "end": "2023-01-16T03:04:05+00:00",
                "rollup": {
                    "orderby": None,
                    "granularity": 3600,
                    "interval": 3600,
                    "with_totals": None,
                },
                "scope": {
                    "org_ids": [1],
                    "project_ids": [11],
                    "use_case_id": "transactions",
                },
                "limit": 100,
                "offset": 5,
                "indexer_mappings": {},
            },
        },
        id="test curried arbitrary function with inner arbitrary function",
    ),
]


@pytest.mark.parametrize("query, translated", metrics_query_formula_to_mql_tests)
def test_metrics_query_to_mql_formula(
    query: MetricsQuery, translated: dict[str, Any]
) -> None:
    query.validate()
    serialized = query.serialize()
    assert isinstance(serialized, dict)
    assert serialized["mql"] == translated["mql"]
    assert serialized["mql_context"] == translated["mql_context"]
    assert parse_mql(str(serialized["mql"])) is not None

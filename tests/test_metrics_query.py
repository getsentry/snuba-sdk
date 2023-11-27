from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from snuba_sdk.aliased_expression import AliasedExpression
from snuba_sdk.column import Column
from snuba_sdk.conditions import Condition, Op
from snuba_sdk.expressions import Limit, Offset
from snuba_sdk.metrics_query import MetricsQuery
from snuba_sdk.metrics_query_visitors import InvalidMetricsQueryError
from snuba_sdk.timeseries import Metric, MetricsScope, Rollup, Timeseries

NOW = datetime(2023, 1, 2, 3, 4, 5, 0, timezone.utc)
metrics_query_to_snql_tests = [
    pytest.param(
        MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    "transaction.duration",
                    "d:transactions/duration@millisecond",
                    123,
                    "metrics_sets",
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
        ),
        "MATCH (metrics_sets) SELECT max(value) AS `aggregate_value` BY toStartOfInterval(timestamp, toIntervalSecond(3600), 'Universal') AS `time` WHERE granularity = 3600 AND metric_id = 123 AND (org_id IN array(1) AND project_id IN array(11) AND use_case_id = 'transactions') AND timestamp >= toDateTime('2023-01-02T03:04:05') AND timestamp < toDateTime('2023-01-16T03:04:05') ORDER BY time ASC",
        id="basic query",
    ),
    pytest.param(
        MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    "transaction.duration",
                    "d:transactions/duration@millisecond",
                    123,
                    "metrics_sets",
                ),
                aggregate="max",
                aggregate_params=None,
                filters=[Condition(Column("tags[transaction]"), Op.EQ, "foo")],
                groupby=[Column("tags[status_code]")],
            ),
            start=NOW,
            end=NOW + timedelta(days=14),
            rollup=Rollup(interval=3600, totals=None, granularity=3600),
            scope=MetricsScope(
                org_ids=[1], project_ids=[11], use_case_id="transactions"
            ),
        ),
        "MATCH (metrics_sets) SELECT max(value) AS `aggregate_value` BY toStartOfInterval(timestamp, toIntervalSecond(3600), 'Universal') AS `time`, tags[status_code] WHERE granularity = 3600 AND metric_id = 123 AND tags[transaction] = 'foo' AND (org_id IN array(1) AND project_id IN array(11) AND use_case_id = 'transactions') AND timestamp >= toDateTime('2023-01-02T03:04:05') AND timestamp < toDateTime('2023-01-16T03:04:05') ORDER BY time ASC",
        id="top level filters/group by",
    ),
    pytest.param(
        MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    "transaction.duration",
                    "d:transactions/duration@millisecond",
                    123,
                    "metrics_sets",
                ),
                aggregate="max",
                aggregate_params=None,
                filters=[
                    Condition(Column("tags[referrer]"), Op.EQ, "foo"),
                    Condition(Column("tags[transaction]"), Op.EQ, "foo"),
                ],
                groupby=[Column("tags[environment]"), Column("tags[status_code]")],
            ),
            start=NOW,
            end=NOW + timedelta(days=14),
            rollup=Rollup(interval=3600, totals=None, granularity=3600),
            scope=MetricsScope(
                org_ids=[1], project_ids=[11], use_case_id="transactions"
            ),
        ),
        "MATCH (metrics_sets) SELECT max(value) AS `aggregate_value` BY toStartOfInterval(timestamp, toIntervalSecond(3600), 'Universal') AS `time`, tags[environment], tags[status_code] WHERE granularity = 3600 AND metric_id = 123 AND tags[referrer] = 'foo' AND tags[transaction] = 'foo' AND (org_id IN array(1) AND project_id IN array(11) AND use_case_id = 'transactions') AND timestamp >= toDateTime('2023-01-02T03:04:05') AND timestamp < toDateTime('2023-01-16T03:04:05') ORDER BY time ASC",
        id="top level filters/group by with low level filters",
    ),
    pytest.param(
        MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    "transaction.duration",
                    "d:transactions/duration@millisecond",
                    123,
                    "metrics_sets",
                ),
                aggregate="max",
                aggregate_params=None,
                filters=[
                    Condition(Column("tags[referrer]"), Op.EQ, "foo"),
                    Condition(Column("tags[transaction]"), Op.EQ, "foo"),
                ],
                groupby=[Column("tags[environment]"), Column("tags[status_code]")],
            ),
            start=NOW,
            end=NOW + timedelta(days=14),
            rollup=Rollup(totals=True, granularity=60),
            scope=MetricsScope(
                org_ids=[1], project_ids=[11], use_case_id="transactions"
            ),
        ),
        "MATCH (metrics_sets) SELECT max(value) AS `aggregate_value` BY tags[environment], tags[status_code] WHERE granularity = 60 AND metric_id = 123 AND tags[referrer] = 'foo' AND tags[transaction] = 'foo' AND (org_id IN array(1) AND project_id IN array(11) AND use_case_id = 'transactions') AND timestamp >= toDateTime('2023-01-02T03:04:05') AND timestamp < toDateTime('2023-01-16T03:04:05')",
        id="totals query",
    ),
    pytest.param(
        MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    "transaction.duration",
                    "d:transactions/duration@millisecond",
                    123,
                    "metrics_sets",
                ),
                aggregate="quantiles",
                aggregate_params=[0.5, 0.99],
                groupby=[AliasedExpression(Column("tags[transaction]"), "transaction")],
            ),
            start=NOW,
            end=NOW + timedelta(days=14),
            rollup=Rollup(interval=60, granularity=60),
            scope=MetricsScope(
                org_ids=[1],
                project_ids=[11],
                use_case_id="transactions",
            ),
        ),
        "MATCH (metrics_sets) SELECT quantiles(0.5, 0.99)(value) AS `aggregate_value` BY toStartOfInterval(timestamp, toIntervalSecond(60), 'Universal') AS `time`, tags[transaction] AS `transaction` WHERE granularity = 60 AND metric_id = 123 AND (org_id IN array(1) AND project_id IN array(11) AND use_case_id = 'transactions') AND timestamp >= toDateTime('2023-01-02T03:04:05') AND timestamp < toDateTime('2023-01-16T03:04:05') ORDER BY time ASC",
        id="aliased groupby",
    ),
    pytest.param(
        MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    "transaction.duration",
                    "d:transactions/duration@millisecond",
                    123,
                    "metrics_sets",
                ),
                aggregate="quantiles",
                aggregate_params=[0.5, 0.99],
                groupby=[AliasedExpression(Column("tags[transaction]"), "transaction")],
            ),
            start=NOW,
            end=NOW + timedelta(days=14),
            rollup=Rollup(interval=60, granularity=60, totals=True),
            scope=MetricsScope(
                org_ids=[1],
                project_ids=[11],
                use_case_id="transactions",
            ),
        ),
        "MATCH (metrics_sets) SELECT quantiles(0.5, 0.99)(value) AS `aggregate_value` BY toStartOfInterval(timestamp, toIntervalSecond(60), 'Universal') AS `time`, tags[transaction] AS `transaction` WHERE granularity = 60 AND metric_id = 123 AND (org_id IN array(1) AND project_id IN array(11) AND use_case_id = 'transactions') AND timestamp >= toDateTime('2023-01-02T03:04:05') AND timestamp < toDateTime('2023-01-16T03:04:05') ORDER BY time ASC TOTALS True",
        id="with totals",
    ),
    pytest.param(
        MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    "transaction.duration",
                    "d:transactions/duration@millisecond",
                    123,
                    "metrics_sets",
                ),
                aggregate="quantiles",
                aggregate_params=[0.5, 0.99],
                groupby=[AliasedExpression(Column("tags[transaction]"), "transaction")],
            ),
            start=NOW,
            end=NOW + timedelta(days=14),
            rollup=Rollup(interval=60, granularity=60, totals=True),
            scope=MetricsScope(
                org_ids=[1],
                project_ids=[11],
                use_case_id="transactions",
            ),
            limit=Limit(100),
        ),
        "MATCH (metrics_sets) SELECT quantiles(0.5, 0.99)(value) AS `aggregate_value` BY toStartOfInterval(timestamp, toIntervalSecond(60), 'Universal') AS `time`, tags[transaction] AS `transaction` WHERE granularity = 60 AND metric_id = 123 AND (org_id IN array(1) AND project_id IN array(11) AND use_case_id = 'transactions') AND timestamp >= toDateTime('2023-01-02T03:04:05') AND timestamp < toDateTime('2023-01-16T03:04:05') ORDER BY time ASC LIMIT 100 TOTALS True",
        id="with totals",
    ),
    pytest.param(
        MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    "transaction.duration",
                    "d:transactions/duration@millisecond",
                    123,
                    "metrics_sets",
                ),
                aggregate="quantiles",
                aggregate_params=[0.5, 0.99],
                groupby=[AliasedExpression(Column("tags[transaction]"), "transaction")],
            ),
            start=NOW,
            end=NOW + timedelta(days=14),
            rollup=Rollup(interval=60, granularity=60, totals=True),
            scope=MetricsScope(
                org_ids=[1],
                project_ids=[11],
                use_case_id="transactions",
            ),
            offset=Offset(100),
        ),
        "MATCH (metrics_sets) SELECT quantiles(0.5, 0.99)(value) AS `aggregate_value` BY toStartOfInterval(timestamp, toIntervalSecond(60), 'Universal') AS `time`, tags[transaction] AS `transaction` WHERE granularity = 60 AND metric_id = 123 AND (org_id IN array(1) AND project_id IN array(11) AND use_case_id = 'transactions') AND timestamp >= toDateTime('2023-01-02T03:04:05') AND timestamp < toDateTime('2023-01-16T03:04:05') ORDER BY time ASC OFFSET 100 TOTALS True",
        id="with totals",
    ),
    pytest.param(
        MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    "transaction.duration",
                    "d:transactions/duration@millisecond",
                    123,
                    "metrics_sets",
                ),
                aggregate="quantiles",
                aggregate_params=[0.5, 0.99],
                groupby=[AliasedExpression(Column("tags[transaction]"), "transaction")],
            ),
            start=NOW,
            end=NOW + timedelta(days=14),
            rollup=Rollup(interval=60, granularity=60, totals=True),
            scope=MetricsScope(
                org_ids=[1],
                project_ids=[11],
                use_case_id="transactions",
            ),
            limit=Limit(100),
            offset=Offset(100),
        ),
        "MATCH (metrics_sets) SELECT quantiles(0.5, 0.99)(value) AS `aggregate_value` BY toStartOfInterval(timestamp, toIntervalSecond(60), 'Universal') AS `time`, tags[transaction] AS `transaction` WHERE granularity = 60 AND metric_id = 123 AND (org_id IN array(1) AND project_id IN array(11) AND use_case_id = 'transactions') AND timestamp >= toDateTime('2023-01-02T03:04:05') AND timestamp < toDateTime('2023-01-16T03:04:05') ORDER BY time ASC LIMIT 100 OFFSET 100 TOTALS True",
        id="with totals",
    ),
]


@pytest.mark.parametrize("query, translated", metrics_query_to_snql_tests)
def test_metrics_query_to_snql(query: MetricsQuery, translated: str | None) -> None:
    query.validate()
    assert query.serialize() == translated


invalid_metrics_query_to_snql_tests = [
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
    pytest.param(
        MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    "transaction.duration",
                    "d:transactions/duration@millisecond",
                    123,
                    "metrics_sets",
                ),
                aggregate="max",
            ),
            start=None,
            end=NOW + timedelta(days=14),
            rollup=Rollup(interval=3600, totals=None, granularity=3600),
            scope=MetricsScope(
                org_ids=[1], project_ids=[11], use_case_id="transactions"
            ),
        ),
        InvalidMetricsQueryError("start is required for a metrics query"),
        id="bad start",
    ),
    pytest.param(
        MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    "transaction.duration",
                    "d:transactions/duration@millisecond",
                    123,
                    "metrics_sets",
                ),
                aggregate="max",
            ),
            start="today",  # type: ignore
            end=NOW + timedelta(days=14),
            rollup=Rollup(interval=3600, totals=None, granularity=3600),
            scope=MetricsScope(
                org_ids=[1], project_ids=[11], use_case_id="transactions"
            ),
        ),
        InvalidMetricsQueryError("start must be a datetime"),
        id="bad start type",
    ),
    pytest.param(
        MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    "transaction.duration",
                    "d:transactions/duration@millisecond",
                    123,
                    "metrics_sets",
                ),
                aggregate="max",
            ),
            start=NOW,
            end=None,
            rollup=Rollup(interval=3600, totals=None, granularity=3600),
            scope=MetricsScope(
                org_ids=[1], project_ids=[11], use_case_id="transactions"
            ),
        ),
        InvalidMetricsQueryError("end is required for a metrics query"),
        id="bad end",
    ),
    pytest.param(
        MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    "transaction.duration",
                    "d:transactions/duration@millisecond",
                    123,
                    "metrics_sets",
                ),
                aggregate="max",
            ),
            start=NOW,
            end="today",  # type: ignore
            rollup=Rollup(interval=3600, totals=None, granularity=3600),
            scope=MetricsScope(
                org_ids=[1], project_ids=[11], use_case_id="transactions"
            ),
        ),
        InvalidMetricsQueryError("end must be a datetime"),
        id="bad end type",
    ),
    pytest.param(
        MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    "transaction.duration",
                    "d:transactions/duration@millisecond",
                    123,
                    "metrics_sets",
                ),
                aggregate="max",
            ),
            start=NOW,
            end=NOW + timedelta(days=14),
            rollup=None,
            scope=MetricsScope(
                org_ids=[1], project_ids=[11], use_case_id="transactions"
            ),
        ),
        InvalidMetricsQueryError("rollup is required for a metrics query"),
        id="bad rollup",
    ),
    pytest.param(
        MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    "transaction.duration",
                    "d:transactions/duration@millisecond",
                    123,
                    "metrics_sets",
                ),
                aggregate="max",
            ),
            start=NOW,
            end=NOW + timedelta(days=14),
            rollup=6,  # type: ignore
            scope=MetricsScope(
                org_ids=[1], project_ids=[11], use_case_id="transactions"
            ),
        ),
        InvalidMetricsQueryError("rollup must be a Rollup object"),
        id="bad rollup type",
    ),
    pytest.param(
        MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    "transaction.duration",
                    "d:transactions/duration@millisecond",
                    123,
                    "metrics_sets",
                ),
                aggregate="max",
            ),
            start=NOW,
            end=NOW + timedelta(days=14),
            rollup=Rollup(interval=3600, totals=None, granularity=3600),
            scope=None,
        ),
        InvalidMetricsQueryError("scope is required for a metrics query"),
        id="bad scope",
    ),
    pytest.param(
        MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    "transaction.duration",
                    "d:transactions/duration@millisecond",
                    123,
                    "metrics_sets",
                ),
                aggregate="max",
            ),
            start=NOW,
            end=NOW + timedelta(days=14),
            rollup=Rollup(interval=3600, totals=None, granularity=3600),
            scope=6,  # type: ignore
        ),
        InvalidMetricsQueryError("scope must be a MetricsScope object"),
        id="bad scope type",
    ),
    pytest.param(
        MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    "transaction.duration",
                    "d:transactions/duration@millisecond",
                    123,
                    "metrics_sets",
                ),
                aggregate="quantiles",
                aggregate_params=[0.5, 0.99],
                groupby=[AliasedExpression(Column("tags[transaction]"), "transaction")],
            ),
            start=NOW,
            end=NOW + timedelta(days=14),
            rollup=Rollup(interval=60, totals=True),
            scope=MetricsScope(
                org_ids=[1],
                project_ids=[11],
                use_case_id="transactions",
            ),
        ),
        InvalidMetricsQueryError("granularity must be set on the rollup"),
        id="granularity must be present",
    ),
]


@pytest.mark.parametrize("query, exception", invalid_metrics_query_to_snql_tests)
def test_invalid_metrics_query_to_snql_tests(
    query: MetricsQuery, exception: Exception
) -> None:
    with pytest.raises(type(exception), match=re.escape(str(exception))):
        query.validate()


metrics_query_to_mql_tests = [
    pytest.param(
        MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    mri="d:transactions/duration@millisecond",
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
        ),
        {
            "mql": "max(d:transactions/duration@millisecond)",
            "mql_context": {
                "start": "2023-01-02T03:04:05+00:00",
                "end": "2023-01-16T03:04:05+00:00",
                "rollup": {
                    "orderby": {"column_name": "time", "direction": "ASC"},
                    "granularity": "3600",
                    "interval": "3600",
                    "with_totals": "",
                },
                "scope": {
                    "org_ids": [1],
                    "project_ids": [11],
                    "use_case_id": "transactions",
                },
                "limit": "",
                "offset": "",
                "indexer_mappings": {},
            },
        },
        id="basic mri query",
    ),
    pytest.param(
        MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    public_name="transactions.duration",
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
        ),
        {
            "mql": "max(transactions.duration)",
            "mql_context": {
                "start": "2023-01-02T03:04:05+00:00",
                "end": "2023-01-16T03:04:05+00:00",
                "rollup": {
                    "orderby": {"column_name": "time", "direction": "ASC"},
                    "granularity": "3600",
                    "interval": "3600",
                    "with_totals": "",
                },
                "scope": {
                    "org_ids": [1],
                    "project_ids": [11],
                    "use_case_id": "transactions",
                },
                "limit": "",
                "offset": "",
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
        ),
        {
            "mql": "max(d:transactions/duration@millisecond){bar:'baz'}",
            "mql_context": {
                "start": "2023-01-02T03:04:05+00:00",
                "end": "2023-01-16T03:04:05+00:00",
                "rollup": {
                    "orderby": {"column_name": "time", "direction": "ASC"},
                    "granularity": "3600",
                    "interval": "3600",
                    "with_totals": "",
                },
                "scope": {
                    "org_ids": [1],
                    "project_ids": [11],
                    "use_case_id": "transactions",
                },
                "limit": "",
                "offset": "",
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
        ),
        {
            "mql": "max(d:transactions/duration@millisecond){bar:['baz', 'bap']}",
            "mql_context": {
                "start": "2023-01-02T03:04:05+00:00",
                "end": "2023-01-16T03:04:05+00:00",
                "rollup": {
                    "orderby": {"column_name": "time", "direction": "ASC"},
                    "granularity": "3600",
                    "interval": "3600",
                    "with_totals": "",
                },
                "scope": {
                    "org_ids": [1],
                    "project_ids": [11],
                    "use_case_id": "transactions",
                },
                "limit": "",
                "offset": "",
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
                ),
                aggregate="max",
                aggregate_params=None,
                filters=[
                    Condition(Column("bar"), Op.EQ, "baz"),
                    Condition(Column("foo"), Op.EQ, "foz"),
                ],
                groupby=None,
            ),
            start=NOW,
            end=NOW + timedelta(days=14),
            rollup=Rollup(interval=3600, totals=None, granularity=3600),
            scope=MetricsScope(
                org_ids=[1], project_ids=[11], use_case_id="transactions"
            ),
        ),
        {
            "mql": "max(d:transactions/duration@millisecond){bar:'baz', foo:'foz'}",
            "mql_context": {
                "start": "2023-01-02T03:04:05+00:00",
                "end": "2023-01-16T03:04:05+00:00",
                "rollup": {
                    "orderby": {"column_name": "time", "direction": "ASC"},
                    "granularity": "3600",
                    "interval": "3600",
                    "with_totals": "",
                },
                "scope": {
                    "org_ids": [1],
                    "project_ids": [11],
                    "use_case_id": "transactions",
                },
                "limit": "",
                "offset": "",
                "indexer_mappings": {},
            },
        },
        id="multiple filters query",
    ),
    pytest.param(
        MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    mri="d:transactions/duration@millisecond",
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
        ),
        {
            "mql": "max(d:transactions/duration@millisecond) by (transaction)",
            "mql_context": {
                "start": "2023-01-02T03:04:05+00:00",
                "end": "2023-01-16T03:04:05+00:00",
                "rollup": {
                    "orderby": {"column_name": "time", "direction": "ASC"},
                    "granularity": "3600",
                    "interval": "3600",
                    "with_totals": "",
                },
                "scope": {
                    "org_ids": [1],
                    "project_ids": [11],
                    "use_case_id": "transactions",
                },
                "limit": "",
                "offset": "",
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
        ),
        {
            "mql": "max(d:transactions/duration@millisecond) by (a, b)",
            "mql_context": {
                "start": "2023-01-02T03:04:05+00:00",
                "end": "2023-01-16T03:04:05+00:00",
                "rollup": {
                    "orderby": {"column_name": "time", "direction": "ASC"},
                    "granularity": "3600",
                    "interval": "3600",
                    "with_totals": "",
                },
                "scope": {
                    "org_ids": [1],
                    "project_ids": [11],
                    "use_case_id": "transactions",
                },
                "limit": "",
                "offset": "",
                "indexer_mappings": {},
            },
        },
        id="multiple groupby query",
    ),
    pytest.param(
        MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    mri="d:transactions/duration@millisecond",
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
        ),
        {
            "mql": "max(d:transactions/duration@millisecond){bar:'baz'} by (transaction)",
            "mql_context": {
                "start": "2023-01-02T03:04:05+00:00",
                "end": "2023-01-16T03:04:05+00:00",
                "rollup": {
                    "orderby": {"column_name": "time", "direction": "ASC"},
                    "granularity": "3600",
                    "interval": "3600",
                    "with_totals": "",
                },
                "scope": {
                    "org_ids": [1],
                    "project_ids": [11],
                    "use_case_id": "transactions",
                },
                "limit": "",
                "offset": "",
                "indexer_mappings": {},
            },
        },
        id="complex single timeseries query",
    ),
]


@pytest.mark.parametrize("query, translated", metrics_query_to_mql_tests)
def test_metrics_query_to_mql(query: MetricsQuery, translated: dict[str, Any]) -> None:
    query.validate()
    assert query.serialize_to_mql()["mql"] == translated["mql"]
    assert query.serialize_to_mql()["mql_context"] == translated["mql_context"]


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

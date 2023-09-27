from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

import pytest

from snuba_sdk import Column, Condition, Op
from snuba_sdk.metrics_query import MetricsQuery
from snuba_sdk.metrics_query_visitors import InvalidMetricsQueryError
from snuba_sdk.timeseries import Metric, MetricsScope, Rollup, Timeseries

NOW = datetime(2023, 1, 2, 3, 4, 5, 0, timezone.utc)
tests = [
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
            filters=None,
            groupby=None,
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
                filters=None,
                groupby=None,
            ),
            filters=[Condition(Column("tags[transaction]"), Op.EQ, "foo")],
            groupby=[Column("tags[status_code]")],
            start=NOW,
            end=NOW + timedelta(days=14),
            rollup=Rollup(interval=3600, totals=None, granularity=3600),
            scope=MetricsScope(
                org_ids=[1], project_ids=[11], use_case_id="transactions"
            ),
        ),
        "MATCH (metrics_sets) SELECT max(value) AS `aggregate_value` BY toStartOfInterval(timestamp, toIntervalSecond(3600), 'Universal') AS `time` WHERE granularity = 3600 AND metric_id = 123 AND tags[transaction] = 'foo' AND (org_id IN array(1) AND project_id IN array(11) AND use_case_id = 'transactions') AND timestamp >= toDateTime('2023-01-02T03:04:05') AND timestamp < toDateTime('2023-01-16T03:04:05') ORDER BY time ASC",
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
                filters=[Condition(Column("tags[referrer]"), Op.EQ, "foo")],
                groupby=[Column("tags[environment]")],
            ),
            filters=[Condition(Column("tags[transaction]"), Op.EQ, "foo")],
            groupby=[Column("tags[status_code]")],
            start=NOW,
            end=NOW + timedelta(days=14),
            rollup=Rollup(interval=3600, totals=None, granularity=3600),
            scope=MetricsScope(
                org_ids=[1], project_ids=[11], use_case_id="transactions"
            ),
        ),
        "MATCH (metrics_sets) SELECT max(value) AS `aggregate_value` BY toStartOfInterval(timestamp, toIntervalSecond(3600), 'Universal') AS `time`, tags[environment] WHERE granularity = 3600 AND metric_id = 123 AND tags[referrer] = 'foo' AND tags[transaction] = 'foo' AND (org_id IN array(1) AND project_id IN array(11) AND use_case_id = 'transactions') AND timestamp >= toDateTime('2023-01-02T03:04:05') AND timestamp < toDateTime('2023-01-16T03:04:05') ORDER BY time ASC",
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
                filters=[Condition(Column("tags[referrer]"), Op.EQ, "foo")],
                groupby=[Column("tags[environment]")],
            ),
            filters=[Condition(Column("tags[transaction]"), Op.EQ, "foo")],
            groupby=[Column("tags[status_code]")],
            start=NOW,
            end=NOW + timedelta(days=14),
            rollup=Rollup(totals=True, granularity=60),
            scope=MetricsScope(
                org_ids=[1], project_ids=[11], use_case_id="transactions"
            ),
        ),
        "MATCH (metrics_sets) SELECT max(value) AS `aggregate_value` BY tags[environment] WHERE granularity = 60 AND metric_id = 123 AND tags[referrer] = 'foo' AND tags[transaction] = 'foo' AND (org_id IN array(1) AND project_id IN array(11) AND use_case_id = 'transactions') AND timestamp >= toDateTime('2023-01-02T03:04:05') AND timestamp < toDateTime('2023-01-16T03:04:05')",
        id="totals query",
    ),
]


@pytest.mark.parametrize("query, translated", tests)
def test_query(query: MetricsQuery, translated: str | None) -> None:
    query.validate()
    assert query.serialize() == translated


invalid_tests = [
    pytest.param(
        MetricsQuery(
            query=None,
            filters=None,
            groupby=None,
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
            filters=[1],  # type: ignore
            groupby=None,
            start=NOW,
            end=NOW + timedelta(days=14),
            rollup=Rollup(interval=3600, totals=None, granularity=3600),
            scope=MetricsScope(
                org_ids=[1], project_ids=[11], use_case_id="transactions"
            ),
        ),
        InvalidMetricsQueryError("filters must be a list of Conditions"),
        id="bad filters",
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
            filters=None,
            groupby=[1],  # type: ignore
            start=NOW,
            end=NOW + timedelta(days=14),
            rollup=Rollup(interval=3600, totals=None, granularity=3600),
            scope=MetricsScope(
                org_ids=[1], project_ids=[11], use_case_id="transactions"
            ),
        ),
        InvalidMetricsQueryError("groupby must be a list of Columns"),
        id="bad groupby",
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
            filters=None,
            groupby=None,
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
            filters=None,
            groupby=None,
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
            filters=None,
            groupby=None,
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
            filters=None,
            groupby=None,
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
            filters=None,
            groupby=None,
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
            filters=None,
            groupby=None,
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
            filters=None,
            groupby=None,
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
            filters=None,
            groupby=None,
            start=NOW,
            end=NOW + timedelta(days=14),
            rollup=Rollup(interval=3600, totals=None, granularity=3600),
            scope=6,  # type: ignore
        ),
        InvalidMetricsQueryError("scope must be a MetricsScope object"),
        id="bad scope type",
    ),
]


@pytest.mark.parametrize("query, exception", invalid_tests)
def test_invalid_query(query: MetricsQuery, exception: Exception) -> None:
    with pytest.raises(type(exception), match=re.escape(str(exception))):
        query.validate()

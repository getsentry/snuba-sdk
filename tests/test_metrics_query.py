import re
from datetime import datetime, timedelta, timezone

import pytest

from snuba_sdk import Column, Condition, Op
from snuba_sdk.metrics_query import MetricScope, MetricsQuery, Rollup
from snuba_sdk.query_visitors import InvalidQueryError
from snuba_sdk.timeseries import Metric, Timeseries

NOW = datetime(2023, 1, 2, 3, 4, 5, 6, timezone.utc)
tests = [
    pytest.param(
        MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    "transaction.duration", "d:transactions/duration@millisecond", 123
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
            rollup=Rollup(interval=3600, totals=None),
            scope=MetricScope(org_ids=[1], project_ids=[11], use_case_id=111),
        ),
        id="basic query",
    ),
    pytest.param(
        MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    "transaction.duration", "d:transactions/duration@millisecond", 123
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
            rollup=Rollup(interval=3600, totals=None),
        ),
        id="top level filters/group by",
    ),
    pytest.param(
        MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    "transaction.duration", "d:transactions/duration@millisecond", 123
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
            rollup=Rollup(interval=3600, totals=None),
        ),
        id="top level filters/group by with low level filters",
    ),
    pytest.param(
        MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    "transaction.duration", "d:transactions/duration@millisecond", 123
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
            rollup=Rollup(interval=None, totals=True),
        ),
        id="totals query",
    ),
]


@pytest.mark.parametrize("query", tests)
def test_query(query: MetricsQuery) -> None:
    with pytest.raises(
        InvalidQueryError
    ):  # TODO: This should be removed once validation is implemented
        query.validate()


invalid_tests = [
    pytest.param(
        MetricsQuery(
            query=None,
            filters=None,
            groupby=None,
            start=NOW,
            end=NOW + timedelta(days=14),
            rollup=Rollup(interval=3600, totals=None),
        ),
        InvalidQueryError("Not implemented"),
        id="missing query",
    ),
]


@pytest.mark.parametrize("query, exception", invalid_tests)
def test_invalid_query(query: MetricsQuery, exception: Exception) -> None:
    with pytest.raises(type(exception), match=re.escape(str(exception))):
        query.validate()

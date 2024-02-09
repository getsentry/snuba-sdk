from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from typing import Mapping

import pytest

from snuba_sdk.column import Column
from snuba_sdk.conditions import Condition, Op
from snuba_sdk.entity import Entity
from snuba_sdk.metrics_query import MetricsQuery
from snuba_sdk.query import Query
from snuba_sdk.query_visitors import InvalidQueryError
from snuba_sdk.request import Flags, InvalidFlagError, InvalidRequestError, Request
from snuba_sdk.timeseries import Metric, MetricsScope, Rollup, Timeseries

NOW = datetime(2021, 1, 2, 3, 4, 5, 6, timezone.utc)
BASIC_QUERY = (
    Query(Entity("events"))
    .set_select([Column("event_id"), Column("title")])
    .set_where([Condition(Column("timestamp"), Op.GT, NOW)])
)
BASIC_METRICS_QUERY = MetricsQuery(
    query=Timeseries(
        metric=Metric(
            mri="d:transactions/duration@millisecond",
            id=11235813,
        ),
        aggregate="max",
        aggregate_params=None,
        filters=[Condition(Column("bar"), Op.EQ, "baz")],
        groupby=[Column("transaction")],
    ),
    start=NOW,
    end=NOW + timedelta(days=14),
    rollup=Rollup(interval=3600, totals=None, granularity=3600),
    scope=MetricsScope(org_ids=[1], project_ids=[11], use_case_id="transactions"),
    indexer_mappings={},
)


tests = [
    pytest.param(
        "events",
        "default",
        {"organization_id": 1, "referrer": "default"},
        BASIC_QUERY,
        Flags(consistent=True, turbo=True, dry_run=True, legacy=True, debug=True),
        "s/g",
        {
            "query": "MATCH (events) SELECT event_id, title WHERE timestamp > toDateTime('2021-01-02T03:04:05.000006')",
            "consistent": True,
            "turbo": True,
            "debug": True,
            "dry_run": True,
            "dataset": "events",
            "app_id": "default",
            "tenant_ids": {"organization_id": 1, "referrer": "default"},
            "parent_api": "s/g",
            "legacy": True,
        },
        None,
        id="flags",
    ),
    pytest.param(
        "events",
        "default",
        {"organization_id": 1, "referrer": "default"},
        BASIC_METRICS_QUERY,
        Flags(consistent=True, turbo=True, dry_run=True, legacy=True, debug=True),
        "s/g",
        {
            "query": 'max(d:transactions/duration@millisecond){bar:"baz"} by (transaction)',
            "mql_context": {
                "start": "2021-01-02T03:04:05.000006+00:00",
                "end": "2021-01-16T03:04:05.000006+00:00",
                "indexer_mappings": {},
                "limit": None,
                "offset": None,
                "rollup": {
                    "granularity": 3600,
                    "interval": 3600,
                    "orderby": None,
                    "with_totals": None,
                },
                "scope": {
                    "org_ids": [1],
                    "project_ids": [11],
                    "use_case_id": "transactions",
                },
            },
            "consistent": True,
            "turbo": True,
            "debug": True,
            "dry_run": True,
            "dataset": "events",
            "app_id": "default",
            "tenant_ids": {"organization_id": 1, "referrer": "default"},
            "parent_api": "s/g",
            "legacy": True,
        },
        None,
        id="flags",
    ),
    pytest.param(
        None,
        "default",
        {"organization_id": 1, "referrer": "default"},
        BASIC_QUERY,
        Flags(consistent=True),
        "s/g",
        None,
        InvalidRequestError("Request must have a valid dataset"),
        id="invalid_dataset",
    ),
    pytest.param(
        "@@ff@@",
        "default",
        {"organization_id": 1, "referrer": "default"},
        BASIC_QUERY,
        Flags(consistent=True),
        "s/g",
        None,
        InvalidRequestError("'@@ff@@' is not a valid dataset"),
        id="invalid_dataset_chars",
    ),
    pytest.param(
        "events",
        2,
        {"organization_id": 1, "referrer": "default"},
        BASIC_QUERY,
        Flags(consistent=True),
        "s/g",
        None,
        InvalidRequestError("Request must have a valid app_id"),
        id="invalid_app_id",
    ),
    pytest.param(
        "events",
        "@@ff@@",
        {"organization_id": 1, "referrer": "default"},
        BASIC_QUERY,
        Flags(consistent=True),
        "s/g",
        None,
        InvalidRequestError("'@@ff@@' is not a valid app_id"),
        id="invalid_app_id_chars",
    ),
    pytest.param(
        "events",
        "default",
        {"organization_id": 1, "referrer": "default"},
        BASIC_QUERY,
        Flags(consistent=True),
        6,
        None,
        InvalidRequestError("`6` is not a valid parent_api"),
        id="invalid_parent_api",
    ),
    pytest.param(
        "events",
        "default",
        {"organization_id": 1, "referrer": "default"},
        Query(Entity("events")),
        Flags(consistent=True, turbo=True, dry_run=True, legacy=True, debug=True),
        "s/g",
        None,
        InvalidQueryError("query must have at least one expression in select"),
        id="invalid_query",
    ),
    pytest.param(
        "events",
        "default",
        {"organization_id": 1, "referrer": "default"},
        BASIC_QUERY,
        Flags(consistent=1),  # type: ignore
        "s/g",
        None,
        InvalidFlagError("consistent must be a boolean"),
        id="invalid_flags",
    ),
]


@pytest.mark.parametrize(
    "dataset, app_id, tenant_ids, query, flags, parent_api, expected, exception", tests
)
def test_request(
    dataset: str,
    app_id: str,
    tenant_ids: dict[str, str | int],
    query: Query,
    flags: Flags,
    parent_api: str,
    expected: Mapping[str, str | bool],
    exception: Exception | None,
) -> None:
    if exception:
        with pytest.raises(type(exception), match=re.escape(str(exception))):
            request = Request(dataset, app_id, query, flags, parent_api, tenant_ids)
            request.validate()
    else:
        request = Request(dataset, app_id, query, flags, parent_api, tenant_ids)
        request.validate()
        assert request.to_dict() == expected
        assert request.print() == json.dumps(expected, sort_keys=True, indent=4 * " ")


def test_request_set_methods() -> None:
    request = Request(
        dataset="events",
        app_id="default",
        query=BASIC_QUERY,
        flags=Flags(consistent=True),
    )
    request.flags = Flags(consistent=False)
    request.query = BASIC_QUERY.set_select([Column("trace_id")])
    request.validate()
    assert request.query == BASIC_QUERY.set_select([Column("trace_id")])
    assert request.flags is not None and request.flags.consistent is False

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Mapping

import pytest

from snuba_sdk.column import Column
from snuba_sdk.conditions import Condition, Op
from snuba_sdk.entity import Entity
from snuba_sdk.query import Query
from snuba_sdk.query_visitors import InvalidQueryError
from snuba_sdk.request import Flags, InvalidFlagError, InvalidRequestError, Request

NOW = datetime(2021, 1, 2, 3, 4, 5, 6, timezone.utc)
BASIC_QUERY = (
    Query(Entity("events"))
    .set_select([Column("event_id"), Column("title")])
    .set_where([Condition(Column("timestamp"), Op.GT, NOW)])
)


tests = [
    pytest.param(
        "events",
        "default",
        BASIC_QUERY,
        Flags(consistent=True, turbo=True, dry_run=True, legacy=True, debug=True),
        {
            "query": "MATCH (events) SELECT event_id, title WHERE timestamp > toDateTime('2021-01-02T03:04:05.000006')",
            "consistent": True,
            "turbo": True,
            "debug": True,
            "dry_run": True,
            "dataset": "events",
            "app_id": "default",
            "legacy": True,
        },
        None,
        id="flags",
    ),
    pytest.param(
        None,
        "default",
        BASIC_QUERY,
        Flags(consistent=True),
        None,
        InvalidRequestError("Request must have a valid dataset"),
        id="invalid_dataset",
    ),
    pytest.param(
        "@@ff@@",
        "default",
        BASIC_QUERY,
        Flags(consistent=True),
        None,
        InvalidRequestError("'@@ff@@' is not a valid dataset"),
        id="invalid_dataset_chars",
    ),
    pytest.param(
        "events",
        2,
        BASIC_QUERY,
        Flags(consistent=True),
        None,
        InvalidRequestError("Request must have a valid app_id"),
        id="invalid_dataset",
    ),
    pytest.param(
        "events",
        "@@ff@@",
        BASIC_QUERY,
        Flags(consistent=True),
        None,
        InvalidRequestError("'@@ff@@' is not a valid app_id"),
        id="invalid_dataset_chars",
    ),
    pytest.param(
        "events",
        "default",
        Query(Entity("events")),
        Flags(consistent=True, turbo=True, dry_run=True, legacy=True, debug=True),
        None,
        InvalidQueryError("query must have at least one expression in select"),
        id="invalid_query",
    ),
    pytest.param(
        "events",
        "default",
        BASIC_QUERY,
        Flags(consistent=1),  # type: ignore
        None,
        InvalidFlagError("consistent must be a boolean"),
        id="invalid_flags",
    ),
]


@pytest.mark.parametrize("dataset, app_id, query, flags, expected, exception", tests)
def test_request(
    dataset: str,
    app_id: str,
    query: Query,
    flags: Flags,
    expected: Mapping[str, str | bool],
    exception: Exception | None,
) -> None:
    if exception:
        with pytest.raises(type(exception), match=re.escape(str(exception))):
            request = Request(dataset, app_id, query, flags)
            request.validate()
    else:
        request = Request(dataset, app_id, query, flags)
        request.validate()
        assert request.to_dict() == expected
        assert request.print() == json.dumps(expected, sort_keys=True, indent=4 * " ")


def test_request_set_methods() -> None:
    request = Request("events", "default", BASIC_QUERY, Flags(consistent=True))
    request.flags = Flags(consistent=False)
    request.query = BASIC_QUERY.set_select([Column("trace_id")])
    request.validate()
    assert request.query == BASIC_QUERY.set_select([Column("trace_id")])
    assert request.flags is not None and request.flags.consistent is False

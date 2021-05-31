from typing import Any, Mapping, Sequence

import pytest

from snuba_sdk.legacy import json_to_snql

tests = [
    pytest.param(
        {
            "project_id": 2,
            "dataset": "events",
            "conditions": [
                ["type", "=", "error"],
                [["ifNull", ["tags[level]", "''"]], "=", "error"],
            ],
            "aggregations": [["count", None, "count"]],
            "time_window": 600,
            "resolution": 60,
        },
        (
            "-- DATASET: events",
            "MATCH (events)",
            "SELECT count() AS count",
            (
                "WHERE project_id IN tuple(2) "
                "AND type = 'error' "
                "AND ifNull(tags[level], '') = 'error'"
            ),
        ),
        "events",
        id="basic_subscription",
    ),
    pytest.param(
        {
            "project_id": 2,
            "dataset": "events",
            "conditions": [
                ["type", "=", "error"],
                [["positionCaseInsensitive", ["message", "'hello'"]], "!=", 0],
                ["environment", "=", "development"],
            ],
            "aggregations": [
                ["uniq", "tags[sentry:user]", "count_unique_tags_sentry_user"]
            ],
            "time_window": 6000,
            "resolution": 120,
        },
        (
            "-- DATASET: events",
            "MATCH (events)",
            "SELECT uniq(tags[sentry:user]) AS count_unique_tags_sentry_user",
            (
                "WHERE project_id IN tuple(2) "
                "AND type = 'error' "
                "AND positionCaseInsensitive(message, 'hello') != 0 "
                "AND environment = 'development'"
            ),
        ),
        "events",
        id="subscription_complex_conditions_aggregate",
    ),
    pytest.param(
        {
            "project_id": 2,
            "dataset": "events",
            "conditions": [
                ["type", "=", "error"],
                [["ifNull", ["tags[level]", "''"]], "=", "error"],
                ["environment", "=", "development"],
            ],
            "aggregations": [["count", None, "count"]],
            "time_window": 600,
            "resolution": 60,
        },
        (
            "-- DATASET: events",
            "MATCH (events)",
            "SELECT count() AS count",
            (
                "WHERE project_id IN tuple(2) "
                "AND type = 'error' "
                "AND ifNull(tags[level], '') = 'error' "
                "AND environment = 'development'"
            ),
        ),
        "events",
        id="basic_subscription_with_extra_conditions",
    ),
]


@pytest.mark.parametrize("json_body, clauses, entity", tests)
def test_discover_json_to_snuba(
    json_body: Mapping[str, Any], clauses: Sequence[str], entity: str
) -> None:
    expected = "\n".join(clauses)
    query = json_to_snql(json_body, entity, skip_time=True)
    query.validate()
    assert query.print() == expected

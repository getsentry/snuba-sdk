import pytest
from typing import Any, Mapping, Sequence

from snuba_sdk.legacy import json_to_snql


tests = [
    pytest.param(
        {
            "selected_columns": ["project_id", "release"],
            "orderby": "sessions",
            "offset": 0,
            "limit": 100,
            "limitby": [11, "release"],
            "project": [2],
            "organization": (2,),
            "dataset": "sessions",
            "from_date": "2020-10-17T20:51:46.110774",
            "to_date": "2021-01-15T20:51:47.110825",
            "groupby": ["release", "project_id"],
            "conditions": [
                ["project_id", "IN", [2]],
                ["array_stuff", "IN", [[2]]],
                ["tuple_stuff", "IN", ((2,),)],
                ["bucketed_started", ">", "2020-10-17T20:51:46.110774"],
            ],
            "having": [["min_users", ">", 10]],
            "aggregations": [["min", [["max", ["users"], "max_users"]], "min_users"]],
            "consistent": True,
            "granularity": 86400,
            "totals": True,
            "sample": 0.1,
            "debug": True,
        },
        (
            "-- DATASET: sessions",
            "-- CONSISTENT: True",
            "-- DEBUG: True",
            "MATCH (sessions SAMPLE 0.100000)",
            "SELECT project_id, release, min(max(users) AS max_users) AS min_users",
            "BY release, project_id",
            (
                "WHERE project_id IN tuple(2) "
                "AND array_stuff IN tuple(tuple(2)) "
                "AND tuple_stuff IN tuple(tuple(2)) "
                "AND bucketed_started > toDateTime('2020-10-17T20:51:46.110774') "
                "AND project_id IN array(2) "
                "AND org_id IN tuple(2) "
                "AND started > toDateTime('2020-10-17T20:51:46.110774') "
                "AND started <= toDateTime('2021-01-15T20:51:47.110825')"
            ),
            "HAVING min_users > 10",
            "ORDER BY sessions ASC",
            "LIMIT 11 BY release",
            "LIMIT 100",
            "OFFSET 0",
            "GRANULARITY 86400",
            "TOTALS True",
        ),
        id="cover as many cases as possible",
    ),
    pytest.param(
        {
            "selected_columns": ["project_id", "release"],
            "orderby": ["sessions", "-project_id"],
            "offset": 0,
            "limit": 100,
            "limitby": [11, "release"],
            "project": [2],
            "organization": (2,),
            "dataset": "sessions",
            "from_date": "2020-10-17T20:51:46.110774",
            "to_date": "2021-01-15T20:51:47.110825",
            "groupby": ["release", "project_id"],
            "conditions": [
                ["project_id", "IN", [2]],
                ["array_stuff", "IN", [[2]]],
                ["tuple_stuff", "IN", ((2,),)],
                ["bucketed_started", ">", "2020-10-17T20:51:46.110774"],
            ],
            "having": [["min_users", ">", 10]],
            "aggregations": [["min", [["max", ["users"], "max_users"]], "min_users"]],
            "consistent": True,
            "sample": 1000,
        },
        (
            "-- DATASET: sessions",
            "-- CONSISTENT: True",
            "MATCH (sessions SAMPLE 1000)",
            "SELECT project_id, release, min(max(users) AS max_users) AS min_users",
            "BY release, project_id",
            (
                "WHERE project_id IN tuple(2) "
                "AND array_stuff IN tuple(tuple(2)) "
                "AND tuple_stuff IN tuple(tuple(2)) "
                "AND bucketed_started > toDateTime('2020-10-17T20:51:46.110774') "
                "AND project_id IN array(2) "
                "AND org_id IN tuple(2) "
                "AND started > toDateTime('2020-10-17T20:51:46.110774') "
                "AND started <= toDateTime('2021-01-15T20:51:47.110825')"
            ),
            "HAVING min_users > 10",
            "ORDER BY sessions ASC, project_id DESC",
            "LIMIT 11 BY release",
            "LIMIT 100",
            "OFFSET 0",
        ),
        id="multiple order by",
    ),
    pytest.param(
        {
            "selected_columns": ["project_id", "release"],
            "orderby": [["divide", ["sessions_crashed", "sessions"]]],
            "offset": 0,
            "limit": 100,
            "limitby": [11, "release"],
            "project": [2],
            "organization": (2,),
            "dataset": "sessions",
            "from_date": "2020-10-17T20:51:46.110774",
            "to_date": "2021-01-15T20:51:47.110825",
            "groupby": ["release", "project_id"],
            "conditions": [
                ["project_id", "IN", [2]],
                ["array_stuff", "IN", [[2]]],
                ["tuple_stuff", "IN", ((2,),)],
                ["bucketed_started", ">", "2020-10-17T20:51:46.110774"],
            ],
            "having": [["min_users", ">", 10]],
            "aggregations": [["min", [["max", ["users"], "max_users"]], "min_users"]],
            "consistent": False,
            "turbo": True,
        },
        (
            "-- DATASET: sessions",
            "-- TURBO: True",
            "MATCH (sessions)",
            "SELECT project_id, release, min(max(users) AS max_users) AS min_users",
            "BY release, project_id",
            (
                "WHERE project_id IN tuple(2) "
                "AND array_stuff IN tuple(tuple(2)) "
                "AND tuple_stuff IN tuple(tuple(2)) "
                "AND bucketed_started > toDateTime('2020-10-17T20:51:46.110774') "
                "AND project_id IN array(2) "
                "AND org_id IN tuple(2) "
                "AND started > toDateTime('2020-10-17T20:51:46.110774') "
                "AND started <= toDateTime('2021-01-15T20:51:47.110825')"
            ),
            "HAVING min_users > 10",
            "ORDER BY divide(sessions_crashed, sessions) ASC",
            "LIMIT 11 BY release",
            "LIMIT 100",
            "OFFSET 0",
        ),
        id="function order by",
    ),
    pytest.param(
        {
            "selected_columns": ["project_id", "release", "array_stuff"],
            "orderby": ["sessions", "-project_id"],
            "offset": 0,
            "limit": 100,
            "limitby": [11, "release"],
            "project": [2],
            "organization": (2,),
            "dataset": "sessions",
            "from_date": "2020-10-17T20:51:46.110774",
            "to_date": "2021-01-15T20:51:47.110825",
            "conditions": [
                ["project_id", "IN", [2]],
                ["array_stuff", "IN", [[2]]],
                ["tuple_stuff", "IN", ((2,),)],
                ["bucketed_started", ">", "2020-10-17T20:51:46.110774"],
            ],
            "having": [["min_users", ">", 10]],
            "aggregations": [],
            "consistent": False,
            "arrayjoin": "array_stuff",
        },
        (
            "-- DATASET: sessions",
            "MATCH (sessions)",
            "SELECT project_id, release, array_stuff, arrayJoin(array_stuff) AS array_stuff",
            (
                "WHERE project_id IN tuple(2) "
                "AND array_stuff IN tuple(tuple(2)) "
                "AND tuple_stuff IN tuple(tuple(2)) "
                "AND bucketed_started > toDateTime('2020-10-17T20:51:46.110774') "
                "AND project_id IN array(2) "
                "AND org_id IN tuple(2) "
                "AND started > toDateTime('2020-10-17T20:51:46.110774') "
                "AND started <= toDateTime('2021-01-15T20:51:47.110825')"
            ),
            "HAVING min_users > 10",
            "ORDER BY sessions ASC, project_id DESC",
            "LIMIT 11 BY release",
            "LIMIT 100",
            "OFFSET 0",
        ),
        id="arrayjoin",
    ),
    pytest.param(
        {
            "selected_columns": ["project_id", "release"],
            "orderby": ["sessions", "-project_id"],
            "offset": 0,
            "limit": 100,
            "limitby": [11, "release"],
            "project": [2],
            "organization": (2,),
            "dataset": "sessions",
            "from_date": "2020-10-17T20:51:46.110774",
            "to_date": "2021-01-15T20:51:47.110825",
            "conditions": [
                ["project_id", "IN", [2]],
                ["array_stuff", "IN", [[2]]],
                ["tuple_stuff", "IN", ((2,),)],
                ["bucketed_started", ">", "2020-10-17T20:51:46.110774"],
            ],
            "having": [["min_users", ">", 10]],
            "aggregations": [
                ["apdex(duration, 300)", None, "apdex"],
                ["uniqIf(user, greater(duration, 1200))", None, "misery"],
                ["quantile(0.75)", "duration", "p75"],
            ],
            "consistent": False,
        },
        (
            "-- DATASET: sessions",
            "MATCH (sessions)",
            "SELECT project_id, release, apdex(duration, 300) AS apdex, uniqIf(user, greater(duration, 1200)) AS misery, quantile(0.75)(duration) AS p75",
            (
                "WHERE project_id IN tuple(2) "
                "AND array_stuff IN tuple(tuple(2)) "
                "AND tuple_stuff IN tuple(tuple(2)) "
                "AND bucketed_started > toDateTime('2020-10-17T20:51:46.110774') "
                "AND project_id IN array(2) "
                "AND org_id IN tuple(2) "
                "AND started > toDateTime('2020-10-17T20:51:46.110774') "
                "AND started <= toDateTime('2021-01-15T20:51:47.110825')"
            ),
            "HAVING min_users > 10",
            "ORDER BY sessions ASC, project_id DESC",
            "LIMIT 11 BY release",
            "LIMIT 100",
            "OFFSET 0",
        ),
        id="curried/string functions",
    ),
]


@pytest.mark.parametrize("json_body, clauses", tests)
def test_json_to_snuba(json_body: Mapping[str, Any], clauses: Sequence[str]) -> None:
    expected = "\n".join(clauses)
    query = json_to_snql(json_body, "sessions")
    assert query.print() == expected


# These are all from the Snuba Discover tests
discover_tests = [
    pytest.param(
        {
            "dataset": "discover",
            "project": 2,
            "selected_columns": [
                ["arrayJoin", ["exception_stacks.type"], "exception_stacks.type"]
            ],
            "aggregations": [["count", None, "count"]],
            "groupby": "exception_stacks.type",
            "debug": True,
            "conditions": [
                [
                    [
                        "or",
                        [
                            [
                                "equals",
                                ["exception_stacks.type", "'ArithmeticException'"],
                            ],
                            ["equals", ["exception_stacks.type", "'RuntimeException'"]],
                        ],
                    ],
                    "=",
                    1,
                ]
            ],
            "limit": 1000,
            "from_date": "2021-03-03T11:22:00+00:00",
            "to_date": "2021-03-03T17:22:00+00:00",
        },
        (
            "-- DATASET: discover",
            "-- DEBUG: True",
            "MATCH (discover_events)",
            "SELECT arrayJoin(exception_stacks.type) AS exception_stacks.type, count AS count",
            "BY exception_stacks.type",
            (
                "WHERE or(equals(exception_stacks.type, 'ArithmeticException'), equals(exception_stacks.type, 'RuntimeException')) = 1 "
                "AND project_id = 2 "
                "AND started > toDateTime('2021-03-03T11:22:00') "
                "AND started <= toDateTime('2021-03-03T17:22:00')"
            ),
            "LIMIT 1000",
        ),
        "discover_events",
        id="arrayjoin in the groupby",
    ),
    pytest.param(
        {
            "dataset": "discover",
            "project": 2,
            "selected_columns": [
                ["arrayJoin", ["measurements.key"], "array_join_measurements_key"],
                [
                    "plus",
                    [
                        [
                            "multiply",
                            [
                                [
                                    "floor",
                                    [
                                        [
                                            "divide",
                                            [
                                                [
                                                    "minus",
                                                    [
                                                        [
                                                            "multiply",
                                                            [
                                                                [
                                                                    "arrayJoin",
                                                                    [
                                                                        "measurements.value"
                                                                    ],
                                                                ],
                                                                100.0,
                                                            ],
                                                        ],
                                                        0.0,
                                                    ],
                                                ],
                                                1.0,
                                            ],
                                        ]
                                    ],
                                ],
                                1.0,
                            ],
                        ],
                        0.0,
                    ],
                    "measurements_histogram_1_0_100",
                ],
            ],
            "aggregations": [["count", None, "count"]],
            "conditions": [
                ["type", "=", "transaction"],
                ["transaction_op", "=", "pageload"],
                ["transaction", "=", "/organizations/:orgId/issues/"],
                ["array_join_measurements_key", "IN", ["cls"]],
                ["measurements_histogram_1_0_100", ">=", 0],
                ["project_id", "IN", [1]],
            ],
            "orderby": [
                "measurements_histogram_1_0_100",
                "array_join_measurements_key",
            ],
            "having": [],
            "groupby": [
                "array_join_measurements_key",
                "measurements_histogram_1_0_100",
            ],
            "limit": 1,
            "from_date": "2021-03-03T11:22:00+00:00",
            "to_date": "2021-03-03T17:22:00+00:00",
        },
        (
            "-- DATASET: discover",
            "MATCH (discover_transactions)",
            "SELECT arrayJoin(measurements.key) AS array_join_measurements_key, plus(multiply(floor(divide(minus(multiply(arrayJoin(measurements.value), 100.0), 0.0), 1.0)), 1.0), 0.0) AS measurements_histogram_1_0_100, count AS count",
            "BY array_join_measurements_key, measurements_histogram_1_0_100",
            (
                "WHERE type = 'transaction' "
                "AND transaction_op = 'pageload' "
                "AND transaction = '/organizations/:orgId/issues/' "
                "AND array_join_measurements_key IN tuple('cls') "
                "AND measurements_histogram_1_0_100 >= 0 "
                "AND project_id IN tuple(1) "
                "AND project_id = 2 "
                "AND started > toDateTime('2021-03-03T11:22:00') "
                "AND started <= toDateTime('2021-03-03T17:22:00')"
            ),
            "ORDER BY measurements_histogram_1_0_100 ASC, array_join_measurements_key ASC",
            "LIMIT 1",
        ),
        "discover_transactions",
        id="array join alias in groupby",
    ),
]


@pytest.mark.parametrize("json_body, clauses, entity", discover_tests)
def test_discover_json_to_snuba(
    json_body: Mapping[str, Any], clauses: Sequence[str], entity: str
) -> None:
    expected = "\n".join(clauses)
    query = json_to_snql(json_body, entity)
    assert query.print() == expected


# These are all taken verbatim from the sentry sessions tests
sentry_tests = [
    pytest.param(
        {
            "selected_columns": ["release", "project_id", "users", "sessions"],
            "project": [2],
            "organization": 2,
            "dataset": "sessions",
            "from_date": "2020-10-17T20:51:46.110774",
            "to_date": "2021-01-15T20:51:47.110825",
            "groupby": ["release", "project_id"],
            "conditions": [
                ["release", "IN", ["foo@1.0.0", "foo@2.0.0"]],
                ["project_id", "IN", [2]],
            ],
            "aggregations": [],
            "consistent": False,
        },
        (
            "-- DATASET: sessions",
            "MATCH (sessions)",
            "SELECT release, project_id, users, sessions",
            "BY release, project_id",
            (
                "WHERE release IN tuple('foo@1.0.0', 'foo@2.0.0') "
                "AND project_id IN tuple(2) "
                "AND project_id IN array(2) "
                "AND org_id = 2 "
                "AND started > toDateTime('2020-10-17T20:51:46.110774') "
                "AND started <= toDateTime('2021-01-15T20:51:47.110825')"
            ),
        ),
        id="a",  # to find the specific test without counting them out
    ),
    pytest.param(
        {
            "selected_columns": [
                "release",
                "project_id",
                "bucketed_started",
                "sessions",
            ],
            "project": [2],
            "organization": 2,
            "dataset": "sessions",
            "from_date": "2020-10-17T20:51:46.110774",
            "to_date": "2021-01-15T20:51:47.110825",
            "groupby": ["release", "project_id", "bucketed_started"],
            "conditions": [
                ["release", "IN", ["foo@1.0.0", "foo@2.0.0"]],
                ["project_id", "IN", [2]],
                ["project_id", "IN", [2]],
            ],
            "aggregations": [],
            "granularity": 3600,
            "consistent": False,
        },
        (
            "-- DATASET: sessions",
            "MATCH (sessions)",
            "SELECT release, project_id, bucketed_started, sessions",
            "BY release, project_id, bucketed_started",
            (
                "WHERE release IN tuple('foo@1.0.0', 'foo@2.0.0') "
                "AND project_id IN tuple(2) "
                "AND project_id IN tuple(2) "
                "AND project_id IN array(2) "
                "AND org_id = 2 "
                "AND started > toDateTime('2020-10-17T20:51:46.110774') "
                "AND started <= toDateTime('2021-01-15T20:51:47.110825')"
            ),
            "GRANULARITY 3600",
        ),
        id="b",  # to find the specific test without counting them out
    ),
    pytest.param(
        {
            "selected_columns": ["release", "project_id", "bucketed_started", "users"],
            "project": [2],
            "organization": 2,
            "dataset": "sessions",
            "from_date": "2020-10-17T20:51:46.110774",
            "to_date": "2021-01-15T20:51:47.110825",
            "groupby": ["release", "project_id", "bucketed_started"],
            "conditions": [
                ["release", "IN", ["foo@1.0.0", "foo@2.0.0"]],
                ["project_id", "IN", [2]],
                ["project_id", "IN", [2]],
            ],
            "aggregations": [],
            "granularity": 3600,
            "consistent": False,
        },
        (
            "-- DATASET: sessions",
            "MATCH (sessions)",
            "SELECT release, project_id, bucketed_started, users",
            "BY release, project_id, bucketed_started",
            (
                "WHERE release IN tuple('foo@1.0.0', 'foo@2.0.0') "
                "AND project_id IN tuple(2) "
                "AND project_id IN tuple(2) "
                "AND project_id IN array(2) "
                "AND org_id = 2 "
                "AND started > toDateTime('2020-10-17T20:51:46.110774') "
                "AND started <= toDateTime('2021-01-15T20:51:47.110825')"
            ),
            "GRANULARITY 3600",
        ),
        id="c",  # to find the specific test without counting them out
    ),
    pytest.param(
        {
            "selected_columns": ["release", "project_id"],
            "project": [2],
            "organization": 2,
            "dataset": "sessions",
            "from_date": "2020-10-17T20:51:46.110774",
            "to_date": "2021-01-15T20:51:47.110825",
            "groupby": ["release", "project_id"],
            "conditions": [
                ["release", "IN", ["foo@1.0.0", "dummy-release"]],
                ["project_id", "IN", [2]],
            ],
            "aggregations": [],
            "consistent": False,
        },
        (
            "-- DATASET: sessions",
            "MATCH (sessions)",
            "SELECT release, project_id",
            "BY release, project_id",
            (
                "WHERE release IN tuple('foo@1.0.0', 'dummy-release') "
                "AND project_id IN tuple(2) "
                "AND project_id IN array(2) "
                "AND org_id = 2 "
                "AND started > toDateTime('2020-10-17T20:51:46.110774') "
                "AND started <= toDateTime('2021-01-15T20:51:47.110825')"
            ),
        ),
        id="d",  # to find the specific test without counting them out
    ),
    pytest.param(
        {
            "selected_columns": [
                ["min", ["started"], "oldest"],
                "project_id",
                "release",
            ],
            "project": [2],
            "organization": 2,
            "dataset": "sessions",
            "from_date": "2020-10-17T20:51:46.110774",
            "to_date": "2021-01-15T20:51:47.110825",
            "groupby": ["release", "project_id"],
            "conditions": [["release", "IN", ["foo@1.0.0"]], ["project_id", "IN", [2]]],
            "aggregations": [],
            "consistent": False,
        },
        (
            "-- DATASET: sessions",
            "MATCH (sessions)",
            "SELECT min(started) AS oldest, project_id, release",
            "BY release, project_id",
            (
                "WHERE release IN tuple('foo@1.0.0') "
                "AND project_id IN tuple(2) "
                "AND project_id IN array(2) "
                "AND org_id = 2 "
                "AND started > toDateTime('2020-10-17T20:51:46.110774') "
                "AND started <= toDateTime('2021-01-15T20:51:47.110825')"
            ),
        ),
        id="e",  # to find the specific test without counting them out
    ),
    pytest.param(
        {
            "selected_columns": ["release", "project_id", "users", "sessions"],
            "project": 2,
            "organization": 2,
            "dataset": "sessions",
            "from_date": "2020-10-17T20:51:46.110774",
            "to_date": "2021-01-15T20:51:47.110825",
            "groupby": ["release", "project_id"],
            "conditions": [
                ["release", "IN", ["foo@1.0.0", "foo@2.0.0", "dummy-release"]],
                ["project_id", "IN", [2]],
            ],
            "aggregations": [],
            "consistent": False,
        },
        (
            "-- DATASET: sessions",
            "MATCH (sessions)",
            "SELECT release, project_id, users, sessions",
            "BY release, project_id",
            (
                "WHERE release IN tuple('foo@1.0.0', 'foo@2.0.0', 'dummy-release') "
                "AND project_id IN tuple(2) "
                "AND project_id = 2 "
                "AND org_id = 2 "
                "AND started > toDateTime('2020-10-17T20:51:46.110774') "
                "AND started <= toDateTime('2021-01-15T20:51:47.110825')"
            ),
        ),
        id="f",  # to find the specific test without counting them out
    ),
    pytest.param(
        {
            "selected_columns": [
                "release",
                "project_id",
                "duration_quantiles",
                "sessions",
                "sessions_errored",
                "sessions_crashed",
                "sessions_abnormal",
                "users",
                "users_crashed",
            ],
            "project": [2],
            "organization": 2,
            "dataset": "sessions",
            "from_date": "2020-10-17T20:51:46.110774",
            "to_date": "2021-01-15T20:51:47.110825",
            "groupby": ["release", "project_id"],
            "conditions": [
                ["release", "IN", ["foo@1.0.0", "foo@2.0.0"]],
                ["project_id", "IN", [2]],
            ],
            "aggregations": [],
            "consistent": False,
        },
        (
            "-- DATASET: sessions",
            "MATCH (sessions)",
            "SELECT release, project_id, duration_quantiles, sessions, sessions_errored, sessions_crashed, sessions_abnormal, users, users_crashed",
            "BY release, project_id",
            (
                "WHERE release IN tuple('foo@1.0.0', 'foo@2.0.0') "
                "AND project_id IN tuple(2) "
                "AND project_id IN array(2) "
                "AND org_id = 2 "
                "AND started > toDateTime('2020-10-17T20:51:46.110774') "
                "AND started <= toDateTime('2021-01-15T20:51:47.110825')"
            ),
        ),
        id="g",  # to find the specific test without counting them out
    ),
    pytest.param(
        {
            "selected_columns": ["project_id", "release"],
            "orderby": ["-users"],
            "offset": 0,
            "limit": 100,
            "project": [2],
            "organization": 2,
            "dataset": "sessions",
            "from_date": "2020-10-17T20:51:46.110774",
            "to_date": "2021-01-15T20:51:47.110825",
            "groupby": ["release", "project_id"],
            "conditions": [["project_id", "IN", [2]]],
            "aggregations": [],
            "consistent": False,
        },
        (
            "-- DATASET: sessions",
            "MATCH (sessions)",
            "SELECT project_id, release",
            "BY release, project_id",
            (
                "WHERE project_id IN tuple(2) "
                "AND project_id IN array(2) "
                "AND org_id = 2 "
                "AND started > toDateTime('2020-10-17T20:51:46.110774') "
                "AND started <= toDateTime('2021-01-15T20:51:47.110825')"
            ),
            "ORDER BY users DESC",
            "LIMIT 100",
            "OFFSET 0",
        ),
        id="h",  # to find the specific test without counting them out
    ),
    pytest.param(
        {
            "selected_columns": ["project_id", "users"],
            "project": [2],
            "organization": 2,
            "dataset": "sessions",
            "from_date": "2020-10-17T20:51:46.110774",
            "to_date": "2021-01-15T20:51:47.110825",
            "groupby": ["project_id"],
            "conditions": [["project_id", "IN", [2]]],
            "aggregations": [],
            "consistent": False,
        },
        (
            "-- DATASET: sessions",
            "MATCH (sessions)",
            "SELECT project_id, users",
            "BY project_id",
            (
                "WHERE project_id IN tuple(2) "
                "AND project_id IN array(2) "
                "AND org_id = 2 "
                "AND started > toDateTime('2020-10-17T20:51:46.110774') "
                "AND started <= toDateTime('2021-01-15T20:51:47.110825')"
            ),
        ),
        id="i",  # to find the specific test without counting them out
    ),
    pytest.param(
        {
            "selected_columns": ["release", "project_id", "users", "sessions"],
            "project": [2],
            "organization": 2,
            "dataset": "sessions",
            "from_date": "2020-10-17T20:51:46.110774",
            "to_date": "2021-01-15T20:51:47.110825",
            "groupby": ["release", "project_id"],
            "conditions": [
                ["release", "IN", ["foo@1.0.0", "foo@2.0.0", "dummy-release"]],
                ["project_id", "IN", [2]],
            ],
            "aggregations": [],
            "consistent": False,
        },
        (
            "-- DATASET: sessions",
            "MATCH (sessions)",
            "SELECT release, project_id, users, sessions",
            "BY release, project_id",
            (
                "WHERE release IN tuple('foo@1.0.0', 'foo@2.0.0', 'dummy-release') "
                "AND project_id IN tuple(2) "
                "AND project_id IN array(2) "
                "AND org_id = 2 "
                "AND started > toDateTime('2020-10-17T20:51:46.110774') "
                "AND started <= toDateTime('2021-01-15T20:51:47.110825')"
            ),
        ),
        id="j",  # to find the specific test without counting them out
    ),
    pytest.param(
        {
            "selected_columns": ["project_id", "release"],
            "orderby": ["-sessions"],
            "offset": 0,
            "limit": 100,
            "project": [2],
            "organization": 2,
            "dataset": "sessions",
            "from_date": "2020-10-17T20:51:46.110774",
            "to_date": "2021-01-15T20:51:47.110825",
            "groupby": ["release", "project_id"],
            "conditions": [["project_id", "IN", [2]]],
            "aggregations": [],
            "consistent": False,
        },
        (
            "-- DATASET: sessions",
            "MATCH (sessions)",
            "SELECT project_id, release",
            "BY release, project_id",
            (
                "WHERE project_id IN tuple(2) "
                "AND project_id IN array(2) "
                "AND org_id = 2 "
                "AND started > toDateTime('2020-10-17T20:51:46.110774') "
                "AND started <= toDateTime('2021-01-15T20:51:47.110825')"
            ),
            "ORDER BY sessions DESC",
            "LIMIT 100",
            "OFFSET 0",
        ),
        id="k",  # to find the specific test without counting them out
    ),
]


@pytest.mark.parametrize("json_body, clauses", sentry_tests)
def test_json_to_snuba_for_sessions(
    json_body: Mapping[str, Any], clauses: Sequence[str]
) -> None:
    expected = "\n".join(clauses)
    query = json_to_snql(json_body, "sessions")
    assert query.print() == expected

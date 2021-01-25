import pytest
import re
from typing import Any, Mapping, Sequence

from snuba_sdk.query_visitors import InvalidQuery
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
            "consistent": False,
            "granularity": 86400,
            "totals": True,
        },
        (
            "-- DATASET: sessions",
            "MATCH (sessions)",
            "SELECT project_id, release, min(max(users) AS max_users) AS min_users",
            "BY release, project_id",
            (
                "WHERE project_id IN array(2) "
                "AND array_stuff IN array(array(2)) "
                "AND tuple_stuff IN tuple(tuple(2)) "
                "AND bucketed_started > toDateTime('2020-10-17T20:51:46.110774') "
                "AND project_id IN array(2) "
                "AND organization_id IN tuple(2) "
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
            "consistent": False,
        },
        (
            "-- DATASET: sessions",
            "MATCH (sessions)",
            "SELECT project_id, release, min(max(users) AS max_users) AS min_users",
            "BY release, project_id",
            (
                "WHERE project_id IN array(2) "
                "AND array_stuff IN array(array(2)) "
                "AND tuple_stuff IN tuple(tuple(2)) "
                "AND bucketed_started > toDateTime('2020-10-17T20:51:46.110774') "
                "AND project_id IN array(2) "
                "AND organization_id IN tuple(2) "
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
                "WHERE project_id IN array(2) "
                "AND array_stuff IN array(array(2)) "
                "AND tuple_stuff IN tuple(tuple(2)) "
                "AND bucketed_started > toDateTime('2020-10-17T20:51:46.110774') "
                "AND project_id IN array(2) "
                "AND organization_id IN tuple(2) "
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
]


@pytest.mark.parametrize("json_body, clauses", tests)
def test_json_to_snuba(json_body: Mapping[str, Any], clauses: Sequence[str]) -> None:
    expected = "\n".join(clauses)
    query = json_to_snql(json_body, "sessions")
    assert query.print() == expected


invalid_tests = [
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
            "aggregations": [["quantile(0.5)", ["duration"], "quantile_0_5"]],
            "consistent": False,
            "arrayjoin": "array_stuff",
        },
        InvalidQuery("SnQL does not support infix expressions: 'quantile(0.5)'"),
    )
]


@pytest.mark.parametrize("json_body, exception", invalid_tests)
def test_invalid_snuba_queries(
    json_body: Mapping[str, Any], exception: Exception
) -> None:
    with pytest.raises(type(exception), match=re.escape(str(exception))):
        query = json_to_snql(json_body, "sessions")
        query.print()


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
                "WHERE release IN array('foo@1.0.0', 'foo@2.0.0') "
                "AND project_id IN array(2) "
                "AND project_id IN array(2) "
                "AND organization_id = 2 "
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
                "WHERE release IN array('foo@1.0.0', 'foo@2.0.0') "
                "AND project_id IN array(2) "
                "AND project_id IN array(2) "
                "AND project_id IN array(2) "
                "AND organization_id = 2 "
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
                "WHERE release IN array('foo@1.0.0', 'foo@2.0.0') "
                "AND project_id IN array(2) "
                "AND project_id IN array(2) "
                "AND project_id IN array(2) "
                "AND organization_id = 2 "
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
                "WHERE release IN array('foo@1.0.0', 'dummy-release') "
                "AND project_id IN array(2) "
                "AND project_id IN array(2) "
                "AND organization_id = 2 "
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
                "WHERE release IN array('foo@1.0.0') "
                "AND project_id IN array(2) "
                "AND project_id IN array(2) "
                "AND organization_id = 2 "
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
                "WHERE release IN array('foo@1.0.0', 'foo@2.0.0', 'dummy-release') "
                "AND project_id IN array(2) "
                "AND project_id = 2 "
                "AND organization_id = 2 "
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
                "WHERE release IN array('foo@1.0.0', 'foo@2.0.0') "
                "AND project_id IN array(2) "
                "AND project_id IN array(2) "
                "AND organization_id = 2 "
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
                "WHERE project_id IN array(2) "
                "AND project_id IN array(2) "
                "AND organization_id = 2 "
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
                "WHERE project_id IN array(2) "
                "AND project_id IN array(2) "
                "AND organization_id = 2 "
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
                "WHERE release IN array('foo@1.0.0', 'foo@2.0.0', 'dummy-release') "
                "AND project_id IN array(2) "
                "AND project_id IN array(2) "
                "AND organization_id = 2 "
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
                "WHERE project_id IN array(2) "
                "AND project_id IN array(2) "
                "AND organization_id = 2 "
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

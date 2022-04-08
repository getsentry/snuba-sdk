from typing import Any, Mapping, Sequence

import pytest

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
            "SELECT min(max(users) AS `max_users`) AS `min_users`, project_id, release",
            "BY release, project_id",
            (
                "WHERE org_id IN tuple(2) "
                "AND started >= toDateTime('2020-10-17T20:51:46.110774') "
                "AND started < toDateTime('2021-01-15T20:51:47.110825') "
                "AND project_id IN tuple(2) "
                "AND project_id IN tuple(2) "
                "AND array_stuff IN tuple(tuple(2)) "
                "AND tuple_stuff IN tuple(tuple(2)) "
                "AND bucketed_started > toDateTime('2020-10-17T20:51:46.110774')"
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
            "MATCH (sessions SAMPLE 1000.0)",
            "SELECT min(max(users) AS `max_users`) AS `min_users`, project_id, release",
            "BY release, project_id",
            (
                "WHERE org_id IN tuple(2) "
                "AND started >= toDateTime('2020-10-17T20:51:46.110774') "
                "AND started < toDateTime('2021-01-15T20:51:47.110825') "
                "AND project_id IN tuple(2) "
                "AND project_id IN tuple(2) "
                "AND array_stuff IN tuple(tuple(2)) "
                "AND tuple_stuff IN tuple(tuple(2)) "
                "AND bucketed_started > toDateTime('2020-10-17T20:51:46.110774')"
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
            "SELECT min(max(users) AS `max_users`) AS `min_users`, project_id, release",
            "BY release, project_id",
            (
                "WHERE org_id IN tuple(2) "
                "AND started >= toDateTime('2020-10-17T20:51:46.110774') "
                "AND started < toDateTime('2021-01-15T20:51:47.110825') "
                "AND project_id IN tuple(2) "
                "AND project_id IN tuple(2) "
                "AND array_stuff IN tuple(tuple(2)) "
                "AND tuple_stuff IN tuple(tuple(2)) "
                "AND bucketed_started > toDateTime('2020-10-17T20:51:46.110774')"
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
            "SELECT project_id, release, array_stuff",
            "ARRAY JOIN array_stuff",
            (
                "WHERE org_id IN tuple(2) "
                "AND started >= toDateTime('2020-10-17T20:51:46.110774') "
                "AND started < toDateTime('2021-01-15T20:51:47.110825') "
                "AND project_id IN tuple(2) "
                "AND project_id IN tuple(2) "
                "AND array_stuff IN tuple(tuple(2)) "
                "AND tuple_stuff IN tuple(tuple(2)) "
                "AND bucketed_started > toDateTime('2020-10-17T20:51:46.110774')"
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
            "groupby": ["project_id", "release"],
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
                ["divide(count(), divide(120, 60))", None, "epm"],
            ],
            "consistent": False,
        },
        (
            "-- DATASET: sessions",
            "MATCH (sessions)",
            "SELECT apdex(duration, 300) AS `apdex`, uniqIf(user, greater(duration, 1200)) AS `misery`, quantile(0.75)(duration) AS `p75`, divide(count(), divide(120, 60)) AS `epm`",
            "BY project_id, release",
            (
                "WHERE org_id IN tuple(2) "
                "AND started >= toDateTime('2020-10-17T20:51:46.110774') "
                "AND started < toDateTime('2021-01-15T20:51:47.110825') "
                "AND project_id IN tuple(2) "
                "AND project_id IN tuple(2) "
                "AND array_stuff IN tuple(tuple(2)) "
                "AND tuple_stuff IN tuple(tuple(2)) "
                "AND bucketed_started > toDateTime('2020-10-17T20:51:46.110774')"
            ),
            "HAVING min_users > 10",
            "ORDER BY sessions ASC, project_id DESC",
            "LIMIT 11 BY release",
            "LIMIT 100",
            "OFFSET 0",
        ),
        id="curried/string functions",
    ),
    pytest.param(
        {
            "selected_columns": [
                "bucketed_started",
                "sessions_crashed",
                "sessions_abnormal",
                "sessions",
                "sessions_errored",
            ],
            "project": [2],
            "organization": 1,
            "dataset": "sessions",
            "from_date": "2021-04-07T17:00:00",
            "to_date": "2021-04-21T16:49:00",
            "groupby": ["bucketed_started"],
            "conditions": [
                ["release", "=", "1.1.4"],
                [["environment", "=", "production"]],
                ["project_id", "IN", [2]],
            ],
            "aggregations": [],
            "granularity": 3600,
            "consistent": False,
        },
        (
            "-- DATASET: sessions",
            "MATCH (sessions)",
            "SELECT bucketed_started, sessions_crashed, sessions_abnormal, sessions, sessions_errored",
            "BY bucketed_started",
            (
                "WHERE org_id = 1 "
                "AND started >= toDateTime('2021-04-07T17:00:00') "
                "AND started < toDateTime('2021-04-21T16:49:00') "
                "AND project_id IN tuple(2) "
                "AND release = '1.1.4' "
                "AND environment = 'production' "
                "AND project_id IN tuple(2)"
            ),
            "GRANULARITY 3600",
        ),
        id="nested_conditions",
    ),
]


@pytest.mark.parametrize("json_body, clauses", tests)
def test_json_to_snuba(json_body: Mapping[str, Any], clauses: Sequence[str]) -> None:
    request = json_to_snql(json_body, "sessions")
    request.validate()
    assert request.app_id == "legacy"
    assert request.flags is not None
    assert request.flags.legacy is True

    query_clauses = [c for c in clauses if not c.startswith("--")]
    query_expected = "\n".join(query_clauses)
    assert request.query.print() == query_expected

    # Rather than rewrite all these tests to have the new format, transform the
    # old format to the new one. This captures all the flags that were using
    # the -- print format, and asserts they are in the request flags
    flag_clauses = [c for c in clauses if c.startswith("--")]
    flags_expected = {}
    for fc in flag_clauses:
        flag, val = fc.replace("-- ", "").split(": ", 1)
        if flag == "DATASET":
            assert (
                request.dataset == val
            ), f"expected dataset {val} not {request.dataset}"
            continue

        flags_expected[flag.lower()] = val

    for eflag, evalue in flags_expected.items():
        assert str(getattr(request.flags, eflag)) == evalue

    for flag, value in request.flags.to_dict().items():
        if flag == "legacy":
            continue  # this is tested separately
        assert flag in flags_expected
        assert str(value) == flags_expected[flag]


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
                "WHERE org_id = 2 "
                "AND started >= toDateTime('2020-10-17T20:51:46.110774') "
                "AND started < toDateTime('2021-01-15T20:51:47.110825') "
                "AND project_id IN tuple(2) "
                "AND release IN tuple('foo@1.0.0', 'foo@2.0.0') "
                "AND project_id IN tuple(2)"
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
                "WHERE org_id = 2 "
                "AND started >= toDateTime('2020-10-17T20:51:46.110774') "
                "AND started < toDateTime('2021-01-15T20:51:47.110825') "
                "AND project_id IN tuple(2) "
                "AND release IN tuple('foo@1.0.0', 'foo@2.0.0') "
                "AND project_id IN tuple(2) "
                "AND project_id IN tuple(2)"
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
                "WHERE org_id = 2 "
                "AND started >= toDateTime('2020-10-17T20:51:46.110774') "
                "AND started < toDateTime('2021-01-15T20:51:47.110825') "
                "AND project_id IN tuple(2) "
                "AND release IN tuple('foo@1.0.0', 'foo@2.0.0') "
                "AND project_id IN tuple(2) "
                "AND project_id IN tuple(2)"
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
                "WHERE org_id = 2 "
                "AND started >= toDateTime('2020-10-17T20:51:46.110774') "
                "AND started < toDateTime('2021-01-15T20:51:47.110825') "
                "AND project_id IN tuple(2) "
                "AND release IN tuple('foo@1.0.0', 'dummy-release') "
                "AND project_id IN tuple(2)"
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
            "SELECT min(started) AS `oldest`, project_id, release",
            "BY release, project_id",
            (
                "WHERE org_id = 2 "
                "AND started >= toDateTime('2020-10-17T20:51:46.110774') "
                "AND started < toDateTime('2021-01-15T20:51:47.110825') "
                "AND project_id IN tuple(2) "
                "AND release IN tuple('foo@1.0.0') "
                "AND project_id IN tuple(2)"
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
                "WHERE org_id = 2 "
                "AND started >= toDateTime('2020-10-17T20:51:46.110774') "
                "AND started < toDateTime('2021-01-15T20:51:47.110825') "
                "AND project_id IN tuple(2) "
                "AND release IN tuple('foo@1.0.0', 'foo@2.0.0', 'dummy-release') "
                "AND project_id IN tuple(2)"
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
                "WHERE org_id = 2 "
                "AND started >= toDateTime('2020-10-17T20:51:46.110774') "
                "AND started < toDateTime('2021-01-15T20:51:47.110825') "
                "AND project_id IN tuple(2) "
                "AND release IN tuple('foo@1.0.0', 'foo@2.0.0') "
                "AND project_id IN tuple(2)"
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
                "WHERE org_id = 2 "
                "AND started >= toDateTime('2020-10-17T20:51:46.110774') "
                "AND started < toDateTime('2021-01-15T20:51:47.110825') "
                "AND project_id IN tuple(2) "
                "AND project_id IN tuple(2)"
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
                "WHERE org_id = 2 "
                "AND started >= toDateTime('2020-10-17T20:51:46.110774') "
                "AND started < toDateTime('2021-01-15T20:51:47.110825') "
                "AND project_id IN tuple(2) "
                "AND project_id IN tuple(2)"
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
                "WHERE org_id = 2 "
                "AND started >= toDateTime('2020-10-17T20:51:46.110774') "
                "AND started < toDateTime('2021-01-15T20:51:47.110825') "
                "AND project_id IN tuple(2) "
                "AND release IN tuple('foo@1.0.0', 'foo@2.0.0', 'dummy-release') "
                "AND project_id IN tuple(2)"
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
                "WHERE org_id = 2 "
                "AND started >= toDateTime('2020-10-17T20:51:46.110774') "
                "AND started < toDateTime('2021-01-15T20:51:47.110825') "
                "AND project_id IN tuple(2) "
                "AND project_id IN tuple(2)"
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
    request = json_to_snql(json_body, "sessions")
    request.validate()
    assert request.app_id == "legacy"
    assert request.flags is not None
    assert request.flags.legacy is True

    query_clauses = [c for c in clauses if not c.startswith("--")]
    query_expected = "\n".join(query_clauses)
    assert request.query.print() == query_expected

    # Rather than rewrite all these tests to have the new format, transform the
    # old format to the new one. This captures all the flags that were using
    # the -- print format, and asserts they are in the request flags
    flag_clauses = [c for c in clauses if c.startswith("--")]
    flags_expected = {}
    for fc in flag_clauses:
        flag, val = fc.replace("-- ", "").split(": ", 1)
        if flag == "DATASET":
            assert (
                request.dataset == val
            ), f"expected dataset {val} not {request.dataset}"
            continue

        flags_expected[flag.lower()] = val

    for eflag, evalue in flags_expected.items():
        assert str(getattr(request.flags, eflag)) == evalue

    for flag, value in request.flags.to_dict().items():
        if flag == "legacy":
            continue  # this is tested separately
        assert flag in flags_expected
        assert str(value) == flags_expected[flag]

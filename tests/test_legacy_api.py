import pytest
from typing import Any, Mapping, Sequence

from snuba_sdk.legacy import json_to_snql


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
            "SELECT count() AS count, arrayJoin(exception_stacks.type) AS exception_stacks.type",
            "BY exception_stacks.type",
            (
                "WHERE timestamp >= toDateTime('2021-03-03T11:22:00') "
                "AND timestamp < toDateTime('2021-03-03T17:22:00') "
                "AND project_id IN tuple(2) "
                "AND or(equals(exception_stacks.type, 'ArithmeticException'), equals(exception_stacks.type, 'RuntimeException')) = 1"
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
            "SELECT count() AS count, arrayJoin(measurements.key) AS array_join_measurements_key, plus(multiply(floor(divide(minus(multiply(arrayJoin(measurements.value), 100.0), 0.0), 1.0)), 1.0), 0.0) AS measurements_histogram_1_0_100",
            "BY array_join_measurements_key, measurements_histogram_1_0_100",
            (
                "WHERE finish_ts >= toDateTime('2021-03-03T11:22:00') "
                "AND finish_ts < toDateTime('2021-03-03T17:22:00') "
                "AND project_id IN tuple(2) "
                "AND type = 'transaction' "
                "AND transaction_op = 'pageload' "
                "AND transaction = '/organizations/:orgId/issues/' "
                "AND array_join_measurements_key IN tuple('cls') "
                "AND measurements_histogram_1_0_100 >= 0 "
                "AND project_id IN tuple(1)"
            ),
            "ORDER BY measurements_histogram_1_0_100 ASC, array_join_measurements_key ASC",
            "LIMIT 1",
        ),
        "discover_transactions",
        id="array join alias in groupby",
    ),
    pytest.param(
        {
            "project": 2,
            "dataset": "events",
            "groupby": ["project_id"],
            "aggregations": [
                ["count", "platform", "platforms"],
                ["uniq", "platform", "uniq_platforms"],
                ["topK(1)", "platform", "top_platforms"],
            ],
            "from_date": "2020-10-17T20:51:46.110774",
            "to_date": "2021-01-15T20:51:47.110825",
        },
        (
            "-- DATASET: events",
            "MATCH (events)",
            "SELECT count(platform) AS platforms, uniq(platform) AS uniq_platforms, topK(1)(platform) AS top_platforms",
            "BY project_id",
            (
                "WHERE timestamp >= toDateTime('2020-10-17T20:51:46.110774') "
                "AND timestamp < toDateTime('2021-01-15T20:51:47.110825') "
                "AND project_id IN tuple(2)"
            ),
        ),
        "events",
        id="topK handled",
    ),
    pytest.param(
        {
            "project": 2,
            "dataset": "events",
            "selected_columns": ["environment", "time"],
            "orderby": [["-substringUTF8", ["environment", 1, 3]], "time"],
            "debug": True,
            "from_date": "2020-10-17T20:51:46.110774",
            "to_date": "2021-01-15T20:51:47.110825",
        },
        (
            "-- DATASET: events",
            "-- DEBUG: True",
            "MATCH (events)",
            "SELECT environment, time",
            (
                "WHERE timestamp >= toDateTime('2020-10-17T20:51:46.110774') "
                "AND timestamp < toDateTime('2021-01-15T20:51:47.110825') "
                "AND project_id IN tuple(2)"
            ),
            "ORDER BY substringUTF8(environment, 1, 3) DESC, time ASC",
        ),
        "events",
        id="orderby with - in function",
    ),
    pytest.param(
        {
            "dataset": "events",
            "project": 2,
            "selected_columns": ["event_id"],
            "conditions": [["event_id", "LIKE", "stuff \\\" ' \\' stuff"]],
            "limit": 4,
            "orderby": ["event_id"],
            "from_date": "2020-10-17T20:51:46.110774",
            "to_date": "2021-01-15T20:51:47.110825",
        },
        (
            "-- DATASET: events",
            "MATCH (events)",
            "SELECT event_id",
            (
                "WHERE timestamp >= toDateTime('2020-10-17T20:51:46.110774') "
                "AND timestamp < toDateTime('2021-01-15T20:51:47.110825') "
                "AND project_id IN tuple(2) "
                "AND event_id LIKE 'stuff \\\" \\' \\\\' stuff'"
            ),
            "ORDER BY event_id ASC",
            "LIMIT 4",
        ),
        "events",
        id="escaping_strings",
    ),
    pytest.param(
        {
            "dataset": "discover",
            "turbo": False,
            "consistent": False,
            "aggregations": [["count", None, "count"]],
            "conditions": [
                [
                    [
                        "or",
                        [
                            ["equals", [["ifNull", ["tags[foo]", "''"]], "'baz'"]],
                            ["equals", [["ifNull", ["tags[foo.bar]", "''"]], "'qux'"]],
                        ],
                    ],
                    "=",
                    1,
                ],
                ["project_id", "IN", [2]],
            ],
            "groupby": "tags_key",
            "orderby": ["-count", "tags_key"],
            "having": [
                ["tags_key", "NOT IN", ["trace", "trace.ctx", "trace.span", "project"]]
            ],
            "project": [2],
            "limit": 10,
            "from_date": "2020-10-17T20:51:46.110774",
            "to_date": "2021-01-15T20:51:47.110825",
        },
        (
            "-- DATASET: discover",
            "MATCH (discover)",
            "SELECT count() AS count",
            "BY tags_key",
            (
                "WHERE timestamp >= toDateTime('2020-10-17T20:51:46.110774') "
                "AND timestamp < toDateTime('2021-01-15T20:51:47.110825') "
                "AND project_id IN tuple(2) "
                "AND or(equals(ifNull(tags[foo], ''), 'baz'), equals(ifNull(tags[foo.bar], ''), 'qux')) = 1 "
                "AND project_id IN tuple(2)"
            ),
            "HAVING tags_key NOT IN tuple('trace', 'trace.ctx', 'trace.span', 'project')",
            "ORDER BY count DESC, tags_key ASC",
            "LIMIT 10",
        ),
        "discover",
        id="tags_key_boolean_condition",
    ),
    pytest.param(
        {
            "dataset": "discover",
            "aggregations": [["divide(count(), divide(86400, 60))", None, "tpm"]],
            "having": [],
            "project": [2],
            "selected_columns": ["transaction"],
            "granularity": 3600,
            "totals": False,
            "conditions": [
                ["event_id", "=", "5897895a14504192"],
                ["type", "=", "transaction"],
            ],
            "groupby": ["transaction"],
            "from_date": "2020-10-17T20:51:46.110774",
            "to_date": "2021-01-15T20:51:47.110825",
        },
        (
            "-- DATASET: discover",
            "MATCH (discover_transactions)",
            "SELECT divide(count(), divide(86400, 60)) AS tpm, transaction",
            "BY transaction",
            (
                "WHERE finish_ts >= toDateTime('2020-10-17T20:51:46.110774') "
                "AND finish_ts < toDateTime('2021-01-15T20:51:47.110825') "
                "AND project_id IN tuple(2) "
                "AND event_id = '5897895a14504192' "
                "AND type = 'transaction'"
            ),
            "GRANULARITY 3600",
        ),
        "discover_transactions",
        id="invalid_event_id_condition",
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
                ["project_id", "IN", [2]],
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
            "from_date": "2020-10-17T20:51:46.110774",
            "to_date": "2021-01-15T20:51:47.110825",
        },
        (
            "-- DATASET: discover",
            "MATCH (discover_transactions)",
            "SELECT count() AS count, arrayJoin(measurements.key) AS array_join_measurements_key, plus(multiply(floor(divide(minus(multiply(arrayJoin(measurements.value), 100.0), 0.0), 1.0)), 1.0), 0.0) AS measurements_histogram_1_0_100",
            "BY array_join_measurements_key, measurements_histogram_1_0_100",
            (
                "WHERE finish_ts >= toDateTime('2020-10-17T20:51:46.110774') "
                "AND finish_ts < toDateTime('2021-01-15T20:51:47.110825') "
                "AND project_id IN tuple(2) "
                "AND type = 'transaction' "
                "AND transaction_op = 'pageload' "
                "AND transaction = '/organizations/:orgId/issues/' "
                "AND array_join_measurements_key IN tuple('cls') "
                "AND measurements_histogram_1_0_100 >= 0 "
                "AND project_id IN tuple(2)"
            ),
            "ORDER BY measurements_histogram_1_0_100 ASC, array_join_measurements_key ASC",
            "LIMIT 1",
        ),
        "discover_transactions",
        id="web_vitals_histogram_function",
    ),
    pytest.param(
        {
            "dataset": "discover",
            "project": 2,
            "aggregations": [["apdex(duration, 300)", None, "apdex_duration_300"]],
            "groupby": ["project_id", "tags[foo]"],
            "conditions": [],
            "orderby": "apdex_duration_300",
            "limit": 1000,
            "from_date": "2020-10-17T20:51:46.110774",
            "to_date": "2021-01-15T20:51:47.110825",
        },
        (
            "-- DATASET: discover",
            "MATCH (discover_transactions)",
            "SELECT apdex(duration, 300) AS apdex_duration_300",
            "BY project_id, tags[foo]",
            (
                "WHERE finish_ts >= toDateTime('2020-10-17T20:51:46.110774') "
                "AND finish_ts < toDateTime('2021-01-15T20:51:47.110825') "
                "AND project_id IN tuple(2)"
            ),
            "ORDER BY apdex_duration_300 ASC",
            "LIMIT 1000",
        ),
        "discover_transactions",
        id="ast_impossible_queries",
    ),
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
                            [
                                "equals",
                                ["exception_stacks.type", "'RuntimeException'"],
                            ],
                        ],
                    ],
                    "=",
                    1,
                ],
            ],
            "limit": 1000,
            "from_date": "2020-10-17T20:51:46.110774",
            "to_date": "2021-01-15T20:51:47.110825",
        },
        (
            "-- DATASET: discover",
            "-- DEBUG: True",
            "MATCH (discover_events)",
            "SELECT count() AS count, arrayJoin(exception_stacks.type) AS exception_stacks.type",
            "BY exception_stacks.type",
            (
                "WHERE timestamp >= toDateTime('2020-10-17T20:51:46.110774') "
                "AND timestamp < toDateTime('2021-01-15T20:51:47.110825') "
                "AND project_id IN tuple(2) "
                "AND or(equals(exception_stacks.type, 'ArithmeticException'), equals(exception_stacks.type, 'RuntimeException')) = 1"
            ),
            "LIMIT 1000",
        ),
        "discover_events",
        id="exception_stack_column_boolean_condition_arrayjoin_function",
    ),
    pytest.param(
        {
            "dataset": "discover",
            "project": 2,
            "selected_columns": ["type", "tags[custom_tag]", "release"],
            "conditions": [["type", "!=", "transaction"]],
            "orderby": "timestamp",
            "limit": 1000,
            "from_date": "2020-10-17T20:51:46.110774",
            "to_date": "2021-01-15T20:51:47.110825",
            "dry_run": True,
        },
        (
            "-- DATASET: discover",
            "-- DRY_RUN: True",
            "MATCH (discover_events)",
            "SELECT type, tags[custom_tag], release",
            (
                "WHERE timestamp >= toDateTime('2020-10-17T20:51:46.110774') "
                "AND timestamp < toDateTime('2021-01-15T20:51:47.110825') "
                "AND project_id IN tuple(2) "
                "AND type != 'transaction'"
            ),
            "ORDER BY timestamp ASC",
            "LIMIT 1000",
        ),
        "discover_events",
        id="dry_run_flag",
    ),
    pytest.param(
        {
            "selected_columns": ["first_session_started", "last_session_started"],
            "project": [2],
            "organization": 1,
            "dataset": "sessions",
            "from_date": "2021-01-12T20:04:37.175368",
            "to_date": "2021-04-12T20:04:38.173543",
            "groupby": [],
            "conditions": [
                ["release", "=", "stuff-2"],
                ["project_id", "IN", [2]],
                ["org_id", "IN", [1]],
            ],
            "aggregations": [
                ["min(started)", None, "first_session_started"],
                ["max(started)", None, "last_session_started"],
            ],
            "consistent": False,
        },
        (
            "-- DATASET: sessions",
            "MATCH (sessions)",
            "SELECT min(started) AS first_session_started, max(started) AS last_session_started, first_session_started, last_session_started",
            (
                "WHERE org_id = 1 "
                "AND started >= toDateTime('2021-01-12T20:04:37.175368') "
                "AND started < toDateTime('2021-04-12T20:04:38.173543') "
                "AND project_id IN tuple(2) "
                "AND release = 'stuff-2' "
                "AND project_id IN tuple(2) "
                "AND org_id IN tuple(1)"
            ),
        ),
        "sessions",
        id="aliases_in_select",
    ),
    pytest.param(
        {
            "selected_columns": ["first_session_started", "last_session_started"],
            "project": [2],
            "organization": 1,
            "dataset": "sessions",
            "from_date": "2021-01-14T17:07:48.124240",
            "to_date": "2021-04-14T17:07:49.078996",
            "groupby": [],
            "conditions": [
                ["release", "=", "stuff-02"],
                ["environment", "IN", set(["production"])],
                ["project_id", "IN", [2]],
                ["org_id", "IN", [1]],
            ],
            "aggregations": [
                ["min(started)", None, "first_session_started"],
                ["max(started)", None, "last_session_started"],
            ],
            "consistent": False,
        },
        (
            "-- DATASET: sessions",
            "MATCH (sessions)",
            "SELECT min(started) AS first_session_started, max(started) AS last_session_started, first_session_started, last_session_started",
            (
                "WHERE org_id = 1 "
                "AND started >= toDateTime('2021-01-14T17:07:48.124240') "
                "AND started < toDateTime('2021-04-14T17:07:49.078996') "
                "AND project_id IN tuple(2) "
                "AND release = 'stuff-02' "
                "AND environment IN tuple('production') "
                "AND project_id IN tuple(2) "
                "AND org_id IN tuple(1)"
            ),
        ),
        "sessions",
        id="convert_sets_in_legacy",
    ),
    pytest.param(
        {
            "selected_columns": [],
            "orderby": "-last_seen",
            "limit": 1000,
            "project": [2],
            "dataset": "events",
            "from_date": "2021-04-01T20:08:40",
            "to_date": "2021-04-15T20:08:40",
            "groupby": ["tags[_userEmail]"],
            "conditions": [
                ["tags[_userEmail]", "LIKE", "%%b%%"],
                ["type", "!=", "transaction"],
                ["project_id", "IN", [2]],
            ],
            "aggregations": [
                ["count()", "", "times_seen"],
                ["min", "timestamp", "first_seen"],
                ["max", "timestamp", "last_seen"],
            ],
            "consistent": False,
        },
        (
            "-- DATASET: events",
            "MATCH (events)",
            "SELECT count() AS times_seen, min(timestamp) AS first_seen, max(timestamp) AS last_seen",
            "BY tags[_userEmail]",
            (
                "WHERE timestamp >= toDateTime('2021-04-01T20:08:40') "
                "AND timestamp < toDateTime('2021-04-15T20:08:40') "
                "AND project_id IN tuple(2) "
                "AND tags[_userEmail] LIKE '%%b%%' "
                "AND type != 'transaction' "
                "AND project_id IN tuple(2)"
            ),
            "ORDER BY last_seen DESC",
            "LIMIT 1000",
        ),
        "events",
        id="handle_underscore_in_columns",
    ),
    pytest.param(
        {
            "selected_columns": [],
            "orderby": "-last_seen",
            "limit": 1000,
            "arrayjoin": "exception_frames",
            "project": [2],
            "dataset": "events",
            "from_date": "2021-04-01T20:05:27",
            "to_date": "2021-04-15T20:05:27",
            "groupby": ["exception_frames.filename"],
            "conditions": [
                ["exception_frames.filename", "LIKE", "%/stuff/things/%"],
                ["project_id", "IN", [2]],
            ],
            "aggregations": [
                ["count()", "", "times_seen"],
                ["min", "timestamp", "first_seen"],
                ["max", "timestamp", "last_seen"],
            ],
            "consistent": False,
        },
        (
            "-- DATASET: events",
            "MATCH (events)",
            "SELECT count() AS times_seen, min(timestamp) AS first_seen, max(timestamp) AS last_seen",
            "BY exception_frames.filename",
            "ARRAY JOIN exception_frames",
            (
                "WHERE timestamp >= toDateTime('2021-04-01T20:05:27') "
                "AND timestamp < toDateTime('2021-04-15T20:05:27') "
                "AND project_id IN tuple(2) "
                "AND exception_frames.filename LIKE '%/stuff/things/%' "
                "AND project_id IN tuple(2)"
            ),
            "ORDER BY last_seen DESC",
            "LIMIT 1000",
        ),
        "events",
        id="arrayjoin_clause_with_groupby",
    ),
    pytest.param(
        {
            "selected_columns": [
                "transaction",
                "project_id",
                [
                    "transform",
                    [
                        ["toString", ["project_id"]],
                        ["array", ["'2'"]],
                        ["array", ["'project_mc_projectpants'"]],
                        "''",
                    ],
                    "project",
                ],
            ],
            "having": [
                ["tpm", ">", 0.01],
                ["count_percentage", ">", 0.25],
                ["count_percentage", "<", 4.0],
                ["trend_percentage", "<", 1.0],
                ["t_test", ">", 6.0],
            ],
            "orderby": ["trend_percentage"],
            "limit": 6,
            "offset": 0,
            "project": [2],
            "dataset": "discover",
            "from_date": "2021-04-22T19:37:49",
            "to_date": "2021-04-23T19:37:49",
            "groupby": ["transaction", "project_id"],
            "conditions": [
                ["duration", ">", 0.0],
                ["duration", "<", 900000.0],
                ["type", "=", "transaction"],
                ["project_id", "IN", [2]],
            ],
            "aggregations": [
                [
                    "varSampIf",
                    [
                        "duration",
                        [
                            "greater",
                            [["toDateTime", ["'2021-04-23T07:37:49'"]], "timestamp"],
                        ],
                    ],
                    "variance_range_1",
                ],
                [
                    "varSampIf",
                    [
                        "duration",
                        [
                            "lessOrEquals",
                            [["toDateTime", ["'2021-04-23T07:37:49'"]], "timestamp"],
                        ],
                    ],
                    "variance_range_2",
                ],
                [
                    "avgIf",
                    [
                        "duration",
                        [
                            "greater",
                            [["toDateTime", ["'2021-04-23T07:37:49'"]], "timestamp"],
                        ],
                    ],
                    "avg_range_1",
                ],
                [
                    "avgIf",
                    [
                        "duration",
                        [
                            "lessOrEquals",
                            [["toDateTime", ["'2021-04-23T07:37:49'"]], "timestamp"],
                        ],
                    ],
                    "avg_range_2",
                ],
                [
                    "divide(minus(avg_range_1,avg_range_2),sqrt(plus(divide(variance_range_1,count_range_1),divide(variance_range_2,count_range_2))))",
                    None,
                    "t_test",
                ],
                [
                    "quantileIf(0.50)",
                    [
                        "duration",
                        [
                            "greater",
                            [["toDateTime", ["'2021-04-23T07:37:49'"]], "timestamp"],
                        ],
                    ],
                    "aggregate_range_1",
                ],
                [
                    "quantileIf(0.50)",
                    [
                        "duration",
                        [
                            "lessOrEquals",
                            [["toDateTime", ["'2021-04-23T07:37:49'"]], "timestamp"],
                        ],
                    ],
                    "aggregate_range_2",
                ],
                [
                    "if(greater(aggregate_range_1,0),divide(aggregate_range_2,aggregate_range_1),null)",
                    None,
                    "trend_percentage",
                ],
                [
                    "minus",
                    ["aggregate_range_2", "aggregate_range_1"],
                    "trend_difference",
                ],
                [
                    "countIf",
                    [
                        [
                            "greater",
                            [["toDateTime", ["'2021-04-23T07:37:49'"]], "timestamp"],
                        ]
                    ],
                    "count_range_1",
                ],
                [
                    "countIf",
                    [
                        [
                            "lessOrEquals",
                            [["toDateTime", ["'2021-04-23T07:37:49'"]], "timestamp"],
                        ]
                    ],
                    "count_range_2",
                ],
                [
                    "if(greater(count_range_1,0),divide(count_range_2,count_range_1),null)",
                    None,
                    "count_percentage",
                ],
                ["divide(count(), divide(86400, 60))", None, "tpm"],
            ],
            "consistent": False,
        },
        (
            "-- DATASET: discover",
            "MATCH (discover_transactions)",
            (
                "SELECT varSampIf(duration, greater(toDateTime('2021-04-23T07:37:49'), timestamp)) AS variance_range_1, "
                "varSampIf(duration, lessOrEquals(toDateTime('2021-04-23T07:37:49'), timestamp)) AS variance_range_2, "
                "avgIf(duration, greater(toDateTime('2021-04-23T07:37:49'), timestamp)) AS avg_range_1, "
                "avgIf(duration, lessOrEquals(toDateTime('2021-04-23T07:37:49'), timestamp)) AS avg_range_2, "
                "divide(minus(avg_range_1,avg_range_2),sqrt(plus(divide(variance_range_1,count_range_1),divide(variance_range_2,count_range_2)))) AS t_test, "
                "quantileIf(0.50)(duration, greater(toDateTime('2021-04-23T07:37:49'), timestamp)) AS aggregate_range_1, "
                "quantileIf(0.50)(duration, lessOrEquals(toDateTime('2021-04-23T07:37:49'), timestamp)) AS aggregate_range_2, "
                "if(greater(aggregate_range_1,0),divide(aggregate_range_2,aggregate_range_1),null) AS trend_percentage, "
                "minus(aggregate_range_2, aggregate_range_1) AS trend_difference, "
                "countIf(greater(toDateTime('2021-04-23T07:37:49'), timestamp)) AS count_range_1, "
                "countIf(lessOrEquals(toDateTime('2021-04-23T07:37:49'), timestamp)) AS count_range_2, "
                "if(greater(count_range_1,0),divide(count_range_2,count_range_1),null) AS count_percentage, divide(count(), divide(86400, 60)) AS tpm, "
                "transaction, project_id, transform(toString(project_id), array('2'), array('project_mc_projectpants'), '') AS project"
            ),
            "BY transaction, project_id",
            (
                "WHERE finish_ts >= toDateTime('2021-04-22T19:37:49') "
                "AND finish_ts < toDateTime('2021-04-23T19:37:49') "
                "AND project_id IN tuple(2) "
                "AND duration > 0.0 "
                "AND duration < 900000.0 "
                "AND type = 'transaction' "
                "AND project_id IN tuple(2)"
            ),
            (
                "HAVING tpm > 0.01 "
                "AND count_percentage > 0.25 "
                "AND count_percentage < 4.0 "
                "AND trend_percentage < 1.0 "
                "AND t_test > 6.0"
            ),
            "ORDER BY trend_percentage ASC",
            "LIMIT 6",
            "OFFSET 0",
        ),
        "discover_transactions",
        id="largs_aggregates_not_in_groupby",
    ),
    pytest.param(
        {
            "selected_columns": [],
            "orderby": "-times_seen",
            "project": [2],
            "dataset": "events",
            "from_date": "2021-04-01T20:05:27",
            "to_date": "2021-04-15T20:05:27",
            "groupby": ["project_id", "tags[sentry:release]"],
            "conditions": [
                ["tags[sentry:release]", "IN", ["2021-04-26T14:57:14"]],
                ["type", "!=", "transaction"],
                ["project_id", "IN", [2]],
            ],
            "aggregations": [
                ["count()", "", "times_seen"],
                ["min", "timestamp", "first_seen"],
                ["max", "timestamp", "last_seen"],
            ],
            "consistent": False,
            "debug": False,
        },
        (
            "-- DATASET: events",
            "MATCH (events)",
            "SELECT count() AS times_seen, min(timestamp) AS first_seen, max(timestamp) AS last_seen",
            "BY project_id, tags[sentry:release]",
            (
                "WHERE timestamp >= toDateTime('2021-04-01T20:05:27') "
                "AND timestamp < toDateTime('2021-04-15T20:05:27') "
                "AND project_id IN tuple(2) "
                "AND tags[sentry:release] IN tuple('2021-04-26T14:57:14') "
                "AND type != 'transaction' "
                "AND project_id IN tuple(2)"
            ),
            "ORDER BY times_seen DESC",
        ),
        "events",
        id="tags_are_always_strings",
    ),
    pytest.param(
        {
            "selected_columns": (
                "time",
                "outcome",
                "category",
                "quantity",
                ("toString", ("category",)),
            ),
            "limit": 10000,
            "organization": 1,
            "dataset": "outcomes_raw",
            "from_date": "2021-04-01T20:05:27",
            "to_date": "2021-04-15T20:05:27",
            "groupby": ["time", "outcome", "category", ("toString", ("category",))],
            "conditions": [("project_id", "IN", [2, 3, 4, 5, 6])],
            "aggregations": [("sum", "quantity", "quantity")],
            "granularity": 60,
            "consistent": False,
        },
        (
            "-- DATASET: outcomes_raw",
            "MATCH (outcomes_raw)",
            "SELECT sum(quantity) AS quantity, time, outcome, category, quantity, toString(tuple('category'))",
            "BY time, outcome, category, toString(tuple('category'))",
            (
                "WHERE org_id = 1 "
                "AND timestamp >= toDateTime('2021-04-01T20:05:27') "
                "AND timestamp < toDateTime('2021-04-15T20:05:27') "
                "AND project_id IN tuple(2, 3, 4, 5, 6)"
            ),
            "LIMIT 10000",
            "GRANULARITY 60",
        ),
        "outcomes_raw",
        id="tuples_in_weird_spots",
    ),
    pytest.param(
        {
            "aggregations": [],
            "conditions": [
                [["coalesce", ["email", "username", "ip_address"]], "=", "8.8.8.8"],
                ["project_id", "IN", [1]],
                ["group_id", "IN", [33]],
            ],
            "granularity": 3600,
            "groupby": [],
            "having": [],
            "limit": 51,
            "offset": 0,
            "orderby": ["-timestamp.to_hour"],
            "project": [1],
            "selected_columns": [
                ["coalesce", ["email", "username", "ip_address"], "user.display"],
                "release",
                ["toStartOfHour", ["timestamp"], "timestamp.to_hour"],
                "event_id",
                "project_id",
                [
                    "transform",
                    [
                        ["toString", ["project_id"]],
                        ["array", ["'1'"]],
                        ["array", ["'stuff'"]],
                        "''",
                    ],
                    "project.name",
                ],
            ],
            "from_date": "2021-04-01T20:05:27",
            "to_date": "2021-04-15T20:05:27",
            "totals": False,
        },
        (
            "-- DATASET: events",
            "MATCH (events)",
            (
                "SELECT coalesce(email, username, ip_address) AS user.display, "
                "release, toStartOfHour(timestamp) AS timestamp.to_hour, event_id, "
                "project_id, transform(toString(project_id), array('1'), array('stuff'), '') AS project.name"
            ),
            (
                "WHERE timestamp >= toDateTime('2021-04-01T20:05:27') "
                "AND timestamp < toDateTime('2021-04-15T20:05:27') "
                "AND project_id IN tuple(1) "
                "AND coalesce(email, username, ip_address) = '8.8.8.8' "
                "AND project_id IN tuple(1) "
                "AND group_id IN tuple(33)"
            ),
            "ORDER BY timestamp.to_hour DESC",
            "LIMIT 51",
            "OFFSET 0",
            "GRANULARITY 3600",
        ),
        "events",
        id="transform_is_not_an_aggregate",
    ),
]


@pytest.mark.parametrize("json_body, clauses, entity", discover_tests)
def test_discover_json_to_snuba(
    json_body: Mapping[str, Any], clauses: Sequence[str], entity: str
) -> None:
    expected = "\n".join(clauses)
    query = json_to_snql(json_body, entity)
    query.validate()
    assert query.print() == expected

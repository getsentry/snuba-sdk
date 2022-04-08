from typing import Any, Mapping, Sequence

import pytest

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
            "SELECT count() AS `count`, arrayJoin(exception_stacks.type) AS `exception_stacks.type`",
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
            "SELECT count() AS `count`, arrayJoin(measurements.key) AS `array_join_measurements_key`, plus(multiply(floor(divide(minus(multiply(arrayJoin(measurements.value), 100.0), 0.0), 1.0)), 1.0), 0.0) AS `measurements_histogram_1_0_100`",
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
            "SELECT count(platform) AS `platforms`, uniq(platform) AS `uniq_platforms`, topK(1)(platform) AS `top_platforms`",
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
            "conditions": [["event_id", "LIKE", "stuff \\\" ' \\' stuff\\"]],
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
                "AND event_id LIKE 'stuff \\\\\" \\' \\\\\\' stuff\\\\'"
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
            "SELECT count() AS `count`",
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
            "SELECT divide(count(), divide(86400, 60)) AS `tpm`, transaction",
            "BY transaction",
            (
                "WHERE finish_ts >= toDateTime('2020-10-17T20:51:46.110774') "
                "AND finish_ts < toDateTime('2021-01-15T20:51:47.110825') "
                "AND project_id IN tuple(2) "
                "AND event_id = '5897895a14504192' "
                "AND type = 'transaction'"
            ),
            "GRANULARITY 3600",
            "TOTALS False",
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
            "SELECT count() AS `count`, arrayJoin(measurements.key) AS `array_join_measurements_key`, plus(multiply(floor(divide(minus(multiply(arrayJoin(measurements.value), 100.0), 0.0), 1.0)), 1.0), 0.0) AS `measurements_histogram_1_0_100`",
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
            "SELECT apdex(duration, 300) AS `apdex_duration_300`",
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
            "SELECT count() AS `count`, arrayJoin(exception_stacks.type) AS `exception_stacks.type`",
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
            "SELECT min(started) AS `first_session_started`, max(started) AS `last_session_started`, first_session_started, last_session_started",
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
            "SELECT min(started) AS `first_session_started`, max(started) AS `last_session_started`, first_session_started, last_session_started",
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
            "SELECT count() AS `times_seen`, min(timestamp) AS `first_seen`, max(timestamp) AS `last_seen`",
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
            "SELECT count() AS `times_seen`, min(timestamp) AS `first_seen`, max(timestamp) AS `last_seen`",
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
                "SELECT varSampIf(duration, greater(toDateTime('2021-04-23T07:37:49'), timestamp)) AS `variance_range_1`, "
                "varSampIf(duration, lessOrEquals(toDateTime('2021-04-23T07:37:49'), timestamp)) AS `variance_range_2`, "
                "avgIf(duration, greater(toDateTime('2021-04-23T07:37:49'), timestamp)) AS `avg_range_1`, "
                "avgIf(duration, lessOrEquals(toDateTime('2021-04-23T07:37:49'), timestamp)) AS `avg_range_2`, "
                "divide(minus(avg_range_1,avg_range_2),sqrt(plus(divide(variance_range_1,count_range_1),divide(variance_range_2,count_range_2)))) AS `t_test`, "
                "quantileIf(0.50)(duration, greater(toDateTime('2021-04-23T07:37:49'), timestamp)) AS `aggregate_range_1`, "
                "quantileIf(0.50)(duration, lessOrEquals(toDateTime('2021-04-23T07:37:49'), timestamp)) AS `aggregate_range_2`, "
                "if(greater(aggregate_range_1,0),divide(aggregate_range_2,aggregate_range_1),null) AS `trend_percentage`, "
                "minus(aggregate_range_2, aggregate_range_1) AS `trend_difference`, "
                "countIf(greater(toDateTime('2021-04-23T07:37:49'), timestamp)) AS `count_range_1`, "
                "countIf(lessOrEquals(toDateTime('2021-04-23T07:37:49'), timestamp)) AS `count_range_2`, "
                "if(greater(count_range_1,0),divide(count_range_2,count_range_1),null) AS `count_percentage`, divide(count(), divide(86400, 60)) AS `tpm`, "
                "transaction, project_id, transform(toString(project_id), array('2'), array('project_mc_projectpants'), '') AS `project`"
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
            "SELECT count() AS `times_seen`, min(timestamp) AS `first_seen`, max(timestamp) AS `last_seen`",
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
            "SELECT sum(quantity) AS `quantity`, time, outcome, category, quantity, toString(tuple('category'))",
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
                "SELECT coalesce(email, username, ip_address) AS `user.display`, "
                "release, toStartOfHour(timestamp) AS `timestamp.to_hour`, event_id, "
                "project_id, transform(toString(project_id), array('1'), array('stuff'), '') AS `project.name`"
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
            "TOTALS False",
        ),
        "events",
        id="transform_is_not_an_aggregate",
    ),
    pytest.param(
        {
            "selected_columns": [],
            "having": [],
            "limit": 51,
            "offset": 0,
            "project": [1],
            "dataset": "discover",
            "from_date": "2021-04-01T20:05:27",
            "to_date": "2021-04-15T20:05:27",
            "groupby": [],
            "conditions": [
                ["type", "=", "transaction"],
                ["transaction", "=", "GET /some/stuff"],
                ["project_id", "IN", [1]],
            ],
            "aggregations": [
                ["apdex(duration, 300)", None, "apdex_300"],
                [
                    "uniqIf",
                    ["user", ["greater", ["duration", 1200.0]]],
                    "count_miserable_user_300",
                ],
                ["quantile(0.95)", "duration", "p95"],
                ["count", None, "count"],
                ["uniq", "user", "count_unique_user"],
                ["failure_rate()", None, "failure_rate"],
                ["divide(count(), divide(1.2096e+06, 60))", None, "tpm"],
                [
                    "ifNull(divide(plus(uniqIf(user, greater(duration, 1200)), 5.8875), plus(uniq(user), 117.75)), 0)",
                    None,
                    "user_misery_300",
                ],
                [
                    "quantile(0.75)",
                    "measurements[fp]",
                    "percentile_measurements_fp_0_75",
                ],
                [
                    "quantile(0.75)",
                    "measurements[fcp]",
                    "percentile_measurements_fcp_0_75",
                ],
                [
                    "quantile(0.75)",
                    "measurements[lcp]",
                    "percentile_measurements_lcp_0_75",
                ],
                [
                    "quantile(0.75)",
                    "measurements[fid]",
                    "percentile_measurements_fid_0_75",
                ],
                [
                    "quantile(0.75)",
                    "measurements[cls]",
                    "percentile_measurements_cls_0_75",
                ],
            ],
            "consistent": False,
        },
        (
            "-- DATASET: discover",
            "MATCH (transactions)",
            (
                "SELECT apdex(duration, 300) AS `apdex_300`, "
                "uniqIf(user, greater(duration, 1200.0)) AS `count_miserable_user_300`, "
                "quantile(0.95)(duration) AS `p95`, count() AS `count`, "
                "uniq(user) AS `count_unique_user`, "
                "failure_rate() AS `failure_rate`, "
                "divide(count(), divide(1.2096e+06, 60)) AS `tpm`, "
                "ifNull(divide(plus(uniqIf(user, greater(duration, 1200)), 5.8875), plus(uniq(user), 117.75)), 0) AS `user_misery_300`, "
                "quantile(0.75)(measurements[fp]) AS `percentile_measurements_fp_0_75`, "
                "quantile(0.75)(measurements[fcp]) AS `percentile_measurements_fcp_0_75`, "
                "quantile(0.75)(measurements[lcp]) AS `percentile_measurements_lcp_0_75`, "
                "quantile(0.75)(measurements[fid]) AS `percentile_measurements_fid_0_75`, "
                "quantile(0.75)(measurements[cls]) AS `percentile_measurements_cls_0_75`"
            ),
            (
                "WHERE finish_ts >= toDateTime('2021-04-01T20:05:27') "
                "AND finish_ts < toDateTime('2021-04-15T20:05:27') "
                "AND project_id IN tuple(1) "
                "AND type = 'transaction' "
                "AND transaction = 'GET /some/stuff' "
                "AND project_id IN tuple(1)"
            ),
            "LIMIT 51",
            "OFFSET 0",
        ),
        "transactions",
        id="plus_in_numbers",
    ),
    pytest.param(
        {
            "selected_columns": [],
            "having": [],
            "orderby": ["-last_seen", "group_id"],
            "limit": 150,
            "offset": 0,
            "totals": True,
            "turbo": False,
            "sample": 1,
            "project": [1],
            "dataset": "events",
            "from_date": "2021-04-01T20:05:27",
            "to_date": "2021-04-15T20:05:27",
            "groupby": ["group_id"],
            "conditions": [
                [["positionCaseInsensitive", ["message", "''''"]], "!=", 0],
                ["project_id", "IN", [1]],
                ["environment", "IN", ["production"]],
                ["group_id", "IN", [3]],
            ],
            "aggregations": [
                ["multiply(toUInt64(max(timestamp)), 1000)", "", "last_seen"],
                ["uniq", "group_id", "total"],
            ],
            "consistent": False,
            "debug": False,
        },
        (
            "-- DATASET: events",
            "MATCH (events SAMPLE 1.0)",
            "SELECT multiply(toUInt64(max(timestamp)), 1000) AS `last_seen`, uniq(group_id) AS `total`",
            "BY group_id",
            (
                "WHERE timestamp >= toDateTime('2021-04-01T20:05:27') "
                "AND timestamp < toDateTime('2021-04-15T20:05:27') "
                "AND project_id IN tuple(1) "
                "AND positionCaseInsensitive(message, '\\'\\'') != 0 "
                "AND project_id IN tuple(1) "
                "AND environment IN tuple('production') "
                "AND group_id IN tuple(3)"
            ),
            "ORDER BY last_seen DESC, group_id ASC",
            "LIMIT 150",
            "OFFSET 0",
            "TOTALS True",
        ),
        "events",
        id="condition_on_empty_string",
    ),
    pytest.param(
        {
            "selected_columns": [
                "timestamp",
                "message",
                "title",
                "event_id",
                "project_id",
                [
                    "transform",
                    [
                        ["toString", ["project_id"]],
                        ["array", ["'1'"]],
                        ["array", ["'proj'"]],
                        "''",
                    ],
                    "`project.name`",
                ],
            ],
            "having": [],
            "limit": 81,
            "offset": 0,
            "project": [1],
            "dataset": "discover",
            "from_date": "2021-04-01T20:05:27",
            "to_date": "2021-04-15T20:05:27",
            "groupby": [],
            "conditions": [[["environment", "=", "PROD"]], ["project_id", "IN", [1]]],
            "aggregations": [],
            "consistent": False,
        },
        (
            "-- DATASET: discover",
            "MATCH (events)",
            "SELECT timestamp, message, title, event_id, project_id, transform(toString(project_id), array('1'), array('proj'), '') AS `project.name`",
            (
                "WHERE timestamp >= toDateTime('2021-04-01T20:05:27') "
                "AND timestamp < toDateTime('2021-04-15T20:05:27') "
                "AND project_id IN tuple(1) "
                "AND environment = 'PROD' "
                "AND project_id IN tuple(1)"
            ),
            "LIMIT 81",
            "OFFSET 0",
        ),
        "events",
        id="dots_in_alias",
    ),
    pytest.param(
        {
            "project": [1],
            "dataset": "events",
            "from_date": "2021-04-01T20:05:27",
            "to_date": "2021-04-15T20:05:27",
            "groupby": ["group_id"],
            "conditions": [
                [["positionCaseInsensitive", ["message", "'Api\\'"]], "!=", 0],
                ["project_id", "IN", [1]],
                ["group_id", "IN", [1234567890]],
                ["environment", "IN", ["production"]],
            ],
            "aggregations": [
                ["count()", "", "times_seen"],
                ["min", "timestamp", "first_seen"],
                ["max", "timestamp", "last_seen"],
                ["uniq", "tags[sentry:user]", "count"],
            ],
            "consistent": False,
            "debug": False,
        },
        (
            "-- DATASET: events",
            "MATCH (events)",
            (
                "SELECT count() AS `times_seen`, "
                "min(timestamp) AS `first_seen`, "
                "max(timestamp) AS `last_seen`, "
                "uniq(tags[sentry:user]) AS `count`"
            ),
            "BY group_id",
            (
                "WHERE timestamp >= toDateTime('2021-04-01T20:05:27') "
                "AND timestamp < toDateTime('2021-04-15T20:05:27') "
                "AND project_id IN tuple(1) "
                "AND positionCaseInsensitive(message, 'Api\\\\') != 0 "
                "AND project_id IN tuple(1) "
                "AND group_id IN tuple(1234567890) "
                "AND environment IN tuple('production')"
            ),
        ),
        "events",
        id="quotes_escaped_with_backslash",
    ),
    pytest.param(
        {
            "aggregations": [],
            "conditions": [
                ["release", "=", "2021-06-15T13:07:34"],
                ("project_id", "IN", [1]),
            ],
            "dataset": "sessions",
            "from_date": "2021-04-01T20:05:27",
            "to_date": "2021-04-15T20:05:27",
            "granularity": 3600,
            "groupby": ["bucketed_started", "project_id", "release"],
            "organization": 2,
            "project": 1,
            "selected_columns": [
                "bucketed_started",
                "sessions",
                "users",
                "project_id",
                "release",
            ],
        },
        (
            "-- DATASET: sessions",
            "MATCH (sessions)",
            "SELECT bucketed_started, sessions, users, project_id, release",
            "BY bucketed_started, project_id, release",
            (
                "WHERE org_id = 2 "
                "AND started >= toDateTime('2021-04-01T20:05:27') "
                "AND started < toDateTime('2021-04-15T20:05:27') "
                "AND project_id IN tuple(1) "
                "AND release = '2021-06-15T13:07:34' "
                "AND project_id IN tuple(1)"
            ),
            "GRANULARITY 3600",
        ),
        "sessions",
        id="strings_that_look_like_datetimes",
    ),
    pytest.param(
        {
            "selected_columns": [
                ["tuple", ["'duration'", 300], "project_threshold_config"]
            ],
            "having": [],
            "orderby": ["-tpm"],
            "limit": 2,
            "offset": 0,
            "project": [1],
            "dataset": "discover",
            "from_date": "2021-04-01T20:05:27",
            "to_date": "2021-04-15T20:05:27",
            "groupby": ["project_threshold_config"],
            "conditions": [
                ["duration", "<", 900000.0],
                ["type", "=", "transaction"],
                ["project_id", "IN", [1]],
            ],
            "aggregations": [
                ["quantile(0.75)", "duration", "p75_transaction_duration"],
                ["divide(count(), divide(86400, 60))", None, "tpm"],
                ["failure_rate()", None, "failure_rate"],
                [
                    "apdex(multiIf(equals(tupleElement(project_threshold_config,1),'lcp'),if(has(measurements.key,'lcp'),arrayElement(measurements.value,indexOf(measurements.key,'lcp')),NULL),duration),tupleElement(project_threshold_config,2))",
                    None,
                    "apdex",
                ],
            ],
            "consistent": False,
        },
        (
            "-- DATASET: discover",
            "MATCH (discover)",
            (
                "SELECT quantile(0.75)(duration) AS `p75_transaction_duration`, "
                "divide(count(), divide(86400, 60)) AS `tpm`, "
                "failure_rate() AS `failure_rate`, "
                "apdex(multiIf(equals(tupleElement(project_threshold_config,1),'lcp'),if(has(measurements.key,'lcp'),arrayElement(measurements.value,indexOf(measurements.key,'lcp')),NULL),duration),tupleElement(project_threshold_config,2)) AS `apdex`, "
                "tuple('duration', 300) AS `project_threshold_config`"
            ),
            "BY project_threshold_config",
            (
                "WHERE timestamp >= toDateTime('2021-04-01T20:05:27') "
                "AND timestamp < toDateTime('2021-04-15T20:05:27') "
                "AND project_id IN tuple(1) "
                "AND duration < 900000.0 "
                "AND type = 'transaction' "
                "AND project_id IN tuple(1)"
            ),
            "ORDER BY tpm DESC",
            "LIMIT 2",
            "OFFSET 0",
        ),
        "discover",
        id="crazy_apdex_functions",
    ),
    pytest.param(
        {
            "conditions": [
                [
                    ["ifNull", ["tags[event.timestamp]", "''"]],
                    "=",
                    "2021-06-06T01:00:00",
                ],
                ["type", "!=", "transaction"],
                ("project_id", "IN", [1]),
                ("group_id", "IN", [1234567890]),
            ],
            "dataset": "events",
            "from_date": "2021-04-01T20:05:27",
            "to_date": "2021-04-15T20:05:27",
            "groupby": [],
            "limit": 101,
            "offset": 0,
            "orderby": ["-timestamp", "-event_id"],
            "project": ["1818675"],
            "selected_columns": ["event_id", "group_id", "project_id", "timestamp"],
        },
        (
            "-- DATASET: events",
            "MATCH (events)",
            "SELECT event_id, group_id, project_id, timestamp",
            (
                "WHERE timestamp >= toDateTime('2021-04-01T20:05:27') "
                "AND timestamp < toDateTime('2021-04-15T20:05:27') "
                "AND project_id IN tuple('1818675') "
                "AND ifNull(tags[event.timestamp], '') = '2021-06-06T01:00:00' "
                "AND type != 'transaction' "
                "AND project_id IN tuple(1) "
                "AND group_id IN tuple(1234567890)"
            ),
            "ORDER BY timestamp DESC, event_id DESC",
            "LIMIT 101",
            "OFFSET 0",
        ),
        "events",
        id="wrapped_tag_functions",
    ),
    pytest.param(
        {
            "selected_columns": [],
            "having": [],
            "orderby": ["-trend", "group_id"],
            "limit": 2,
            "offset": 0,
            "totals": True,
            "turbo": False,
            "sample": 1,
            "project": [1],
            "dataset": "events",
            "from_date": "2021-07-22T18:23:15",
            "to_date": "2021-07-23T18:23:14",
            "groupby": ["group_id"],
            "conditions": [("project_id", "IN", [1]), ("group_id", "IN", [1, 2])],
            "aggregations": [
                ["uniq", "group_id", "total"],
                [
                    "if(greater(countIf(greater(toDateTime('2021-07-23T06:23:14'), timestamp)), 0), divide(countIf(lessOrEquals(toDateTime('2021-07-23T06:23:14'), timestamp)), countIf(greater(toDateTime('2021-07-23T06:23:14'), timestamp))), 0)",
                    "",
                    "trend",
                ],
            ],
            "consistent": False,
        },
        (
            "-- DATASET: events",
            "MATCH (events SAMPLE 1.0)",
            (
                "SELECT uniq(group_id) AS `total`, "
                "if(greater(countIf(greater(toDateTime('2021-07-23T06:23:14'), timestamp)), 0), "
                "divide(countIf(lessOrEquals(toDateTime('2021-07-23T06:23:14'), timestamp)), "
                "countIf(greater(toDateTime('2021-07-23T06:23:14'), timestamp))), 0) AS `trend`"
            ),
            "BY group_id",
            (
                "WHERE timestamp >= toDateTime('2021-07-22T18:23:15') "
                "AND timestamp < toDateTime('2021-07-23T18:23:14') "
                "AND project_id IN tuple(1) "
                "AND project_id IN tuple(1) "
                "AND group_id IN tuple(1, 2)"
            ),
            "ORDER BY trend DESC, group_id ASC",
            "LIMIT 2",
            "OFFSET 0",
            "TOTALS True",
        ),
        "events",
        id="string_functions_with_datetimes",
    ),
    pytest.param(
        {
            "aggregations": [],
            "conditions": [
                [
                    [
                        "or",
                        [
                            ["release", "IN", ["hash"]],
                            [
                                "or",
                                [
                                    ["equals", ["release", "deadbeef"]],
                                    [
                                        "or",
                                        [
                                            ["equals", ["release", "abadcafe"]],
                                            ["release", "IN", ["fullhash"]],
                                        ],
                                    ],
                                ],
                            ],
                        ],
                    ],
                    "=",
                    1,
                ],
                ["project_id", "IN", ["1"]],
            ],
            "dataset": "sessions",
            "from_date": "2021-07-22T18:23:15",
            "to_date": "2021-07-23T18:23:14",
            "granularity": 86400,
            "groupby": ["release", "project_id"],
            "organization": 2,
            "project": [1],
            "selected_columns": [
                "sessions_crashed",
                "sessions_abnormal",
                "sessions_errored",
                "sessions",
                "project_id",
                "release",
            ],
        },
        (
            "-- DATASET: sessions",
            "MATCH (sessions)",
            "SELECT sessions_crashed, sessions_abnormal, sessions_errored, sessions, project_id, release",
            "BY release, project_id",
            (
                "WHERE org_id = 2 "
                "AND started >= toDateTime('2021-07-22T18:23:15') "
                "AND started < toDateTime('2021-07-23T18:23:14') "
                "AND project_id IN tuple(1) "
                "AND or(in(release, tuple('hash')), or(equals(release, deadbeef), or(equals(release, abadcafe), in(release, tuple('fullhash'))))) = 1 "
                "AND project_id IN tuple('1')"
            ),
            "GRANULARITY 86400",
        ),
        "sessions",
        id="conditions_nested_in_or_functions",
    ),
]


@pytest.mark.parametrize("json_body, clauses, entity", discover_tests)
def test_discover_json_to_snuba(
    json_body: Mapping[str, Any], clauses: Sequence[str], entity: str
) -> None:
    request = json_to_snql(json_body, entity)
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

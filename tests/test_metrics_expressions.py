from __future__ import annotations

import re
from typing import Any, Mapping, Optional

import pytest

from snuba_sdk.expressions import InvalidExpressionError, Totals
from snuba_sdk.metrics_visitors import RollupSnQLPrinter, ScopeSnQLPrinter
from snuba_sdk.orderby import Direction
from snuba_sdk.timeseries import MetricsScope, Rollup

rollup_tests = [
    pytest.param(
        60,
        None,
        None,
        60,
        {
            "orderby": "time ASC",
            "filter": "granularity = 60",
            "interval": "toStartOfInterval(timestamp, toIntervalSecond(60), 'Universal') AS `time`",
        },
        None,
        id="1",
    ),
    pytest.param(
        3600,
        None,
        None,
        60,
        {
            "orderby": "time ASC",
            "filter": "granularity = 60",
            "interval": "toStartOfInterval(timestamp, toIntervalSecond(3600), 'Universal') AS `time`",
        },
        None,
        id="2",
    ),
    pytest.param(
        None,
        None,
        None,
        60,
        None,
        InvalidExpressionError("Rollup must have at least one of interval or totals"),
        id="3",
    ),
    pytest.param(
        None,
        True,
        Direction.ASC,
        60,
        {
            "orderby": "aggregate_value ASC",
            "filter": "granularity = 60",
            "interval": "",
        },
        None,
        id="4",
    ),
    pytest.param(
        "61",
        None,
        None,
        60,
        None,
        InvalidExpressionError("interval '61' must be an integer"),
        id="5",
    ),
    pytest.param(
        None,
        Totals(True),
        None,
        60,
        None,
        InvalidExpressionError("totals must be a boolean"),
        id="6",
    ),
    pytest.param(
        60,
        Totals(True),
        None,
        60,
        None,
        InvalidExpressionError(
            "Only one of interval and totals can be set: Timeseries can't be rolled up by an interval and by a total"
        ),
        id="7",
    ),
    pytest.param(
        None,
        "False",
        None,
        60,
        None,
        InvalidExpressionError("totals must be a boolean"),
        id="8",
    ),
    pytest.param(
        60,
        None,
        6,
        60,
        None,
        InvalidExpressionError("orderby must be a Direction object"),
        id="9",
    ),
    pytest.param(
        60,
        None,
        None,
        None,
        None,
        InvalidExpressionError("granularity must be an integer"),
        id="10",
    ),
    pytest.param(
        60,
        None,
        6,
        "60",
        None,
        InvalidExpressionError("granularity must be an integer"),
        id="11",
    ),
    pytest.param(
        60,
        None,
        None,
        61,
        None,
        InvalidExpressionError("granularity must be an integer"),
        id="12",
    ),
    pytest.param(
        60,
        None,
        None,
        3600,
        None,
        InvalidExpressionError("interval must be greater than or equal to granularity"),
        id="13",
    ),
]


TRANSLATOR = RollupSnQLPrinter()


@pytest.mark.parametrize(
    "interval, totals, orderby, granularity, translated, exception", rollup_tests
)
def test_rollup(
    interval: Any,
    totals: Any,
    orderby: Any,
    granularity: Any,
    translated: Mapping[str, str],
    exception: Optional[Exception],
) -> None:
    if exception is not None:
        with pytest.raises(type(exception), match=re.escape(str(exception))):
            Rollup(interval, totals, orderby, granularity)
    else:
        rollup = Rollup(interval, totals, orderby, granularity)
        assert rollup.interval == interval
        assert TRANSLATOR.visit(rollup) == translated


metric_scope_tests = [
    pytest.param(
        [1],
        [11],
        "transactions",
        "(org_id IN array(1) AND project_id IN array(11) AND use_case_id = 'transactions')",
        None,
    ),
    pytest.param(
        [1, 2],
        [11, 12],
        "transactions",
        "(org_id IN array(1, 2) AND project_id IN array(11, 12) AND use_case_id = 'transactions')",
        None,
    ),
    pytest.param([1, 2], [11, 12], None, None, None),
    pytest.param(
        "1",
        [11, 12],
        "transactions",
        None,
        InvalidExpressionError("org_ids must be a list of integers"),
    ),
    pytest.param(
        [1, "2"],
        [11, 12],
        "transactions",
        None,
        InvalidExpressionError("org_ids must be a list of integers"),
    ),
    pytest.param(
        None,
        [11, 12],
        "transactions",
        None,
        InvalidExpressionError("org_ids must be a list of integers"),
    ),
    pytest.param(
        [1, 2],
        "12",
        "transactions",
        None,
        InvalidExpressionError("project_ids must be a list of integers"),
    ),
    pytest.param(
        [1, 2],
        [11, "12"],
        "transactions",
        None,
        InvalidExpressionError("project_ids must be a list of integers"),
    ),
    pytest.param(
        [1, 2],
        None,
        "transactions",
        None,
        InvalidExpressionError("project_ids must be a list of integers"),
    ),
    pytest.param(
        [1, 2],
        [11, 12],
        111,
        None,
        InvalidExpressionError("use_case_id must be an str"),
    ),
]

SCOPE_TRANSLATOR = ScopeSnQLPrinter()


@pytest.mark.parametrize(
    "org_ids, project_ids, use_case_id, translated, exception", metric_scope_tests
)
def test_metric_scope(
    org_ids: Any,
    project_ids: Any,
    use_case_id: Any,
    translated: str | None,
    exception: Exception | None,
) -> None:
    if exception is not None:
        with pytest.raises(type(exception), match=re.escape(str(exception))):
            m = MetricsScope(org_ids, project_ids, use_case_id)
            SCOPE_TRANSLATOR.visit(m)
    else:
        scope = MetricsScope(org_ids, project_ids, use_case_id)
        assert scope.project_ids == project_ids
        if translated is not None:
            assert SCOPE_TRANSLATOR.visit(scope) == translated

        if use_case_id:  # It's not possible to "unset" a use_case_id
            new_scope = MetricsScope(org_ids, project_ids, None).set_use_case_id(
                use_case_id
            )
            assert new_scope.use_case_id == use_case_id

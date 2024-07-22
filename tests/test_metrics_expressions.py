from __future__ import annotations

import re
from typing import Any, Mapping, Optional

import pytest

from snuba_sdk.expressions import InvalidExpressionError, Totals
from snuba_sdk.metrics_visitors import RollupMQLPrinter, ScopeMQLPrinter
from snuba_sdk.orderby import Direction
from snuba_sdk.timeseries import MetricsScope, Rollup

rollup_tests = [
    pytest.param(
        60,
        None,
        None,
        60,
        {"granularity": 60, "interval": 60, "orderby": None, "with_totals": None},
        None,
        id="1",
    ),
    pytest.param(
        3600,
        None,
        None,
        60,
        {"granularity": 60, "interval": 3600, "orderby": None, "with_totals": None},
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
        False,
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
        {"granularity": 60, "interval": None, "orderby": "ASC", "with_totals": "True"},
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
        True,
        None,
        60,
        {"granularity": 60, "interval": 60, "orderby": None, "with_totals": "True"},
        None,
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
        {"granularity": None, "interval": 60, "orderby": None, "with_totals": None},
        None,
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


TRANSLATOR = RollupMQLPrinter()


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
        assert TRANSLATOR.visit(rollup) == translated, TRANSLATOR.visit(rollup)


metric_scope_tests = [
    pytest.param(
        [1],
        [11],
        "transactions",
        {"org_ids": [1], "project_ids": [11], "use_case_id": "transactions"},
        None,
    ),
    pytest.param(
        [1, 2],
        [11, 12],
        "transactions",
        {"org_ids": [1, 2], "project_ids": [11, 12], "use_case_id": "transactions"},
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

SCOPE_TRANSLATOR = ScopeMQLPrinter()


@pytest.mark.parametrize(
    "org_ids, project_ids, use_case_id, translated, exception", metric_scope_tests
)
def test_metric_scope(
    org_ids: Any,
    project_ids: Any,
    use_case_id: Any,
    translated: dict[str, Any] | None,
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
            assert SCOPE_TRANSLATOR.visit(scope) == translated, SCOPE_TRANSLATOR.visit(
                scope
            )

        if use_case_id:  # It's not possible to "unset" a use_case_id
            new_scope = MetricsScope(org_ids, project_ids, None).set_use_case_id(
                use_case_id
            )
            assert new_scope.use_case_id == use_case_id

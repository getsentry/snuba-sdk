import re
from typing import Any, Optional

import pytest

from snuba_sdk.expressions import InvalidExpressionError, Totals
from snuba_sdk.metrics_query import MetricScope, Rollup

# TODO: Test orderby properly once it's correctly implemented

rollup_tests = [
    pytest.param(60, None, None, None),
    pytest.param(None, True, None, None),
    pytest.param(
        None,
        None,
        None,
        InvalidExpressionError("Rollup must have at least one of interval or totals"),
    ),
    pytest.param(
        61,
        None,
        None,
        InvalidExpressionError("interval 61 must be one of (60, 3600, 86400)"),
    ),
    pytest.param(
        "61",
        None,
        None,
        InvalidExpressionError(
            "interval must be an integer and one of (60, 3600, 86400)"
        ),
    ),
    pytest.param(
        None,
        Totals(True),
        None,
        InvalidExpressionError("totals must be a boolean"),
    ),
    pytest.param(
        None,
        False,
        None,
        InvalidExpressionError("Rollup must have at least one of interval or totals"),
    ),
    pytest.param(
        None,
        "False",
        None,
        InvalidExpressionError("totals must be a boolean"),
    ),
]


@pytest.mark.parametrize("interval, totals, orderby, exception", rollup_tests)
def test_rollup(
    interval: Any, totals: Any, orderby: Any, exception: Optional[Exception]
) -> None:
    if exception is not None:
        with pytest.raises(type(exception), match=re.escape(str(exception))):
            Rollup(interval, totals, orderby)
    else:
        assert Rollup(interval, totals, orderby).interval == interval


metric_scope_tests = [
    pytest.param([1], [11], "transactions", None),
    pytest.param([1, 2], [11, 12], "transactions", None),
    pytest.param([1, 2], [11, 12], None, None),
    pytest.param(
        "1",
        [11, 12],
        "transactions",
        InvalidExpressionError("org_ids must be a list of integers"),
    ),
    pytest.param(
        [1, "2"],
        [11, 12],
        "transactions",
        InvalidExpressionError("org_ids must be a list of integers"),
    ),
    pytest.param(
        None,
        [11, 12],
        "transactions",
        InvalidExpressionError("org_ids must be a list of integers"),
    ),
    pytest.param(
        [1, 2],
        "12",
        "transactions",
        InvalidExpressionError("project_ids must be a list of integers"),
    ),
    pytest.param(
        [1, 2],
        [11, "12"],
        "transactions",
        InvalidExpressionError("project_ids must be a list of integers"),
    ),
    pytest.param(
        [1, 2],
        None,
        "transactions",
        InvalidExpressionError("project_ids must be a list of integers"),
    ),
    pytest.param(
        [1, 2],
        [11, 12],
        1,
        InvalidExpressionError("use_case_id must be an str"),
    ),
]


@pytest.mark.parametrize(
    "org_ids, project_ids, use_case_id, exception", metric_scope_tests
)
def test_metric_scope(
    org_ids: Any, project_ids: Any, use_case_id: Any, exception: Optional[Exception]
) -> None:
    if exception is not None:
        with pytest.raises(type(exception), match=re.escape(str(exception))):
            MetricScope(org_ids, project_ids, use_case_id)
    else:
        assert MetricScope(org_ids, project_ids, use_case_id).project_ids == project_ids
        if use_case_id:  # It's not possible to "unset" a use_case_id
            new_scope = MetricScope(org_ids, project_ids, None).set_use_case_id(
                use_case_id
            )
            assert new_scope.use_case_id == use_case_id

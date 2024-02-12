from __future__ import annotations

import re
from typing import Any, Callable, Optional

import pytest

from snuba_sdk.aliased_expression import AliasedExpression
from snuba_sdk.column import Column
from snuba_sdk.conditions import Condition, Op, Or
from snuba_sdk.metrics_visitors import MetricMQLPrinter, TimeseriesMQLPrinter
from snuba_sdk.timeseries import InvalidTimeseriesError, Metric
from tests import timeseries

metric_tests = [
    pytest.param(
        "transaction.duration",
        "d:transactions/duration@millisecond",
        123,
        "d:transactions/duration@millisecond",
        None,
    ),
    pytest.param(
        "transaction.duration",
        "d:transactions/duration@millisecond",
        None,
        None,
        None,
    ),
    pytest.param(
        "transaction.duration",
        None,
        123,
        "transaction.duration",
        None,
    ),
    pytest.param(
        None,
        "d:transactions/duration@millisecond",
        123,
        "d:transactions/duration@millisecond",
        None,
    ),
    pytest.param(
        "transaction.duration",
        None,
        None,
        None,
        None,
    ),
    pytest.param(
        None,
        "d:transactions/duration@millisecond",
        None,
        None,
        None,
    ),
    pytest.param(
        None,
        None,
        123,
        None,
        InvalidTimeseriesError("Metric must have at least one of public_name or mri"),
    ),
    pytest.param(
        None,
        None,
        None,
        None,
        InvalidTimeseriesError("Metric must have at least one of public_name or mri"),
    ),
    pytest.param(
        123,
        "d:transactions/duration@millisecond",
        123,
        None,
        InvalidTimeseriesError("public_name must be a string"),
    ),
    pytest.param(
        "transaction.duration",
        123,
        123,
        None,
        InvalidTimeseriesError("mri must be a string"),
    ),
    pytest.param(
        "transaction.duration",
        "d:transactions/duration@millisecond",
        "wrong",
        None,
        InvalidTimeseriesError("id must be an integer"),
    ),
]

METRIC_PRINTER = MetricMQLPrinter()


@pytest.mark.parametrize("public_name, mri, mid, translated, exception", metric_tests)
def test_metric(
    public_name: Any,
    mri: Any,
    mid: Any,
    translated: str | None,
    exception: Optional[Exception],
) -> None:
    if exception is not None:
        with pytest.raises(type(exception), match=re.escape(str(exception))):
            Metric(public_name, mri, mid)
    else:
        metric = Metric(public_name, mri, mid)
        assert metric.id == mid
        if translated:
            assert METRIC_PRINTER.visit(metric) == translated, METRIC_PRINTER.visit(
                metric
            )


timeseries_tests = [
    pytest.param(
        timeseries(Metric("duration"), "count", None, None, None),
        "count(duration)",
        None,
        id="simple test",
    ),
    pytest.param(
        timeseries(Metric("duration"), "quantile", [0.95], None, None),
        "quantile(0.95)(duration)",
        None,
        id="aggregate params",
    ),
    pytest.param(
        timeseries(
            Metric("duration"),
            "quantile",
            [0.95],
            [Condition(Column("tags[release]"), Op.EQ, "1.2.3")],
            None,
        ),
        'quantile(0.95)(duration){tags[release]:"1.2.3"}',
        None,
        id="filter",
    ),
    pytest.param(
        timeseries(
            Metric("duration"),
            "quantile",
            [0.95],
            [
                Condition(Column("tags[release]"), Op.EQ, "1.2.3"),
                Condition(Column("tags[highway]"), Op.EQ, "401"),
            ],
            None,
        ),
        'quantile(0.95)(duration){tags[release]:"1.2.3" AND tags[highway]:"401"}',
        None,
        id="filters",
    ),
    pytest.param(
        timeseries(
            Metric("duration"),
            "quantile",
            [0.95],
            [
                Or(
                    [
                        Condition(Column("tags[release]"), Op.EQ, "1.2.3"),
                        Condition(Column("tags[highway]"), Op.EQ, "401"),
                    ]
                )
            ],
            None,
        ),
        'quantile(0.95)(duration){(tags[release]:"1.2.3" OR tags[highway]:"401")}',
        None,
        id="boolean condition filters",
    ),
    pytest.param(
        timeseries(
            Metric("duration"),
            "quantile",
            [0.95],
            [
                Condition(Column("tags[release]"), Op.EQ, "1.2.3"),
                Condition(Column("tags[highway]"), Op.EQ, "401"),
            ],
            [Column("tags[transaction]")],
        ),
        'quantile(0.95)(duration){tags[release]:"1.2.3" AND tags[highway]:"401"} by (tags[transaction])',
        None,
        id="groupby",
    ),
    pytest.param(
        timeseries(
            Metric("duration"),
            "quantile",
            [0.95],
            [
                Condition(Column("tags[release]"), Op.EQ, "1.2.3"),
                Condition(Column("tags[highway]"), Op.EQ, "401"),
            ],
            [Column("tags[transaction]"), Column("tags[device]")],
        ),
        'quantile(0.95)(duration){tags[release]:"1.2.3" AND tags[highway]:"401"} by (tags[transaction], tags[device])',
        None,
        id="groupbys",
    ),
    pytest.param(
        timeseries(
            Metric("duration"),
            "quantile",
            [0.95],
            [
                Condition(Column("tags[release]"), Op.EQ, "1.2.3"),
                Condition(Column("tags[highway]"), Op.EQ, "401"),
            ],
            [
                AliasedExpression(Column("tags[transaction]"), "transaction"),
                AliasedExpression(Column("tags[device]"), "device"),
            ],
        ),
        'quantile(0.95)(duration){tags[release]:"1.2.3" AND tags[highway]:"401"} by (tags[transaction] AS `transaction`, tags[device] AS `device`)',
        None,
        id="aliased groupbys",
    ),
    pytest.param(
        timeseries(Metric("duration"), 456, None, None, None),
        None,
        InvalidTimeseriesError("aggregate must be a string"),
        id="invalid aggregate",
    ),
    pytest.param(
        timeseries(Metric("duration"), "count", 6, None, None),
        None,
        InvalidTimeseriesError("aggregate_params must be a list"),
        id="invalid aggregate param",
    ),
    pytest.param(
        timeseries(
            Metric("duration"),
            "count",
            [Column("test")],
            None,
            None,
        ),
        None,
        InvalidTimeseriesError("aggregate_params can only be literal types"),
        id="invalid aggregate params",
    ),
    pytest.param(
        timeseries(Metric("duration"), "count", None, 6, None),
        None,
        InvalidTimeseriesError("filters must be a list"),
        id="invalid filter",
    ),
    pytest.param(
        timeseries(
            Metric("duration"),
            "count",
            None,
            [Column("test")],
            None,
        ),
        None,
        InvalidTimeseriesError("filters must be a list of Conditions"),
        id="invalid filters",
    ),
    pytest.param(
        timeseries(Metric("duration"), "count", None, None, 3),
        None,
        InvalidTimeseriesError("groupby must be a list"),
        id="invalid groupby",
    ),
    pytest.param(
        timeseries(
            Metric("duration"),
            "count",
            None,
            None,
            [Metric("duration")],
        ),
        None,
        InvalidTimeseriesError("groupby must be a list of Columns"),
        id="invalid groupbys",
    ),
]

TRANSLATOR = TimeseriesMQLPrinter()


@pytest.mark.parametrize("func_wrapper, translated, exception", timeseries_tests)
def test_timeseries(
    func_wrapper: Callable[[], Any],
    translated: str | None,
    exception: Exception | None,
) -> None:
    def verify() -> None:
        exp = func_wrapper()
        assert TRANSLATOR.visit(exp) == translated, TRANSLATOR.visit(exp)

    if exception is not None:
        with pytest.raises(type(exception), match=re.escape(str(exception))):
            verify()
    else:
        verify()

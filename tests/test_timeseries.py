from __future__ import annotations

import re
from typing import Any, Callable, Mapping, Optional

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
        "generic_metrics_distributions",
        {
            "entity": "generic_metrics_distributions",
            "metric_name": "d:transactions/duration@millisecond",
        },
        None,
    ),
    pytest.param(
        "transaction.duration",
        "d:transactions/duration@millisecond",
        None,
        "generic_metrics_distributions",
        None,
        None,
    ),
    pytest.param(
        "transaction.duration",
        None,
        123,
        "generic_metrics_distributions",
        {
            "entity": "generic_metrics_distributions",
            "metric_name": "transaction.duration",
        },
        None,
    ),
    pytest.param(
        None,
        "d:transactions/duration@millisecond",
        123,
        "generic_metrics_distributions",
        {
            "entity": "generic_metrics_distributions",
            "metric_name": "d:transactions/duration@millisecond",
        },
        None,
    ),
    pytest.param(
        "transaction.duration",
        None,
        None,
        "generic_metrics_distributions",
        None,
        None,
    ),
    pytest.param(
        None,
        "d:transactions/duration@millisecond",
        None,
        "generic_metrics_distributions",
        None,
        None,
    ),
    pytest.param(
        None,
        None,
        123,
        "generic_metrics_distributions",
        None,
        InvalidTimeseriesError("Metric must have at least one of public_name or mri"),
    ),
    pytest.param(
        None,
        None,
        None,
        "generic_metrics_distributions",
        None,
        InvalidTimeseriesError("Metric must have at least one of public_name or mri"),
    ),
    pytest.param(
        123,
        "d:transactions/duration@millisecond",
        123,
        "generic_metrics_distributions",
        None,
        InvalidTimeseriesError("public_name must be a string"),
    ),
    pytest.param(
        "transaction.duration",
        123,
        123,
        "generic_metrics_distributions",
        None,
        InvalidTimeseriesError("mri must be a string"),
    ),
    pytest.param(
        "transaction.duration",
        "d:transactions/duration@millisecond",
        "wrong",
        "generic_metrics_distributions",
        None,
        InvalidTimeseriesError("id must be an integer"),
    ),
    pytest.param(
        "transaction.duration",
        "d:transactions/duration@millisecond",
        123,
        667,
        None,
        InvalidTimeseriesError("entity must be a string"),
    ),
]

METRIC_PRINTER = MetricMQLPrinter()


@pytest.mark.parametrize(
    "public_name, mri, mid, entity, translated, exception", metric_tests
)
def test_metric(
    public_name: Any,
    mri: Any,
    mid: Any,
    entity: Any,
    translated: dict[str, str] | None,
    exception: Optional[Exception],
) -> None:
    if exception is not None:
        with pytest.raises(type(exception), match=re.escape(str(exception))):
            Metric(public_name, mri, mid, entity)
    else:
        metric = Metric(public_name, mri, mid, entity)
        assert metric.id == mid
        if translated:
            assert METRIC_PRINTER.visit(metric) == translated, METRIC_PRINTER.visit(
                metric
            )


timeseries_tests = [
    pytest.param(
        timeseries(
            Metric("duration", entity="metrics_sets"), "count", None, None, None
        ),
        {"entity": "metrics_sets", "mql_string": "count(duration)"},
        None,
        id="simple test",
    ),
    pytest.param(
        timeseries(
            Metric("duration", entity="metrics_sets"), "quantile", [0.95], None, None
        ),
        {"entity": "metrics_sets", "mql_string": "quantile(0.95)(duration)"},
        None,
        id="aggregate params",
    ),
    pytest.param(
        timeseries(
            Metric("duration", entity="metrics_sets"),
            "quantile",
            [0.95],
            [Condition(Column("tags[release]"), Op.EQ, "1.2.3")],
            None,
        ),
        {
            "entity": "metrics_sets",
            "mql_string": 'quantile(0.95)(duration){tags[release]:"1.2.3"}',
        },
        None,
        id="filter",
    ),
    pytest.param(
        timeseries(
            Metric("duration", entity="metrics_sets"),
            "quantile",
            [0.95],
            [
                Condition(Column("tags[release]"), Op.EQ, "1.2.3"),
                Condition(Column("tags[highway]"), Op.EQ, "401"),
            ],
            None,
        ),
        {
            "entity": "metrics_sets",
            "mql_string": 'quantile(0.95)(duration){tags[release]:"1.2.3" AND tags[highway]:"401"}',
        },
        None,
        id="filters",
    ),
    pytest.param(
        timeseries(
            Metric("duration", entity="metrics_sets"),
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
        {
            "entity": "metrics_sets",
            "mql_string": 'quantile(0.95)(duration){(tags[release]:"1.2.3" OR tags[highway]:"401")}',
        },
        None,
        id="boolean condition filters",
    ),
    pytest.param(
        timeseries(
            Metric("duration", entity="metrics_sets"),
            "quantile",
            [0.95],
            [
                Condition(Column("tags[release]"), Op.EQ, "1.2.3"),
                Condition(Column("tags[highway]"), Op.EQ, "401"),
            ],
            [Column("tags[transaction]")],
        ),
        {
            "entity": "metrics_sets",
            "mql_string": 'quantile(0.95)(duration){tags[release]:"1.2.3" AND tags[highway]:"401"} by (tags[transaction])',
        },
        None,
        id="groupby",
    ),
    pytest.param(
        timeseries(
            Metric("duration", entity="metrics_sets"),
            "quantile",
            [0.95],
            [
                Condition(Column("tags[release]"), Op.EQ, "1.2.3"),
                Condition(Column("tags[highway]"), Op.EQ, "401"),
            ],
            [Column("tags[transaction]"), Column("tags[device]")],
        ),
        {
            "entity": "metrics_sets",
            "mql_string": 'quantile(0.95)(duration){tags[release]:"1.2.3" AND tags[highway]:"401"} by (tags[transaction], tags[device])',
        },
        None,
        id="groupbys",
    ),
    pytest.param(
        timeseries(
            Metric("duration", entity="metrics_sets"),
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
        {
            "entity": "metrics_sets",
            "mql_string": 'quantile(0.95)(duration){tags[release]:"1.2.3" AND tags[highway]:"401"} by (tags[transaction] AS `transaction`, tags[device] AS `device`)',
        },
        None,
        id="aliased groupbys",
    ),
    pytest.param(
        timeseries(Metric("duration", entity="metrics_sets"), 456, None, None, None),
        None,
        InvalidTimeseriesError("aggregate must be a string"),
        id="invalid aggregate",
    ),
    pytest.param(
        timeseries(Metric("duration", entity="metrics_sets"), "count", 6, None, None),
        None,
        InvalidTimeseriesError("aggregate_params must be a list"),
        id="invalid aggregate param",
    ),
    pytest.param(
        timeseries(
            Metric("duration", entity="metrics_sets"),
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
        timeseries(Metric("duration", entity="metrics_sets"), "count", None, 6, None),
        None,
        InvalidTimeseriesError("filters must be a list"),
        id="invalid filter",
    ),
    pytest.param(
        timeseries(
            Metric("duration", entity="metrics_sets"),
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
        timeseries(Metric("duration", entity="metrics_sets"), "count", None, None, 3),
        None,
        InvalidTimeseriesError("groupby must be a list"),
        id="invalid groupby",
    ),
    pytest.param(
        timeseries(
            Metric("duration", entity="metrics_sets"),
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
    translated: Mapping[str, str] | None,
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

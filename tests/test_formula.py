from __future__ import annotations

import re
from typing import Any, Callable, Optional

import pytest

from snuba_sdk.column import Column
from snuba_sdk.conditions import Condition, Op
from snuba_sdk.formula import ArithmeticOperator, InvalidFormulaError
from snuba_sdk.metrics_visitors import FormulaSnQLVisitor
from snuba_sdk.timeseries import Metric, Timeseries
from tests import formula

tests = [
    # This might be possible in the future, but if the formula doesn't contain a metric
    # then it's not possible to infer the entity
    # pytest.param(
    #     formula(ArithmeticOperator.PLUS, [1, 1], None, None),
    #     None,
    #     id="basic formula test",
    # ),
    pytest.param(
        formula(
            ArithmeticOperator.PLUS,
            [
                Timeseries(
                    metric=Metric(public_name="foo", entity="metrics_sets"),
                    aggregate="sum",
                ),
                1,
            ],
            None,
            None,
        ),
        None,
        id="timeseries and number formula test",
    ),
    pytest.param(
        formula(
            ArithmeticOperator.PLUS,
            [
                Timeseries(
                    metric=Metric(public_name="foo", entity="metrics_sets"),
                    aggregate="sum",
                ),
                1,
            ],
            [Condition(Column("tags[transaction]"), Op.EQ, "foo")],
            None,
        ),
        None,
        id="filters in formula",
    ),
    pytest.param(
        formula(
            ArithmeticOperator.PLUS,
            [
                Timeseries(
                    metric=Metric(public_name="foo", entity="metrics_sets"),
                    aggregate="sum",
                ),
                1,
            ],
            None,
            [Column("tags[status_code]")],
        ),
        None,
        id="groupby in formula",
    ),
    pytest.param(
        formula(
            ArithmeticOperator.PLUS,
            [
                Timeseries(
                    metric=Metric(public_name="foo", entity="metrics_sets"),
                    aggregate="sum",
                ),
                Timeseries(
                    metric=Metric(public_name="bar", entity="metrics_sets"),
                    aggregate="sum",
                ),
            ],
            None,
            None,
        ),
        None,
        id="timeseries in formula",
    ),
    pytest.param(
        formula(
            42,
            [
                Timeseries(
                    metric=Metric(public_name="foo", entity="metrics_sets"),
                    aggregate="sum",
                ),
                1,
            ],
            None,
            None,
        ),
        InvalidFormulaError("formula '42' must be a ArithmeticOperator"),
        id="invalid operator type",
    ),
    pytest.param(
        formula(ArithmeticOperator.PLUS, 42, None, None),
        InvalidFormulaError("parameters of formula plus must be a Sequence"),
        id="invalid parameters",
    ),
    pytest.param(
        formula(
            ArithmeticOperator.MULTIPLY,
            [
                Timeseries(
                    metric=Metric(public_name="foo", entity="metrics_sets"),
                    aggregate="sum",
                ),
                "foo",
            ],
            None,
            None,
        ),
        InvalidFormulaError("parameter 'foo' of formula multiply is an invalid type"),
        id="unsupported parameter for operator",
    ),
    pytest.param(
        formula(
            ArithmeticOperator.PLUS,
            [
                Timeseries(
                    metric=Metric(
                        "transaction.duration",
                        "d:transactions/duration@millisecond",
                        1123,
                        "metrics_sets",
                    ),
                    aggregate="sum",
                    filters=[Condition(Column("tags[referrer]"), Op.EQ, "foo")],
                    groupby=[Column("tags[status_code]")],
                ),
                Timeseries(
                    metric=Metric(
                        "transaction.duration",
                        "d:transactions/duration@millisecond",
                        123,
                        "metrics_distributions",
                    ),
                    aggregate="sum",
                    filters=[Condition(Column("tags[referrer]"), Op.EQ, "bar")],
                    groupby=[Column("tags[status_code]")],
                ),
            ],
            [Condition(Column("tags[status_code]"), Op.EQ, 200)],
            [Column("tags[release]")],
        ),
        InvalidFormulaError("Formulas must operate on a single entity"),
        id="different entities",
    ),
    pytest.param(
        formula(
            ArithmeticOperator.PLUS,
            [
                Timeseries(
                    metric=Metric(
                        "transaction.duration",
                        "d:transactions/duration@millisecond",
                        1123,
                        "metrics_sets",
                    ),
                    aggregate="sum",
                    filters=[Condition(Column("tags[referrer]"), Op.EQ, "foo")],
                    groupby=[Column("tags[status_code]")],
                ),
                Timeseries(
                    metric=Metric(
                        "transaction.duration",
                        "d:transactions/duration@millisecond",
                        123,
                        "metrics_sets",
                    ),
                    aggregate="sum",
                    filters=[Condition(Column("tags[referrer]"), Op.EQ, "bar")],
                    groupby=[Column("tags[platform]")],
                ),
            ],
            [Condition(Column("tags[status_code]"), Op.EQ, 200)],
            [Column("tags[release]")],
        ),
        InvalidFormulaError("Formula parameters must group by the same columns"),
        id="different groupby",
    ),
    pytest.param(
        formula(
            ArithmeticOperator.PLUS,
            [100, 100],
            [Condition(Column("tags[status_code]"), Op.EQ, 200)],
            [Column("tags[release]")],
        ),
        InvalidFormulaError("Formulas must operate on a single entity"),
        id="no simple math",
    ),
]


@pytest.mark.parametrize("func_wrapper, exception", tests)
def test_formulas(
    func_wrapper: Callable[[], Any],
    exception: Optional[Exception],
) -> None:
    if exception is None:
        formula = func_wrapper()
        formula.validate()
    else:
        with pytest.raises(type(exception), match=re.escape(str(exception))):
            formula = func_wrapper()
            formula.validate()


formula_snql_tests = [
    pytest.param(
        formula(
            ArithmeticOperator.MULTIPLY,
            [
                Timeseries(
                    metric=Metric(
                        "transaction.duration",
                        "d:transactions/duration@millisecond",
                        1123,
                        "metrics_sets",
                    ),
                    aggregate="sum",
                    filters=[Condition(Column("tags[referrer]"), Op.EQ, "foo")],
                ),
                100,
            ],
            None,
            None,
        ),
        {
            "entity": "metrics_sets",
            "groupby": "",
            "aggregate": "multiply(sumIf(value, and(equals(metric_id, 1123), equals(tags[referrer], 'foo'))), 100) AS aggregate_value",
        },
        None,
        id="basic formula",
    ),
    pytest.param(
        formula(
            ArithmeticOperator.PLUS,
            [
                Timeseries(
                    metric=Metric(
                        "transaction.duration",
                        "d:transactions/duration@millisecond",
                        1123,
                        "metrics_sets",
                    ),
                    aggregate="sum",
                    filters=[Condition(Column("tags[referrer]"), Op.EQ, "foo")],
                ),
                Timeseries(
                    metric=Metric(
                        "transaction.duration",
                        "d:transactions/duration@millisecond",
                        123,
                        "metrics_sets",
                    ),
                    aggregate="sum",
                    filters=[Condition(Column("tags[referrer]"), Op.EQ, "bar")],
                ),
            ],
            None,
            None,
        ),
        {
            "entity": "metrics_sets",
            "groupby": "",
            "aggregate": "plus(sumIf(value, and(equals(metric_id, 1123), equals(tags[referrer], 'foo'))), sumIf(value, and(equals(metric_id, 123), equals(tags[referrer], 'bar')))) AS aggregate_value",
        },
        None,
        id="basic timeseries formula",
    ),
    pytest.param(
        formula(
            ArithmeticOperator.PLUS,
            [
                Timeseries(
                    metric=Metric(
                        "transaction.duration",
                        "d:transactions/duration@millisecond",
                        1123,
                        "metrics_sets",
                    ),
                    aggregate="sum",
                    filters=[Condition(Column("tags[referrer]"), Op.EQ, "foo")],
                ),
                Timeseries(
                    metric=Metric(
                        "transaction.duration",
                        "d:transactions/duration@millisecond",
                        123,
                        "metrics_sets",
                    ),
                    aggregate="sum",
                    filters=[Condition(Column("tags[referrer]"), Op.EQ, "bar")],
                ),
            ],
            [Condition(Column("tags[status_code]"), Op.EQ, 200)],
            None,
        ),
        {
            "entity": "metrics_sets",
            "groupby": "",
            "aggregate": "plus(sumIf(value, and(equals(metric_id, 1123), equals(tags[referrer], 'foo'), equals(tags[status_code], 200))), sumIf(value, and(equals(metric_id, 123), equals(tags[referrer], 'bar'), equals(tags[status_code], 200)))) AS aggregate_value",
        },
        None,
        id="formula with filters",
    ),
    pytest.param(
        formula(
            ArithmeticOperator.PLUS,
            [
                Timeseries(
                    metric=Metric(
                        "transaction.duration",
                        "d:transactions/duration@millisecond",
                        1123,
                        "metrics_sets",
                    ),
                    aggregate="sum",
                    filters=[Condition(Column("tags[referrer]"), Op.EQ, "foo")],
                    groupby=[Column("tags[status_code]")],
                ),
                Timeseries(
                    metric=Metric(
                        "transaction.duration",
                        "d:transactions/duration@millisecond",
                        123,
                        "metrics_sets",
                    ),
                    aggregate="sum",
                    filters=[Condition(Column("tags[referrer]"), Op.EQ, "bar")],
                    groupby=[Column("tags[status_code]")],
                ),
            ],
            [Condition(Column("tags[status_code]"), Op.EQ, 200)],
            [Column("tags[release]")],
        ),
        {
            "entity": "metrics_sets",
            "groupby": "tags[release], tags[status_code]",
            "aggregate": "plus(sumIf(value, and(equals(metric_id, 1123), equals(tags[referrer], 'foo'), equals(tags[status_code], 200))), sumIf(value, and(equals(metric_id, 123), equals(tags[referrer], 'bar'), equals(tags[status_code], 200)))) AS aggregate_value",
        },
        None,
        id="group bys",
    ),
]


TRANSLATOR = FormulaSnQLVisitor()


@pytest.mark.parametrize("formula_func, translated, exception", formula_snql_tests)
def test_formula_translate(
    formula_func: Callable[[], Any],
    translated: dict[str, str],
    exception: Optional[Exception],
) -> None:
    if exception is not None:
        with pytest.raises(type(exception), match=re.escape(str(exception))):
            formula_func()
    else:
        formula = formula_func()
        assert TRANSLATOR.visit(formula) == translated

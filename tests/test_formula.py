from __future__ import annotations

import re
from typing import Any, Callable, Optional

import pytest

from snuba_sdk.column import Column
from snuba_sdk.conditions import Condition, Op
from snuba_sdk.formula import ArithmeticOperator, InvalidFormulaError
from snuba_sdk.metrics_visitors import FormulaMQLPrinter
from snuba_sdk.timeseries import Metric, Timeseries
from tests import formula

tests = [
    # This might be possible in the future, but if the formula doesn't contain a metric
    # then it's not possible to infer the entity
    # pytest.param(
    #     formula(ArithmeticOperator.PLUS.value, [1, 1], None, None),
    #     None,
    #     id="basic formula test",
    # ),
    pytest.param(
        formula(
            ArithmeticOperator.PLUS.value,
            [
                Timeseries(
                    metric=Metric(public_name="foo"),
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
            ArithmeticOperator.PLUS.value,
            [
                Timeseries(
                    metric=Metric(public_name="foo"),
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
            ArithmeticOperator.PLUS.value,
            [
                Timeseries(
                    metric=Metric(public_name="foo"),
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
            ArithmeticOperator.PLUS.value,
            [
                Timeseries(
                    metric=Metric(public_name="foo"),
                    aggregate="sum",
                ),
                Timeseries(
                    metric=Metric(public_name="bar"),
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
            ArithmeticOperator.DIVIDE.value,
            [
                Timeseries(
                    metric=Metric(public_name="foo"),
                    aggregate="sum",
                    groupby=[Column("transaction")],
                ),
                Timeseries(
                    metric=Metric(public_name="foo"),
                    aggregate="avg",
                    groupby=[Column("transaction")],
                ),
            ],
        ),
        None,
        id="timeseries in formula have the same groupby",
    ),
    pytest.param(
        formula(
            42,
            [
                Timeseries(
                    metric=Metric(public_name="foo"),
                    aggregate="sum",
                ),
                1,
            ],
            None,
            None,
        ),
        InvalidFormulaError("formula '42' must be a str"),
        id="invalid operator type",
    ),
    pytest.param(
        formula(ArithmeticOperator.PLUS.value, 42, None, None),
        InvalidFormulaError("parameters of formula plus must be a Sequence"),
        id="invalid parameters",
    ),
    pytest.param(
        formula(
            ArithmeticOperator.MULTIPLY.value,
            [
                Timeseries(
                    metric=Metric(public_name="foo"),
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
            ArithmeticOperator.PLUS.value,
            [
                Timeseries(
                    metric=Metric(
                        "transaction.duration",
                        "d:transactions/duration@millisecond",
                        1123,
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
            ArithmeticOperator.PLUS.value,
            [100, 100],
            [Condition(Column("tags[status_code]"), Op.EQ, 200)],
            [Column("tags[release]")],
        ),
        InvalidFormulaError("Formulas must operate on at least one Timeseries"),
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


formula_mql_tests = [
    pytest.param(
        formula(
            ArithmeticOperator.MULTIPLY.value,
            [
                Timeseries(
                    metric=Metric(
                        "transaction.duration",
                        "d:transactions/duration@millisecond",
                        1123,
                    ),
                    aggregate="sum",
                    filters=[Condition(Column("tags[referrer]"), Op.EQ, "foo")],
                ),
                100,
            ],
            None,
            None,
        ),
        '(sum(d:transactions/duration@millisecond){tags[referrer]:"foo"} * 100)',
        None,
        id="basic formula",
    ),
    pytest.param(
        formula(
            ArithmeticOperator.PLUS.value,
            [
                Timeseries(
                    metric=Metric(
                        "transaction.duration",
                        "d:transactions/duration@millisecond",
                        1123,
                    ),
                    aggregate="sum",
                    filters=[Condition(Column("tags[referrer]"), Op.EQ, "foo")],
                ),
                Timeseries(
                    metric=Metric(
                        "transaction.duration",
                        "d:transactions/duration@millisecond",
                        123,
                    ),
                    aggregate="sum",
                    filters=[Condition(Column("tags[referrer]"), Op.EQ, "bar")],
                ),
            ],
            None,
            None,
        ),
        '(sum(d:transactions/duration@millisecond){tags[referrer]:"foo"} + sum(d:transactions/duration@millisecond){tags[referrer]:"bar"})',
        None,
        id="basic timeseries formula",
    ),
    pytest.param(
        formula(
            ArithmeticOperator.PLUS.value,
            [
                Timeseries(
                    metric=Metric(
                        "transaction.duration",
                        "d:transactions/duration@millisecond",
                        1123,
                    ),
                    aggregate="sum",
                    filters=[Condition(Column("tags[referrer]"), Op.EQ, "foo")],
                ),
                Timeseries(
                    metric=Metric(
                        "transaction.duration",
                        "d:transactions/duration@millisecond",
                        123,
                    ),
                    aggregate="sum",
                    filters=[Condition(Column("tags[referrer]"), Op.EQ, "bar")],
                ),
            ],
            [Condition(Column("tags[status_code]"), Op.EQ, 200)],
            None,
        ),
        '(sum(d:transactions/duration@millisecond){tags[referrer]:"foo"} + sum(d:transactions/duration@millisecond){tags[referrer]:"bar"}){tags[status_code]:200}',
        None,
        id="formula with filters",
    ),
    pytest.param(
        formula(
            ArithmeticOperator.PLUS.value,
            [
                Timeseries(
                    metric=Metric(
                        "transaction.duration",
                        "d:transactions/duration@millisecond",
                        1123,
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
                    ),
                    aggregate="sum",
                    filters=[Condition(Column("tags[referrer]"), Op.EQ, "bar")],
                    groupby=[Column("tags[status_code]")],
                ),
            ],
            [Condition(Column("tags[status_code]"), Op.EQ, 200)],
            [Column("tags[release]")],
        ),
        '(sum(d:transactions/duration@millisecond){tags[referrer]:"foo"} by (tags[status_code]) + sum(d:transactions/duration@millisecond){tags[referrer]:"bar"} by (tags[status_code])){tags[status_code]:200} by (tags[release])',
        None,
        id="group bys",
    ),
]


TRANSLATOR = FormulaMQLPrinter()


@pytest.mark.parametrize("formula_func, translated, exception", formula_mql_tests)
def test_formula_translate(
    formula_func: Callable[[], Any],
    translated: str,
    exception: Optional[Exception],
) -> None:
    if exception is not None:
        with pytest.raises(type(exception), match=re.escape(str(exception))):
            formula_func()
    else:
        formula = formula_func()
        assert TRANSLATOR.visit(formula) == translated, TRANSLATOR.visit(formula)

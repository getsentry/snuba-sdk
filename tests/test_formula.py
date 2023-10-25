import pytest
import re

from typing import Any, Callable, Optional

from snuba_sdk.column import Column
from snuba_sdk.conditions import Condition, Op
from snuba_sdk.formula import InvalidFormulaError
from snuba_sdk.timeseries import Metric, Timeseries
from tests import formula

tests = [
    pytest.param(
        formula("plus", [1, 1], None, None),
        None,
        id="basic formula test",
    ),
    pytest.param(
        formula("plus", [Timeseries(metric=Metric(public_name="foo"), aggregate="sum"), 1], None, None),
        None,
        id="timeseries and number formula test",
    ),
    pytest.param(
        formula("plus", [Timeseries(metric=Metric(public_name="foo"), aggregate="sum"), 1], [Condition(Column("tags[transaction]"), Op.EQ, "foo")], None),
        None,
        id="filters in formula",
    ),
    pytest.param(
        formula("plus", [Timeseries(metric=Metric(public_name="foo"), aggregate="sum"), 1], None, [Column("tags[status_code]")]),
        None,
        id="groupby in formula",
    ),
    pytest.param(
        formula("plus", [Timeseries(metric=Metric(public_name="foo"), aggregate="sum"), Timeseries(metric=Metric(public_name="bar"), aggregate="sum")], None, None),
        None,
        id="timeseries in formula",
    ),
    pytest.param(
        formula(42, [Timeseries(metric=Metric(public_name="foo"), aggregate="sum"), 1], None, None),
        InvalidFormulaError("formula '42' must be a string"),
        id="invalid operator type",
    ),
    pytest.param(
        formula("", [Timeseries(metric=Metric(public_name="foo"), aggregate="sum"), 1], None, None),
        InvalidFormulaError("operator cannot be empty"),
        id="empty operator",
    ),
    pytest.param(
        formula("impossible_operator", [Timeseries(metric=Metric(public_name="foo"), aggregate="sum"), 1], None, None),
        InvalidFormulaError("operator 'impossible_operator' is not supported"),
        id="unsupported operator",
    ),
    pytest.param(
        formula("plus", 42, None, None),
        InvalidFormulaError("parameters of formula plus must be a Sequence"),
        id="invalid parameters",
    ),
    pytest.param(
        formula("multiply", [Timeseries(metric=Metric(public_name="foo"), aggregate="sum"), "foo"], None, None),
        InvalidFormulaError("parameter 'foo' of formula multiply is an invalid type"),
        id="unsupported parameter for operator",
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

from __future__ import annotations

import pytest

from snuba_sdk.column import Column
from snuba_sdk.conditions import Condition, Op
from snuba_sdk.formula import ArithmeticOperator, Formula
from snuba_sdk.metrics_visitors import FormulaMQLPrinter
from snuba_sdk.mql.mql import parse_mql
from snuba_sdk.timeseries import Metric, Timeseries

formula_tests = [
    pytest.param(
        Formula(
            ArithmeticOperator.DIVIDE,
            [
                Timeseries(
                    metric=Metric(
                        public_name="foo", entity="generic_metrics_distributions"
                    ),
                    aggregate="sum",
                ),
                1000,
            ],
        ),
        "(sum(foo) / 1000)",
        id="test_terms",
    ),
    pytest.param(
        Formula(
            ArithmeticOperator.MULTIPLY,
            [
                Timeseries(
                    metric=Metric(
                        public_name="foo", entity="generic_metrics_distributions"
                    ),
                    aggregate="sum",
                ),
                Timeseries(
                    metric=Metric(
                        public_name="bar", entity="generic_metrics_distributions"
                    ),
                    aggregate="max",
                ),
            ],
        ),
        "(sum(foo) * max(bar))",
    ),
    pytest.param(
        Formula(
            ArithmeticOperator.DIVIDE,
            [
                Formula(
                    ArithmeticOperator.MULTIPLY,
                    [
                        Timeseries(
                            metric=Metric(
                                public_name="foo",
                                entity="generic_metrics_distributions",
                            ),
                            aggregate="sum",
                        ),
                        Timeseries(
                            metric=Metric(
                                public_name="bar",
                                entity="generic_metrics_distributions",
                            ),
                            aggregate="sum",
                        ),
                    ],
                ),
                1000.0,
            ],
        ),
        "((sum(foo) * sum(bar)) / 1000.0)",
    ),
    pytest.param(
        Formula(
            ArithmeticOperator.DIVIDE,
            [
                Timeseries(
                    metric=Metric(
                        public_name="foo", entity="generic_metrics_distributions"
                    ),
                    aggregate="sum",
                ),
                Timeseries(
                    metric=Metric(
                        public_name="bar", entity="generic_metrics_distributions"
                    ),
                    aggregate="sum",
                ),
            ],
            filters=[Condition(Column("tag"), Op.EQ, "tag_value")],
        ),
        '(sum(foo) / sum(bar)){tag:"tag_value"}',
    ),
    pytest.param(
        Formula(
            ArithmeticOperator.DIVIDE,
            [
                Timeseries(
                    metric=Metric(
                        public_name="foo", entity="generic_metrics_distributions"
                    ),
                    aggregate="sum",
                    filters=[Condition(Column("tag"), Op.EQ, "tag_value")],
                ),
                Timeseries(
                    metric=Metric(
                        public_name="bar", entity="generic_metrics_distributions"
                    ),
                    aggregate="sum",
                    filters=[Condition(Column("tag"), Op.EQ, "tag_value")],
                ),
            ],
        ),
        '(sum(foo){tag:"tag_value"} / sum(bar){tag:"tag_value"})',
    ),
    pytest.param(
        Formula(
            ArithmeticOperator.DIVIDE,
            [
                Timeseries(
                    metric=Metric(
                        public_name="foo", entity="generic_metrics_distributions"
                    ),
                    aggregate="sum",
                ),
                Timeseries(
                    metric=Metric(
                        public_name="bar", entity="generic_metrics_distributions"
                    ),
                    aggregate="sum",
                ),
            ],
            filters=[Condition(Column("tag"), Op.EQ, "tag_value")],
            groupby=[Column("transaction")],
        ),
        # '(sum(foo) / sum(bar)){tag:"tag_value"} by transaction',
        '(sum(foo) / sum(bar)){tag:"tag_value"} by (transaction)',
    ),
    pytest.param(
        Formula(
            ArithmeticOperator.DIVIDE,
            [
                Timeseries(
                    metric=Metric(
                        public_name="foo", entity="generic_metrics_distributions"
                    ),
                    aggregate="sum",
                    groupby=[Column("transaction")],
                ),
                Timeseries(
                    metric=Metric(
                        public_name="bar", entity="generic_metrics_distributions"
                    ),
                    aggregate="sum",
                    groupby=[Column("transaction")],
                ),
            ],
        ),
        "(sum(foo) by (transaction) / sum(bar) by (transaction))",
    ),
    pytest.param(
        Formula(
            ArithmeticOperator.DIVIDE,
            [
                Timeseries(
                    metric=Metric(
                        public_name="foo", entity="generic_metrics_distributions"
                    ),
                    aggregate="sum",
                    groupby=[Column("transaction")],
                ),
                Timeseries(
                    metric=Metric(
                        public_name="bar", entity="generic_metrics_distributions"
                    ),
                    aggregate="sum",
                    groupby=[Column("transaction")],
                ),
            ],
            filters=[Condition(Column("tag"), Op.EQ, "tag_value")],
        ),
        '(sum(foo) by (transaction) / sum(bar) by (transaction)){tag:"tag_value"}',
    ),
    pytest.param(
        Formula(
            ArithmeticOperator.DIVIDE,
            [
                Timeseries(
                    metric=Metric(
                        public_name="foo", entity="generic_metrics_distributions"
                    ),
                    aggregate="sum",
                    filters=[Condition(Column("tag"), Op.EQ, "tag_value")],
                    groupby=[Column("transaction")],
                ),
                Timeseries(
                    metric=Metric(
                        public_name="bar", entity="generic_metrics_distributions"
                    ),
                    aggregate="sum",
                    filters=[Condition(Column("tag"), Op.EQ, "tag_value")],
                    groupby=[Column("transaction")],
                ),
            ],
        ),
        '(sum(foo){tag:"tag_value"} by (transaction) / sum(bar){tag:"tag_value"} by (transaction))',
    ),
    pytest.param(
        Formula(
            ArithmeticOperator.DIVIDE,
            [
                Timeseries(
                    metric=Metric(
                        public_name="foo", entity="generic_metrics_distributions"
                    ),
                    aggregate="sum",
                    filters=[Condition(Column("tag"), Op.EQ, "tag_value")],
                    groupby=[Column("transaction")],
                ),
                Timeseries(
                    metric=Metric(
                        public_name="bar", entity="generic_metrics_distributions"
                    ),
                    aggregate="sum",
                    filters=[Condition(Column("tag"), Op.EQ, "tag_value")],
                    groupby=[Column("transaction")],
                ),
            ],
        ),
        '(sum(foo){tag:"tag_value"} by (transaction) / sum(bar){tag:"tag_value"} by (transaction))',
    ),
    pytest.param(
        Formula(
            ArithmeticOperator.DIVIDE,
            [
                Timeseries(
                    metric=Metric(
                        public_name="foo", entity="generic_metrics_distributions"
                    ),
                    aggregate="sum",
                ),
                Timeseries(
                    metric=Metric(
                        public_name="bar", entity="generic_metrics_distributions"
                    ),
                    aggregate="sum",
                ),
            ],
            filters=[Condition(Column("tag"), Op.EQ, "tag_value")],
            groupby=[Column("transaction")],
        ),
        '(sum(foo) / sum(bar)){tag:"tag_value"} by (transaction)',
    ),
    pytest.param(
        Formula(
            operator=ArithmeticOperator.MULTIPLY,
            parameters=[
                Formula(
                    ArithmeticOperator.DIVIDE,
                    [
                        Timeseries(
                            metric=Metric(
                                public_name="foo",
                                entity="generic_metrics_distributions",
                            ),
                            aggregate="sum",
                            filters=[
                                Condition(Column("tag2"), Op.EQ, "tag_value2"),
                                Condition(Column("tag"), Op.EQ, "tag_value"),
                            ],
                        ),
                        Timeseries(
                            metric=Metric(
                                public_name="bar",
                                entity="generic_metrics_distributions",
                            ),
                            aggregate="sum",
                        ),
                    ],
                    filters=[Condition(Column("tag3"), Op.EQ, "tag_value3")],
                ),
                Timeseries(
                    metric=Metric(
                        public_name="pop", entity="generic_metrics_distributions"
                    ),
                    aggregate="sum",
                ),
            ],
            groupby=[Column("transaction")],
        ),
        '((sum(foo){tag2:"tag_value2" AND tag:"tag_value"} / sum(bar)){tag3:"tag_value3"} * sum(pop)) by (transaction)',
    ),
]

FORMULA_PRINTER = FormulaMQLPrinter()


@pytest.mark.parametrize("formula, mql", formula_tests)
def test_metrics_query_to_mql_formula(formula: Formula, mql: str) -> None:
    output = FORMULA_PRINTER.visit(formula)
    assert output["mql_string"] == mql

    # TODO: We can't simply assert the whole query, because we need an Entity in order to serialize the formula,
    # but when we parse the MQL the entity is None. Once SnQL support is removed, we can change this.
    # assert parse_mql(output["mql_string"]).query == formula
    parsed = parse_mql(output["mql_string"])
    assert parsed.query is not None
    assert parsed.query.groupby == formula.groupby
    assert parsed.query.filters == formula.filters

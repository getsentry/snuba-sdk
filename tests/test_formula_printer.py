from __future__ import annotations

import pytest

from snuba_sdk.column import Column
from snuba_sdk.conditions import Condition, Op, Or
from snuba_sdk.formula import ArithmeticOperator, Formula
from snuba_sdk.metrics_visitors import FormulaMQLPrinter
from snuba_sdk.mql.mql import parse_mql
from snuba_sdk.timeseries import Metric, Timeseries

formula_tests = [
    pytest.param(
        Formula(
            ArithmeticOperator.DIVIDE.value,
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
            ArithmeticOperator.MULTIPLY.value,
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
        id="test two aggregate terms",
    ),
    pytest.param(
        Formula(
            ArithmeticOperator.DIVIDE.value,
            [
                Formula(
                    ArithmeticOperator.MULTIPLY.value,
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
        id="test nested terms",
    ),
    pytest.param(
        Formula(
            ArithmeticOperator.DIVIDE.value,
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
        id="test terms with filter",
    ),
    pytest.param(
        Formula(
            ArithmeticOperator.DIVIDE.value,
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
        id="test terms with filters",
    ),
    pytest.param(
        Formula(
            ArithmeticOperator.DIVIDE.value,
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
        id="test terms with filters and groupby",
    ),
    pytest.param(
        Formula(
            ArithmeticOperator.DIVIDE.value,
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
        id="test terms with filters and groupbys",
    ),
    pytest.param(
        Formula(
            ArithmeticOperator.DIVIDE.value,
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
        id="test terms with inner groupbys and outer filter",
    ),
    pytest.param(
        Formula(
            ArithmeticOperator.DIVIDE.value,
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
        id="test terms with inner groupbys and inner filter",
    ),
    pytest.param(
        Formula(
            ArithmeticOperator.DIVIDE.value,
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
        id="test terms with outer groupbys and outer filter",
    ),
    pytest.param(
        Formula(
            function_name=ArithmeticOperator.MULTIPLY.value,
            parameters=[
                Formula(
                    ArithmeticOperator.DIVIDE.value,
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
        id="test complex nested terms",
    ),
    pytest.param(
        Formula(
            function_name=ArithmeticOperator.MULTIPLY.value,
            parameters=[
                Formula(
                    ArithmeticOperator.DIVIDE.value,
                    [
                        Timeseries(
                            metric=Metric(
                                public_name="foo",
                                entity="generic_metrics_distributions",
                            ),
                            aggregate="sum",
                            filters=[
                                Condition(Column("tag2"), Op.EQ, "tag_value2"),
                                Or(
                                    [
                                        Condition(Column("tag"), Op.EQ, "tag_value"),
                                        Condition(Column("tag"), Op.EQ, "tag_valueor"),
                                    ]
                                ),
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
        '((sum(foo){tag2:"tag_value2" AND (tag:"tag_value" OR tag:"tag_valueor")} / sum(bar)){tag3:"tag_value3"} * sum(pop)) by (transaction)',
        id="test complex nested terms with OR condition",
    ),
    pytest.param(
        Formula(
            function_name="apdex",
            parameters=[
                Timeseries(
                    metric=Metric(
                        public_name="foo",
                        entity="generic_metrics_distributions",
                    ),
                    aggregate="sum",
                ),
                500,
            ],
            filters=[Condition(Column("tag"), Op.EQ, "tag_value")],
            groupby=[Column("transaction")],
        ),
        'apdex(sum(foo), 500){tag:"tag_value"} by (transaction)',
        id="test arbitrary function with filters and groupby",
    ),
    pytest.param(
        Formula(
            function_name="apdex",
            parameters=[
                Formula(
                    function_name=ArithmeticOperator.DIVIDE.value,
                    parameters=[
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
                500,
            ],
            filters=[Condition(Column("tag"), Op.EQ, "tag_value")],
            groupby=[Column("transaction")],
        ),
        'apdex((sum(foo) / sum(bar)), 500){tag:"tag_value"} by (transaction)',
        id="test arbitrary function with filters, groupby, and terms",
    ),
    pytest.param(
        Formula(
            function_name="topK",
            parameters=[
                Timeseries(
                    metric=Metric(
                        public_name="foo",
                        entity="generic_metrics_distributions",
                    ),
                    aggregate="sum",
                ),
                500,
                "random",
                "test",
                4.2,
            ],
            filters=[Condition(Column("tag"), Op.EQ, "tag_value")],
            groupby=[Column("transaction")],
        ),
        'topK(sum(foo), 500, random, test, 4.2){tag:"tag_value"} by (transaction)',
        id="test arbitrary function with filters, groupby, and terms",
    ),
    pytest.param(
        Formula(
            function_name="multiply",
            parameters=[
                Formula(
                    "apdex",
                    [
                        Timeseries(
                            metric=Metric(
                                public_name="foo",
                                entity="generic_metrics_distributions",
                            ),
                            aggregate="sum",
                        ),
                        500,
                    ],
                ),
                Formula(
                    "apdex",
                    [
                        Timeseries(
                            metric=Metric(
                                public_name="foo",
                                entity="generic_metrics_distributions",
                            ),
                            aggregate="sum",
                        ),
                        400,
                    ],
                ),
            ],
            filters=[Condition(Column("tag"), Op.EQ, "tag_value")],
            groupby=[Column("transaction")],
        ),
        '(apdex(sum(foo), 500) * apdex(sum(foo), 400)){tag:"tag_value"} by (transaction)',
        id="test arbitrary functions",
    ),
    pytest.param(
        Formula(
            "apdex",
            [
                Timeseries(
                    metric=Metric(
                        public_name="foo",
                        entity="generic_metrics_distributions",
                    ),
                    aggregate="quantiles",
                    aggregate_params=[0.5],
                ),
                500,
            ],
            filters=[Condition(Column("tag"), Op.EQ, "tag_value")],
            groupby=[Column("transaction")],
        ),
        'apdex(quantiles(0.5)(foo), 500){tag:"tag_value"} by (transaction)',
        id="test arbitrary functions",
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

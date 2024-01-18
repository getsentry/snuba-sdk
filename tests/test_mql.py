from __future__ import annotations

import pytest

from snuba_sdk.column import Column
from snuba_sdk.conditions import And, Condition, Op, Or
from snuba_sdk.formula import ArithmeticOperator, Formula
from snuba_sdk.metrics_query import MetricsQuery
from snuba_sdk.mql.mql import parse_mql
from snuba_sdk.timeseries import Metric, Timeseries

base_tests = [
    pytest.param(
        "sum(`d:transactions/duration@millisecond`)",
        MetricsQuery(
            query=Timeseries(
                metric=Metric(mri="d:transactions/duration@millisecond"),
                aggregate="sum",
            )
        ),
        id="test quoted mri name",
    ),
    pytest.param(
        "sum(d:transactions/duration@millisecond)",
        MetricsQuery(
            query=Timeseries(
                metric=Metric(mri="d:transactions/duration@millisecond"),
                aggregate="sum",
            )
        ),
        id="test unquoted mri name",
    ),
    pytest.param(
        "sum(`transactions.duration`)",
        MetricsQuery(
            query=Timeseries(
                metric=Metric(public_name="transactions.duration"), aggregate="sum"
            )
        ),
        id="test quoted public name 1",
    ),
    pytest.param(
        "sum(`foo`)",
        MetricsQuery(
            query=Timeseries(metric=Metric(public_name="foo"), aggregate="sum")
        ),
        id="test quoted public name 2",
    ),
    pytest.param(
        "sum(transactions.duration)",
        MetricsQuery(
            query=Timeseries(
                metric=Metric(public_name="transactions.duration"), aggregate="sum"
            )
        ),
        id="test unquoted public name 1",
    ),
    pytest.param(
        "sum(foo)",
        MetricsQuery(
            query=Timeseries(metric=Metric(public_name="foo"), aggregate="sum")
        ),
        id="test unquoted public name 1",
    ),
    pytest.param(
        "(sum(foo))",
        MetricsQuery(
            query=Timeseries(metric=Metric(public_name="foo"), aggregate="sum")
        ),
        id="test nested expressions 1",
    ),
    pytest.param(
        "(sum(foo))",
        MetricsQuery(
            query=Timeseries(metric=Metric(public_name="foo"), aggregate="sum")
        ),
        id="test nested expressions 2",
    ),
    pytest.param(
        'sum(foo){bar:"baz"}',
        MetricsQuery(
            query=Timeseries(
                metric=Metric(public_name="foo"),
                aggregate="sum",
                filters=[Condition(Column("bar"), Op.EQ, "baz")],
            )
        ),
        id="test filter",
    ),
    pytest.param(
        "sum(foo){}",
        MetricsQuery(
            query=Timeseries(
                metric=Metric(public_name="foo"),
                aggregate="sum",
            )
        ),
        id="test empty filter",
    ),
    pytest.param(
        "sum(foo){bar:baz}",
        MetricsQuery(
            query=Timeseries(
                metric=Metric(public_name="foo"),
                aggregate="sum",
                filters=[Condition(Column("bar"), Op.EQ, "baz")],
            )
        ),
        id="test filter with unquoted value",
    ),
    pytest.param(
        'sum(foo){bar:"2023-01-03T10:00:00"}',
        MetricsQuery(
            query=Timeseries(
                metric=Metric(public_name="foo"),
                aggregate="sum",
                filters=[Condition(Column("bar"), Op.EQ, "2023-01-03T10:00:00")],
            )
        ),
        id="test filter with quoted value with special characters",
    ),
    pytest.param(
        "sum(foo){bar:2023-01-03T10:00:00}",
        MetricsQuery(
            query=Timeseries(
                metric=Metric(public_name="foo"),
                aggregate="sum",
                filters=[Condition(Column("bar"), Op.EQ, "2023-01-03T10:00:00")],
            )
        ),
        id="test filter with unquoted value with special characters",
    ),
    pytest.param(
        'sum(foo){!bar:"baz"}',
        MetricsQuery(
            query=Timeseries(
                metric=Metric(public_name="foo"),
                aggregate="sum",
                filters=[Condition(Column("bar"), Op.NEQ, "baz")],
            )
        ),
        id="test not filter",
    ),
    pytest.param(
        "sum(foo){!bar:baz}",
        MetricsQuery(
            query=Timeseries(
                metric=Metric(public_name="foo"),
                aggregate="sum",
                filters=[Condition(Column("bar"), Op.NEQ, "baz")],
            )
        ),
        id="test not filter with unquoted value",
    ),
    pytest.param(
        'sum(foo){bar:["baz", "bap"]}',
        MetricsQuery(
            query=Timeseries(
                metric=Metric(public_name="foo"),
                aggregate="sum",
                filters=[Condition(Column("bar"), Op.IN, ["baz", "bap"])],
            )
        ),
        id="test in filter",
    ),
    pytest.param(
        'sum(foo){bar:["baz", bap]}',
        MetricsQuery(
            query=Timeseries(
                metric=Metric(public_name="foo"),
                aggregate="sum",
                filters=[Condition(Column("bar"), Op.IN, ["baz", "bap"])],
            )
        ),
        id="test in filter with unquoted values",
    ),
    pytest.param(
        "sum(foo){bar:[baz, bap]}",
        MetricsQuery(
            query=Timeseries(
                metric=Metric(public_name="foo"),
                aggregate="sum",
                filters=[Condition(Column("bar"), Op.IN, ["baz", "bap"])],
            )
        ),
        id="test in filter with quoted and unquoted values",
    ),
    pytest.param(
        'sum(foo){!bar:["baz", "bap"]}',
        MetricsQuery(
            query=Timeseries(
                metric=Metric(public_name="foo"),
                aggregate="sum",
                filters=[Condition(Column("bar"), Op.NOT_IN, ["baz", "bap"])],
            )
        ),
        id="test not in filter",
    ),
    pytest.param(
        "sum(foo){!bar:[baz, bap]}",
        MetricsQuery(
            query=Timeseries(
                metric=Metric(public_name="foo"),
                aggregate="sum",
                filters=[Condition(Column("bar"), Op.NOT_IN, ["baz", "bap"])],
            )
        ),
        id="test not in filter with unquoted values",
    ),
    pytest.param(
        'sum(foo){!bar:["baz", bap]}',
        MetricsQuery(
            query=Timeseries(
                metric=Metric(public_name="foo"),
                aggregate="sum",
                filters=[Condition(Column("bar"), Op.NOT_IN, ["baz", "bap"])],
            )
        ),
        id="test not in filter with quoted and unquoted values",
    ),
    pytest.param(
        'sum(foo{bar:"baz"})',
        MetricsQuery(
            query=Timeseries(
                metric=Metric(public_name="foo"),
                aggregate="sum",
                filters=[Condition(Column("bar"), Op.EQ, "baz")],
            )
        ),
        id="test filter inside aggregate",
    ),
    pytest.param(
        "sum(foo{bar:baz})",
        MetricsQuery(
            query=Timeseries(
                metric=Metric(public_name="foo"),
                aggregate="sum",
                filters=[Condition(Column("bar"), Op.EQ, "baz")],
            )
        ),
        id="test filter inside aggregate with unquoted value",
    ),
    pytest.param(
        'sum(user{bar:"baz", foo:"foz"})',
        MetricsQuery(
            query=Timeseries(
                metric=Metric(public_name="user"),
                aggregate="sum",
                filters=[
                    And(
                        conditions=[
                            Condition(Column("bar"), Op.EQ, "baz"),
                            Condition(Column("foo"), Op.EQ, "foz"),
                        ],
                    )
                ],
            )
        ),
        id="test multiple filters",
    ),
    pytest.param(
        'sum(user{bar:"baz" foo:"foz"})',
        MetricsQuery(
            query=Timeseries(
                metric=Metric(public_name="user"),
                aggregate="sum",
                filters=[
                    And(
                        conditions=[
                            Condition(Column("bar"), Op.EQ, "baz"),
                            Condition(Column("foo"), Op.EQ, "foz"),
                        ]
                    )
                ],
            )
        ),
        id="test multiple filters with space delimiter",
    ),
    pytest.param(
        'sum(user{bar:"baz" AND foo:"foz"})',
        MetricsQuery(
            query=Timeseries(
                metric=Metric(public_name="user"),
                aggregate="sum",
                filters=[
                    And(
                        conditions=[
                            Condition(Column("bar"), Op.EQ, "baz"),
                            Condition(Column("foo"), Op.EQ, "foz"),
                        ]
                    )
                ],
            )
        ),
        id="test multiple filters with AND operator",
    ),
    pytest.param(
        'sum(user{bar:"baz" OR foo:"foz" AND (hee:"haw")})',
        MetricsQuery(
            query=Timeseries(
                metric=Metric(public_name="user"),
                aggregate="sum",
                filters=[
                    Or(
                        conditions=[
                            Condition(Column("bar"), Op.EQ, "baz"),
                            And(
                                conditions=[
                                    Condition(Column("foo"), Op.EQ, "foz"),
                                    Condition(Column("hee"), Op.EQ, "haw"),
                                ]
                            ),
                        ],
                    ),
                ],
            )
        ),
        id="test multiple filters with AND and OR operators and no parentheses",
    ),
    pytest.param(
        'sum(user{(bar:"baz" OR foo:"foz") AND hee:"haw"})',
        MetricsQuery(
            query=Timeseries(
                metric=Metric(public_name="user"),
                aggregate="sum",
                filters=[
                    And(
                        conditions=[
                            Or(
                                conditions=[
                                    Condition(Column("bar"), Op.EQ, "baz"),
                                    Condition(Column("foo"), Op.EQ, "foz"),
                                ],
                            ),
                            Condition(Column("hee"), Op.EQ, "haw"),
                        ]
                    )
                ],
            )
        ),
        id="test multiple filters with AND and OR operators",
    ),
    pytest.param(
        'sum(user{bar:"baz" foo:"foz", hee:"haw" AND key:"value"})',
        MetricsQuery(
            query=Timeseries(
                metric=Metric(public_name="user"),
                aggregate="sum",
                filters=[
                    And(
                        conditions=[
                            Condition(Column("bar"), Op.EQ, "baz"),
                            Condition(Column("foo"), Op.EQ, "foz"),
                            Condition(Column("hee"), Op.EQ, "haw"),
                            Condition(Column("key"), Op.EQ, "value"),
                        ]
                    )
                ],
            )
        ),
        id="test multiple filters with space and comma delimiter",
    ),
    pytest.param(
        "sum(user{bar:baz, foo:foz})",
        MetricsQuery(
            query=Timeseries(
                metric=Metric(public_name="user"),
                aggregate="sum",
                filters=[
                    And(
                        conditions=[
                            Condition(Column("bar"), Op.EQ, "baz"),
                            Condition(Column("foo"), Op.EQ, "foz"),
                        ]
                    )
                ],
            )
        ),
        id="test multiple filters with unquoted values",
    ),
    pytest.param(
        "sum(user{bar:baz foo:foz})",
        MetricsQuery(
            query=Timeseries(
                metric=Metric(public_name="user"),
                aggregate="sum",
                filters=[
                    And(
                        conditions=[
                            Condition(Column("bar"), Op.EQ, "baz"),
                            Condition(Column("foo"), Op.EQ, "foz"),
                        ]
                    )
                ],
            )
        ),
        id="test multiple filters with unquoted values with space delimiter",
    ),
    pytest.param(
        "sum(user{bar:baz foo:foz, hee:haw})",
        MetricsQuery(
            query=Timeseries(
                metric=Metric(public_name="user"),
                aggregate="sum",
                filters=[
                    And(
                        conditions=[
                            Condition(Column("bar"), Op.EQ, "baz"),
                            Condition(Column("foo"), Op.EQ, "foz"),
                            Condition(Column("hee"), Op.EQ, "haw"),
                        ]
                    )
                ],
            )
        ),
        id="test multiple filters with unquoted values with space and comma delimiter",
    ),
    pytest.param(
        'sum(user{bar:"baz", foo:foz})',
        MetricsQuery(
            query=Timeseries(
                metric=Metric(public_name="user"),
                aggregate="sum",
                filters=[
                    And(
                        conditions=[
                            Condition(Column("bar"), Op.EQ, "baz"),
                            Condition(Column("foo"), Op.EQ, "foz"),
                        ]
                    )
                ],
            )
        ),
        id="test multiple filters with quoted and unquoted values",
    ),
    pytest.param(
        'sum(user{bar:"baz" foo:foz})',
        MetricsQuery(
            query=Timeseries(
                metric=Metric(public_name="user"),
                aggregate="sum",
                filters=[
                    And(
                        conditions=[
                            Condition(Column("bar"), Op.EQ, "baz"),
                            Condition(Column("foo"), Op.EQ, "foz"),
                        ]
                    )
                ],
            )
        ),
        id="test multiple filters with quoted and unquoted values with space delimiter",
    ),
    pytest.param(
        'sum(user{bar:"baz" foo:foz, hee:"haw"})',
        MetricsQuery(
            query=Timeseries(
                metric=Metric(public_name="user"),
                aggregate="sum",
                filters=[
                    And(
                        conditions=[
                            Condition(Column("bar"), Op.EQ, "baz"),
                            Condition(Column("foo"), Op.EQ, "foz"),
                            Condition(Column("hee"), Op.EQ, "haw"),
                        ]
                    )
                ],
            )
        ),
        id="test multiple filters with quoted and unquoted values with space and comma delimiter",
    ),
    pytest.param(
        'sum(user{bar:baz foo:"foz", !hee:["haw", hoo]})',
        MetricsQuery(
            query=Timeseries(
                metric=Metric(public_name="user"),
                aggregate="sum",
                filters=[
                    And(
                        conditions=[
                            Condition(Column("bar"), Op.EQ, "baz"),
                            Condition(Column("foo"), Op.EQ, "foz"),
                            Condition(Column("hee"), Op.NOT_IN, ["haw", "hoo"]),
                        ]
                    )
                ],
            )
        ),
        id="test complex filters",
    ),
    pytest.param(
        'sum(`d:transactions/duration@millisecond`{foo:"foz", hee:"haw"}){bar:"baz"}',
        MetricsQuery(
            query=Timeseries(
                metric=Metric(mri="d:transactions/duration@millisecond"),
                aggregate="sum",
                filters=[
                    Condition(Column("bar"), Op.EQ, "baz"),
                    And(
                        conditions=[
                            Condition(Column("foo"), Op.EQ, "foz"),
                            Condition(Column("hee"), Op.EQ, "haw"),
                        ]
                    ),
                ],
            ),
        ),
        id="test multiple layer filters",
    ),
    pytest.param(
        'max(`d:transactions/duration@millisecond`{foo:"foz"}) by transaction',
        MetricsQuery(
            query=Timeseries(
                metric=Metric(mri="d:transactions/duration@millisecond"),
                aggregate="max",
                filters=[Condition(Column("foo"), Op.EQ, "foz")],
                groupby=[Column("transaction")],
            )
        ),
        id="test group by 1",
    ),
    pytest.param(
        "max(`d:transactions/duration@millisecond`{transaction.status:foz} by http.status_code)",
        MetricsQuery(
            query=Timeseries(
                metric=Metric(mri="d:transactions/duration@millisecond"),
                aggregate="max",
                filters=[Condition(Column("transaction.status"), Op.EQ, "foz")],
                groupby=[Column("http.status_code")],
            ),
        ),
        id="test group by 2",
    ),
    pytest.param(
        'max(`d:transactions/duration@millisecond`{transaction.status:"foz"}) by (transaction)',
        MetricsQuery(
            query=Timeseries(
                metric=Metric(mri="d:transactions/duration@millisecond"),
                aggregate="max",
                filters=[Condition(Column("transaction.status"), Op.EQ, "foz")],
                groupby=[Column("transaction")],
            ),
        ),
        id="test group by 3",
    ),
    pytest.param(
        'max(`d:transactions/duration@millisecond`{transaction.status:"foz"}){transaction.op:baz} by (a.something, b.something)',
        MetricsQuery(
            query=Timeseries(
                metric=Metric(mri="d:transactions/duration@millisecond"),
                aggregate="max",
                filters=[
                    Condition(Column("transaction.op"), Op.EQ, "baz"),
                    Condition(Column("transaction.status"), Op.EQ, "foz"),
                ],
                groupby=[Column("a.something"), Column("b.something")],
            )
        ),
        id="test group by 4",
    ),
    pytest.param(
        "p90(`d:transactions/duration@millisecond`)",
        MetricsQuery(
            query=Timeseries(
                metric=Metric(mri="d:transactions/duration@millisecond"),
                aggregate="p90",
            )
        ),
        id="test percentile function",
    ),
    pytest.param(
        "quantiles(0.5)(`d:transactions/duration@millisecond`)",
        MetricsQuery(
            query=Timeseries(
                metric=Metric(mri="d:transactions/duration@millisecond"),
                aggregate="quantiles",
                aggregate_params=[0.5],
            )
        ),
        id="test curried functions",
    ),
    pytest.param(
        "quantiles(0.5, 0.95)(`d:transactions/duration@millisecond`)",
        MetricsQuery(
            query=Timeseries(
                metric=Metric(mri="d:transactions/duration@millisecond"),
                aggregate="quantiles",
                aggregate_params=[0.5, 0.95],
            )
        ),
        id="test curried functions with multiple params",
    ),
    pytest.param(
        "quantiles()(`d:transactions/duration@millisecond`)",
        MetricsQuery(
            query=Timeseries(
                metric=Metric(mri="d:transactions/duration@millisecond"),
                aggregate="quantiles",
                aggregate_params=[],
            )
        ),
        id="test curried functions with no params",
    ),
    pytest.param(
        'quantiles(0.5, "random", other, 9)(`d:transactions/duration@millisecond`)',
        MetricsQuery(
            query=Timeseries(
                metric=Metric(mri="d:transactions/duration@millisecond"),
                aggregate="quantiles",
                aggregate_params=[0.5, "random", "other", 9],
            )
        ),
        id="test curried functions with random params",
    ),
    pytest.param(
        "quantiles(0.5)(`d:transactions/duration@millisecond`{})",
        MetricsQuery(
            query=Timeseries(
                metric=Metric(mri="d:transactions/duration@millisecond"),
                aggregate="quantiles",
                aggregate_params=[0.5],
            )
        ),
        id="test curried functions with empty filter",
    ),
    pytest.param(
        'quantiles(0.5)(`d:transactions/duration@millisecond`{foo:"foz"}){bar:baz} by (a, b)',
        MetricsQuery(
            query=Timeseries(
                metric=Metric(mri="d:transactions/duration@millisecond"),
                aggregate="quantiles",
                aggregate_params=[0.5],
                filters=[
                    Condition(Column("bar"), Op.EQ, "baz"),
                    Condition(Column("foo"), Op.EQ, "foz"),
                ],
                groupby=[Column("a"), Column("b")],
            )
        ),
        id="test curried functions with filters and group by",
    ),
    pytest.param(
        "quantiles(0.5)(`d:transactions/duration@millisecond`{foo:'foz' AND hee:\"hoo\"}){bar:baz} by (a, b)",
        MetricsQuery(
            query=Timeseries(
                metric=Metric(mri="d:transactions/duration@millisecond"),
                aggregate="quantiles",
                aggregate_params=[0.5],
                filters=[
                    Condition(Column("bar"), Op.EQ, "baz"),
                    And(
                        [
                            Condition(Column("foo"), Op.EQ, "'foz'"),
                            Condition(Column("hee"), Op.EQ, "hoo"),
                        ]
                    ),
                ],
                groupby=[Column("a"), Column("b")],
            )
        ),
        id="test quotes parsing",
    ),
    pytest.param(
        'max(d:transactions/duration@millisecond){bar:" !\\"#$%&\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"} by (transaction)',
        MetricsQuery(
            query=Timeseries(
                metric=Metric(mri="d:transactions/duration@millisecond"),
                aggregate="max",
                filters=[
                    Condition(
                        Column("bar"),
                        Op.EQ,
                        " !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\\\]^_`abcdefghijklmnopqrstuvwxyz{|}~",
                    ),
                ],
                groupby=[Column("transaction")],
            )
        ),
        id="test terms with crazy characters",
    ),
]


@pytest.mark.parametrize("mql_string, metrics_query", base_tests)
def test_parse_mql_base(mql_string: str, metrics_query: MetricsQuery) -> None:
    result = parse_mql(mql_string)
    assert result == metrics_query


term_tests = [
    pytest.param(
        "sum(foo) / 1000",
        MetricsQuery(
            query=Formula(
                ArithmeticOperator.DIVIDE.value,
                [
                    Timeseries(
                        metric=Metric(public_name="foo"),
                        aggregate="sum",
                    ),
                    1000.0,
                ],
            )
        ),
        id="test terms with number",
    ),
    pytest.param(
        "sum(foo) * max(bar)",
        MetricsQuery(
            query=Formula(
                ArithmeticOperator.MULTIPLY.value,
                [
                    Timeseries(
                        metric=Metric(public_name="foo"),
                        aggregate="sum",
                    ),
                    Timeseries(
                        metric=Metric(public_name="bar"),
                        aggregate="max",
                    ),
                ],
            )
        ),
        id="test terms with both aggregates",
    ),
    pytest.param(
        "(sum(foo) * sum(bar)) / 1000",
        MetricsQuery(
            query=Formula(
                ArithmeticOperator.DIVIDE.value,
                [
                    Formula(
                        ArithmeticOperator.MULTIPLY.value,
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
                    ),
                    1000.0,
                ],
            )
        ),
        id="test multi terms",
    ),
    pytest.param(
        '(sum(foo) / sum(bar)){tag:"tag_value"}',
        MetricsQuery(
            query=Formula(
                ArithmeticOperator.DIVIDE.value,
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
                filters=[Condition(Column("tag"), Op.EQ, "tag_value")],
            ),
        ),
        id="test terms with one filter",
    ),
    pytest.param(
        'sum(foo{tag:"tag_value"}) / sum(bar{tag:"tag_value"})',
        MetricsQuery(
            query=Formula(
                ArithmeticOperator.DIVIDE.value,
                [
                    Timeseries(
                        metric=Metric(public_name="foo"),
                        aggregate="sum",
                        filters=[Condition(Column("tag"), Op.EQ, "tag_value")],
                    ),
                    Timeseries(
                        metric=Metric(public_name="bar"),
                        aggregate="sum",
                        filters=[Condition(Column("tag"), Op.EQ, "tag_value")],
                    ),
                ],
            ),
        ),
        id="test terms with two filters",
    ),
    pytest.param(
        '(sum(foo) / sum(bar)){tag:"tag_value"} by transaction',
        MetricsQuery(
            query=Formula(
                ArithmeticOperator.DIVIDE.value,
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
                filters=[Condition(Column("tag"), Op.EQ, "tag_value")],
                groupby=[Column("transaction")],
            ),
        ),
        id="test terms with groupby 1",
    ),
    pytest.param(
        "(sum(foo) by transaction / sum(bar) by transaction)",
        MetricsQuery(
            query=Formula(
                ArithmeticOperator.DIVIDE.value,
                [
                    Timeseries(
                        metric=Metric(public_name="foo"),
                        aggregate="sum",
                        groupby=[Column("transaction")],
                    ),
                    Timeseries(
                        metric=Metric(public_name="bar"),
                        aggregate="sum",
                        groupby=[Column("transaction")],
                    ),
                ],
            ),
        ),
        id="test terms with groupby 2",
    ),
    pytest.param(
        '(sum(foo) by transaction / sum(bar) by transaction){tag:"tag_value"}',
        MetricsQuery(
            query=Formula(
                ArithmeticOperator.DIVIDE.value,
                [
                    Timeseries(
                        metric=Metric(public_name="foo"),
                        aggregate="sum",
                        groupby=[Column("transaction")],
                    ),
                    Timeseries(
                        metric=Metric(public_name="bar"),
                        aggregate="sum",
                        groupby=[Column("transaction")],
                    ),
                ],
                filters=[Condition(Column("tag"), Op.EQ, "tag_value")],
            ),
        ),
        id="test terms with groupby 3",
    ),
    pytest.param(
        '(sum(foo{tag:"tag_value"}) by transaction) / (sum(bar{tag:"tag_value"}) by transaction)',
        MetricsQuery(
            query=Formula(
                ArithmeticOperator.DIVIDE.value,
                [
                    Timeseries(
                        metric=Metric(public_name="foo"),
                        aggregate="sum",
                        filters=[Condition(Column("tag"), Op.EQ, "tag_value")],
                        groupby=[Column("transaction")],
                    ),
                    Timeseries(
                        metric=Metric(public_name="bar"),
                        aggregate="sum",
                        filters=[Condition(Column("tag"), Op.EQ, "tag_value")],
                        groupby=[Column("transaction")],
                    ),
                ],
            ),
        ),
        id="test terms with groupby 4",
    ),
    pytest.param(
        '(sum(foo) / sum(bar)){tag:"tag_value"} by transaction',
        MetricsQuery(
            query=Formula(
                ArithmeticOperator.DIVIDE.value,
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
                filters=[Condition(Column("tag"), Op.EQ, "tag_value")],
                groupby=[Column("transaction")],
            ),
        ),
        id="test terms with groupby 5",
    ),
    pytest.param(
        '((sum(foo{tag:"tag_value"}){tag2:"tag_value2"} / sum(bar)){tag3:"tag_value3"} * sum(pop)) by transaction',
        MetricsQuery(
            query=Formula(
                function_name=ArithmeticOperator.MULTIPLY.value,
                parameters=[
                    Formula(
                        ArithmeticOperator.DIVIDE.value,
                        [
                            Timeseries(
                                metric=Metric(public_name="foo"),
                                aggregate="sum",
                                filters=[
                                    Condition(Column("tag2"), Op.EQ, "tag_value2"),
                                    Condition(Column("tag"), Op.EQ, "tag_value"),
                                ],
                            ),
                            Timeseries(
                                metric=Metric(public_name="bar"),
                                aggregate="sum",
                            ),
                        ],
                        filters=[Condition(Column("tag3"), Op.EQ, "tag_value3")],
                    ),
                    Timeseries(
                        metric=Metric(public_name="pop"),
                        aggregate="sum",
                    ),
                ],
                groupby=[Column("transaction")],
            )
        ),
        id="test complex nested terms",
    ),
]


@pytest.mark.parametrize("mql_string, metrics_query", term_tests)
def test_parse_mql_terms(mql_string: str, metrics_query: MetricsQuery) -> None:
    result = parse_mql(mql_string)
    assert result == metrics_query


arbitrary_function_tests = [
    pytest.param(
        "simple_function(sum(transaction.duration))",
        MetricsQuery(
            query=Formula(
                "simple_function",
                [
                    Timeseries(
                        metric=Metric(public_name="transaction.duration"),
                        aggregate="sum",
                    ),
                ],
            )
        ),
        id="test simple arbitrary function",
    ),
    pytest.param(
        'another_function("test", 500)',
        MetricsQuery(
            query=Formula(
                "another_function",
                [
                    "test",
                    500,
                ],
            )
        ),
        id="test arbitrary function with string parameter",
    ),
    pytest.param(
        "sum(count(transaction.duration))",
        MetricsQuery(
            query=Formula(
                "sum",
                [
                    Timeseries(
                        metric=Metric(public_name="transaction.duration"),
                        aggregate="count",
                    ),
                ],
            )
        ),
        id="test arbitrary function with inner aggregate",
    ),
    pytest.param(
        'apdex(sum(transaction.duration), 500){tag:"tag_value"} by transaction',
        MetricsQuery(
            query=Formula(
                function_name="apdex",
                parameters=[
                    Timeseries(
                        metric=Metric(public_name="transaction.duration"),
                        aggregate="sum",
                    ),
                    500,
                ],
                filters=[Condition(Column("tag"), Op.EQ, "tag_value")],
                groupby=[Column("transaction")],
            ),
        ),
        id="test arbitrary function with filters and groupby",
    ),
    pytest.param(
        "apdex(quantiles(0.5)(transaction.duration), 500)",
        MetricsQuery(
            query=Formula(
                "apdex",
                [
                    Timeseries(
                        metric=Metric(public_name="transaction.duration"),
                        aggregate="quantiles",
                        aggregate_params=[0.5],
                    ),
                    500,
                ],
            )
        ),
        id="test arbitrary function with curried aggregate",
    ),
    pytest.param(
        "apdex(failure_rate(sum(transaction.duration)), 500)",
        MetricsQuery(
            query=Formula(
                "apdex",
                [
                    Formula(
                        function_name="failure_rate",
                        parameters=[
                            Timeseries(
                                metric=Metric(public_name="transaction.duration"),
                                aggregate="sum",
                            )
                        ],
                    ),
                    500,
                ],
            )
        ),
        id="test arbitrary function within arbitrary function",
    ),
    pytest.param(
        'topK(sum(transaction.duration), 500, 4.2){tag:"tag_value"} by transaction',
        MetricsQuery(
            query=Formula(
                function_name="topK",
                parameters=[
                    Timeseries(
                        metric=Metric(public_name="transaction.duration"),
                        aggregate="sum",
                    ),
                    500,
                    4.2,
                ],
                filters=[Condition(Column("tag"), Op.EQ, "tag_value")],
                groupby=[Column("transaction")],
            ),
        ),
        id="test arbitrary function with filters and groupby",
    ),
    pytest.param(
        'apdex(sum(foo) / sum(bar), 500){tag:"tag_value"} by transaction',
        MetricsQuery(
            query=Formula(
                function_name="apdex",
                parameters=[
                    Formula(
                        function_name=ArithmeticOperator.DIVIDE.value,
                        parameters=[
                            Timeseries(
                                metric=Metric(public_name="foo"),
                                aggregate="sum",
                            ),
                            Timeseries(
                                metric=Metric(public_name="bar"),
                                aggregate="sum",
                            ),
                        ],
                    ),
                    500,
                ],
                filters=[Condition(Column("tag"), Op.EQ, "tag_value")],
                groupby=[Column("transaction")],
            )
        ),
        id="test arbitrary function with inner terms",
    ),
    pytest.param(
        "apdex(sum(transaction.duration), 500) * failure_rate(sum(transaction.duration))",
        MetricsQuery(
            query=Formula(
                function_name="multiply",
                parameters=[
                    Formula(
                        function_name="apdex",
                        parameters=[
                            Timeseries(
                                metric=Metric(public_name="transaction.duration"),
                                aggregate="sum",
                            ),
                            500,
                        ],
                    ),
                    Formula(
                        function_name="failure_rate",
                        parameters=[
                            Timeseries(
                                metric=Metric(public_name="transaction.duration"),
                                aggregate="sum",
                            )
                        ],
                    ),
                ],
            )
        ),
        id="test arbitrary function as outer terms",
    ),
]


@pytest.mark.parametrize("mql_string, metrics_query", arbitrary_function_tests)
def test_parse_mql_arbitrary_functions(
    mql_string: str, metrics_query: MetricsQuery
) -> None:
    result = parse_mql(mql_string)
    assert result == metrics_query


curried_arbitrary_function_tests = [
    pytest.param(
        'topK(10)("test.duration")',
        MetricsQuery(
            query=Formula(
                function_name="topK",
                aggregate_params=[10],
                parameters=["test.duration"],
            )
        ),
        id="test curried arbitrary function with string param",
    ),
    pytest.param(
        "topK(10)(sum(transaction.duration))",
        MetricsQuery(
            query=Formula(
                function_name="topK",
                aggregate_params=[10],
                parameters=[
                    Timeseries(
                        metric=Metric(public_name="transaction.duration"),
                        aggregate="sum",
                    ),
                ],
            )
        ),
        id="test curried arbitrary function with inner aggregate",
    ),
    pytest.param(
        'topK(10)(sum(transaction.duration), 500, "test")',
        MetricsQuery(
            query=Formula(
                function_name="topK",
                aggregate_params=[10],
                parameters=[
                    Timeseries(
                        metric=Metric(public_name="transaction.duration"),
                        aggregate="sum",
                    ),
                    500,
                    "test",
                ],
            )
        ),
        id="test curried arbitrary function with inner aggregate and params",
    ),
    pytest.param(
        "topK(10)(sum(transaction.duration), count(transaction.duration))",
        MetricsQuery(
            query=Formula(
                function_name="topK",
                aggregate_params=[10],
                parameters=[
                    Timeseries(
                        metric=Metric(public_name="transaction.duration"),
                        aggregate="sum",
                    ),
                    Timeseries(
                        metric=Metric(public_name="transaction.duration"),
                        aggregate="count",
                    ),
                ],
            )
        ),
        id="test curried arbitrary function with multiple inner aggregate params",
    ),
    pytest.param(
        "topK(10)(sum(transaction.duration) / count(transaction.duration))",
        MetricsQuery(
            query=Formula(
                function_name="topK",
                aggregate_params=[10],
                parameters=[
                    Formula(
                        function_name="divide",
                        parameters=[
                            Timeseries(
                                metric=Metric(public_name="transaction.duration"),
                                aggregate="sum",
                            ),
                            Timeseries(
                                metric=Metric(public_name="transaction.duration"),
                                aggregate="count",
                            ),
                        ],
                    ),
                ],
            )
        ),
        id="test curried arbitrary function with inner aggregate and terms",
    ),
    pytest.param(
        "topK(10)(sum(transaction.duration{bar:baz}) / count(transaction.duration{foo:foz})) by transaction",
        MetricsQuery(
            query=Formula(
                function_name="topK",
                aggregate_params=[10],
                parameters=[
                    Formula(
                        function_name="divide",
                        parameters=[
                            Timeseries(
                                metric=Metric(public_name="transaction.duration"),
                                aggregate="sum",
                                filters=[Condition(Column("bar"), Op.EQ, "baz")],
                            ),
                            Timeseries(
                                metric=Metric(public_name="transaction.duration"),
                                aggregate="count",
                                filters=[Condition(Column("foo"), Op.EQ, "foz")],
                            ),
                        ],
                    ),
                ],
                groupby=[Column("transaction")],
            ),
        ),
        id="test complex curried arbitrary function with inner terms",
    ),
    pytest.param(
        "topK(10)(topK(5)(transaction.duration){bar:baz})",
        MetricsQuery(
            query=Formula(
                function_name="topK",
                aggregate_params=[10],
                parameters=[
                    Timeseries(
                        metric=Metric(public_name="transaction.duration"),
                        aggregate="topK",
                        aggregate_params=[5],
                        filters=[Condition(Column("bar"), Op.EQ, "baz")],
                    ),
                ],
            ),
        ),
        id="test nested curried arbitrary function",
    ),
    pytest.param(
        "topK(10)(apdex(sum(transaction.duration), 500){bar:baz})",
        MetricsQuery(
            query=Formula(
                function_name="topK",
                aggregate_params=[10],
                parameters=[
                    Formula(
                        function_name="apdex",
                        parameters=[
                            Timeseries(
                                metric=Metric(public_name="transaction.duration"),
                                aggregate="sum",
                            ),
                            500,
                        ],
                        filters=[Condition(Column("bar"), Op.EQ, "baz")],
                    ),
                ],
            ),
        ),
        id="test curried arbitrary function with inner arbitrary function",
    ),
]


@pytest.mark.parametrize("mql_string, metrics_query", curried_arbitrary_function_tests)
def test_parse_mql_curried_arbitrary_functions(
    mql_string: str, metrics_query: MetricsQuery
) -> None:
    result = parse_mql(mql_string)
    assert result == metrics_query

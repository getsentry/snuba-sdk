import pytest

from snuba_sdk.column import Column
from snuba_sdk.conditions import Condition, Op
from snuba_sdk.dsl.dsl import parse_mql
from snuba_sdk.formula import Formula
from snuba_sdk.metrics_query import MetricsQuery
from snuba_sdk.timeseries import Timeseries, Metric


tests = [
    pytest.param(
        'sum(`d:transactions/duration@millisecond`)',
        MetricsQuery(
            query=Timeseries(
                metric=Metric(mri="d:transactions/duration@millisecond"), aggregate="sum"
            )
        ),
        id="test quoted mri name"
    ),
    pytest.param(
        'sum(d:transactions/duration@millisecond)',
        MetricsQuery(
            query=Timeseries(
                metric=Metric(mri="d:transactions/duration@millisecond"), aggregate="sum"
            )
        ),
        id="test unquoted mri name"
    ),
    pytest.param(
        'sum(`transactions.duration`)',
        MetricsQuery(
            query=Timeseries(
                metric=Metric(public_name="transactions.duration"), aggregate="sum"
            )
        ),
        id="test quoted public name 1"
    ),
    pytest.param(
        'sum(`foo`)',
        MetricsQuery(
            query=Timeseries(metric=Metric(public_name="foo"), aggregate="sum")
        ),
        id="test quoted public name 2"
    ),
    pytest.param(
        'sum(transactions.duration)',
        MetricsQuery(
            query=Timeseries(
                metric=Metric(public_name="transactions.duration"), aggregate="sum"
            )
        ),
        id="test unquoted public name 1"
    ),
    pytest.param(
        'sum(foo)',
        MetricsQuery(
            query=Timeseries(metric=Metric(public_name="foo"), aggregate="sum")
        ),
        id="test unquoted public name 1"
    ),
    pytest.param(
        '(sum(foo))',
        MetricsQuery(
            query=Timeseries(metric=Metric(public_name="foo"), aggregate="sum")
        ),
        id="test nested expressions 1"
    ),
    pytest.param(
        '(sum(foo))',
        MetricsQuery(
            query=Timeseries(metric=Metric(public_name="foo"), aggregate="sum")
        ),
        id="test nested expressions 2"
    ),
    pytest.param(
        'sum(foo){bar="baz"}',
        MetricsQuery(
            query=Timeseries(metric=Metric(public_name="foo"), aggregate="sum", filters=[Condition(Column("bar"), Op.EQ, "baz")])
        ),
        id="test filter"
    ),
    pytest.param(
        'sum(foo){bar IN ("baz", "bap")}',
        MetricsQuery(
            query=Timeseries(metric=Metric(public_name="foo"), aggregate="sum", filters=[Condition(Column("bar"), Op.IN, ["baz", "bap"])])
        ),
        id="test in filter"
    ),
    pytest.param(
        'sum(foo{bar="baz"})',
        MetricsQuery(
            query=Timeseries(
                metric=Metric(public_name="foo"),
                aggregate="sum",
                filters=[Condition(Column("bar"), Op.EQ, "baz")],
            )
        ),
        id="test filter inside aggregate"
    ),
    pytest.param(
        'sum(user{bar="baz", foo="foz"})',
        MetricsQuery(
            query=Timeseries(
                metric=Metric(public_name="user"),
                aggregate="sum",
                filters=[
                    Condition(Column("bar"), Op.EQ, "baz"),
                    Condition(Column("foo"), Op.EQ, "foz"),
                ],
            )
        ),
        id="test multiple filters"
    ),
    pytest.param(
        'sum(`d:transactions/duration@millisecond`{foo="foz", hee="haw"}){bar="baz"}',
        MetricsQuery(
            query=Timeseries(
                metric=Metric(mri="d:transactions/duration@millisecond"),
                aggregate="sum",
                filters=[
                    Condition(Column("bar"), Op.EQ, "baz"),
                    Condition(Column("foo"), Op.EQ, "foz"),
                    Condition(Column("hee"), Op.EQ, "haw"),
                ],
            ),
        ),
        id="test multiple layer filters"
    ),
    pytest.param(
        'max(`d:transactions/duration@millisecond`{foo="foz"}) by transaction',
        MetricsQuery(
            query=Timeseries(
                metric=Metric(mri="d:transactions/duration@millisecond"),
                aggregate="max",
                filters=[Condition(Column("foo"), Op.EQ, "foz")],
                groupby=[Column("transaction")],
            )
        ),
        id="test group by 1"
    ),
    pytest.param(
        'max(`d:transactions/duration@millisecond`{foo="foz"} by transaction)',
        MetricsQuery(
            query=Timeseries(
                metric=Metric(mri="d:transactions/duration@millisecond"),
                aggregate="max",
                filters=[Condition(Column("foo"), Op.EQ, "foz")],
                groupby=[Column("transaction")],
            ),
        ),
        id="test group by 2"
    ),
    pytest.param(
        'max(`d:transactions/duration@millisecond`{foo="foz"}) by (transaction)',
        MetricsQuery(
            query=Timeseries(
                metric=Metric(mri="d:transactions/duration@millisecond"),
                aggregate="max",
                filters=[Condition(Column("foo"), Op.EQ, "foz")],
                groupby=[Column("transaction")],
            ),
        ),
        id="test group by 3"
    ),
    pytest.param(
        'max(`d:transactions/duration@millisecond`{foo="foz"}){bar="baz"} by (a, b)',
        MetricsQuery(
            query=Timeseries(
                metric=Metric(mri="d:transactions/duration@millisecond"),
                aggregate="max",
                filters=[Condition(Column("bar"), Op.EQ, "baz"), Condition(Column("foo"), Op.EQ, "foz")],
                groupby=[Column("a"), Column("b")]
            )
        ),
        id="test group by 4"
    ),
]


@pytest.mark.parametrize("mql_string, metrics_query", tests)
def test_parse_mql(mql_string, metrics_query: MetricsQuery) -> None:
    result = parse_mql(mql_string)
    assert result == metrics_query


@pytest.mark.xfail(reason="Not supported")
def test_terms() -> None:
    dsl = "sum(foo) / 1000"
    result = parse_mql(dsl)
    assert result == MetricsQuery(
        query=Formula(
            "divide",
            [
                Timeseries(
                    metric=Metric(public_name="foo"),
                    aggregate="sum",
                ),
                1000.0,
            ],
        )
    )

    dsl = "sum(foo) * sum(bar)"
    result = parse_mql(dsl)
    assert result == MetricsQuery(
        query=Formula(
            "multiply",
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
        )
    )


@pytest.mark.xfail(reason="Not supported")
def test_terms_with_filters() -> None:
    dsl = '(sum(foo) / sum(bar)){tag="tag_value"}'
    result = parse_mql(dsl)
    assert result == MetricsQuery(
        query=Formula(
            "divide",
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
    )

    dsl = 'sum(foo{tag="tag_value"}) / sum(bar{tag="tag_value"})'
    result = parse_mql(dsl)
    assert result == MetricsQuery(
        query=Formula(
            "divide",
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
    )


@pytest.mark.xfail(reason="Not supported")
def test_terms_with_groupby() -> None:
    dsl = '(sum(foo) / sum(bar)){tag="tag_value"} by transaction'
    result = parse_mql(dsl)
    assert result == MetricsQuery(
        query=Formula(
            "divide",
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
    )

    dsl = "(sum(foo) by transaction / sum(bar) by transaction)"
    result = parse_mql(dsl)
    assert result == MetricsQuery(
        query=Formula(
            "divide",
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
    )

    dsl = '(sum(foo) by transaction / sum(bar) by transaction){tag="tag_value"}'
    result = parse_mql(dsl)
    assert result == MetricsQuery(
        query=Formula(
            "divide",
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
    )

    dsl = '(sum(foo{tag="tag_value"}) by transaction) / (sum(bar{tag="tag_value"}) by transaction)'
    result = parse_mql(dsl)
    assert result == MetricsQuery(
        query=Formula(
            "divide",
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
    )

    dsl = '(sum(foo){tag="tag_value"}) by transaction / (sum(bar){tag="tag_value"}) by transaction'
    result = parse_mql(dsl)
    assert result == MetricsQuery(
        query=Formula(
            "divide",
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
    )

    dsl = '(sum(foo) / sum(bar)){tag="tag_value"} by transaction'
    result = parse_mql(dsl)
    assert result == MetricsQuery(
        query=Formula(
            "divide",
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
    )

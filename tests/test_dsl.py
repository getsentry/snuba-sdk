import pytest

from snuba_sdk.column import Column
from snuba_sdk.conditions import Condition, Op
from snuba_sdk.dsl.dsl import parse_mql
from snuba_sdk.function import Function
from snuba_sdk.metrics_query import MetricsQuery
from snuba_sdk.timeseries import Timeseries, Metric


def test_quoted_mri_name() -> None:
    dsl = "sum(`d:transactions/duration@millisecond`)"
    result = parse_mql(dsl)
    assert result == MetricsQuery(
        query=Timeseries(
            metric=Metric(mri="d:transactions/duration@millisecond"), aggregate="sum"
        )
    )


def test_unquoted_mri_name() -> None:
    dsl = "sum(d:transactions/duration@millisecond)"
    result = parse_mql(dsl)
    assert result == MetricsQuery(
        query=Timeseries(
            metric=Metric(mri="d:transactions/duration@millisecond"), aggregate="sum"
        )
    )


def test_quoted_public_name() -> None:
    dsl = "sum(`transactions.duration`)"
    result = parse_mql(dsl)
    assert result == MetricsQuery(
        query=Timeseries(
            metric=Metric(public_name="transactions.duration"), aggregate="sum"
        )
    )

    dsl = "sum(`foo`)"
    result = parse_mql(dsl)
    assert result == MetricsQuery(
        query=Timeseries(metric=Metric(public_name="foo"), aggregate="sum")
    )


def test_unquoted_public_name() -> None:
    dsl = "sum(transactions.duration)"
    result = parse_mql(dsl)
    assert result == MetricsQuery(
        query=Timeseries(
            metric=Metric(public_name="transactions.duration"), aggregate="sum"
        )
    )

    dsl = "sum(foo)"
    result = parse_mql(dsl)
    assert result == MetricsQuery(
        query=Timeseries(metric=Metric(public_name="foo"), aggregate="sum")
    )


def test_nested_expression() -> None:
    dsl = "(sum(foo))"
    result = parse_mql(dsl)
    assert result == MetricsQuery(
        query=Timeseries(metric=Metric(public_name="foo"), aggregate="sum")
    )

    dsl = "sum((foo))"
    result = parse_mql(dsl)
    assert result == MetricsQuery(
        query=Timeseries(metric=Metric(public_name="foo"), aggregate="sum")
    )


def test_filter() -> None:
    dsl = 'sum(foo){bar="baz"}'
    result = parse_mql(dsl)
    assert result == MetricsQuery(
        query=Timeseries(metric=Metric(public_name="foo"), aggregate="sum"),
        filters=[Condition(Column("bar"), Op.EQ, "baz")],
    )


def test_in_filter() -> None:
    dsl = 'sum(foo){bar IN ("baz", "bap")}'
    result = parse_mql(dsl)
    assert result == MetricsQuery(
        query=Timeseries(metric=Metric(public_name="foo"), aggregate="sum"),
        filters=[Condition(Column("bar"), Op.IN, ["baz", "bap"])],
    )


def test_filter_inside_aggregate() -> None:
    dsl = 'sum(foo{bar="baz"})'
    result = parse_mql(dsl)
    assert result == MetricsQuery(
        query=Timeseries(
            metric=Metric(public_name="foo"),
            aggregate="sum",
            filters=[Condition(Column("bar"), Op.EQ, "baz")],
        )
    )


def test_multiple_filters() -> None:
    dsl = 'sum(user{bar="baz", foo="foz"})'
    result = parse_mql(dsl)
    assert result == MetricsQuery(
        query=Timeseries(
            metric=Metric(public_name="user"),
            aggregate="sum",
            filters=[
                Condition(Column("bar"), Op.EQ, "baz"),
                Condition(Column("foo"), Op.EQ, "foz"),
            ],
        )
    )


def test_multi_layer_filters() -> None:
    dsl = 'sum(`d:transactions/duration@millisecond`{foo="foz", hee="haw"}){bar="baz"}'
    result = parse_mql(dsl)
    assert result == MetricsQuery(
        query=Timeseries(
            metric=Metric(mri="d:transactions/duration@millisecond"),
            aggregate="sum",
            filters=[
                Condition(Column("foo"), Op.EQ, "foz"),
                Condition(Column("hee"), Op.EQ, "haw"),
            ],
        ),
        filters=[Condition(Column("bar"), Op.EQ, "baz")],
    )


def test_group_by() -> None:
    dsl = 'max(`d:transactions/duration@millisecond`{foo="foz"}) by transaction'
    result = parse_mql(dsl)
    assert result == MetricsQuery(
        query=Timeseries(
            metric=Metric(mri="d:transactions/duration@millisecond"),
            aggregate="max",
            filters=[Condition(Column("foo"), Op.EQ, "foz")],
        ),
        groupby=[Column("transaction")],
    )

    dsl = 'max(`d:transactions/duration@millisecond`{foo="foz"}) by (transaction)'
    result = parse_mql(dsl)
    assert result == MetricsQuery(
        query=Timeseries(
            metric=Metric(mri="d:transactions/duration@millisecond"),
            aggregate="max",
            filters=[Condition(Column("foo"), Op.EQ, "foz")],
        ),
        groupby=[Column("transaction")],
    )

    dsl = 'max(`d:transactions/duration@millisecond`{foo="foz"}){bar="baz"} by (a, b)'
    result = parse_mql(dsl)
    assert result == MetricsQuery(
        query=Timeseries(
            metric=Metric(mri="d:transactions/duration@millisecond"),
            aggregate="max",
            filters=[Condition(Column("foo"), Op.EQ, "foz")],
        ),
        filters=[Condition(Column("bar"), Op.EQ, "baz")],
        groupby=[Column("a"), Column("b")],
    )


@pytest.mark.xfail(reason="Not supported")
def test_terms() -> None:
    dsl = "sum(foo) / 1000"
    result = parse_mql(dsl)
    assert result == MetricsQuery(
        query=Function(
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
        query=Function(
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
        query=Function(
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
        ),
        filters=[Condition(Column("tag"), Op.EQ, "tag_value")],
    )

    dsl = 'sum(foo{tag="tag_value"}) / sum(bar{tag="tag_value"})'
    result = parse_mql(dsl)
    assert result == MetricsQuery(
        query=Function(
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
        query=Function(
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
        ),
        filters=[Condition(Column("tag"), Op.EQ, "tag_value")],
        groupby=[Column("transaction")],
    )

    dsl = "(sum(foo) by transaction / sum(bar) by transaction)"
    result = parse_mql(dsl)
    assert result == MetricsQuery(
        query=Function(
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
        query=Function(
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
        filters=[Condition(Column("tag"), Op.EQ, "tag_value")],
    )

    dsl = '(sum(foo{tag="tag_value"}) by transaction) / (sum(bar{tag="tag_value"}) by transaction)'
    result = parse_mql(dsl)
    assert result == MetricsQuery(
        query=Function(
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
        query=Function(
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
        query=Function(
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
        ),
        filters=[Condition(Column("tag"), Op.EQ, "tag_value")],
        groupby=[Column("transaction")],
    )

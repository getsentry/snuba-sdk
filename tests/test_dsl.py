from snuba_sdk.dsl.dsl import parse_expression
from snuba_sdk.dsl.types import Function, MetricName
from snuba_sdk.metrics_query import MetricsQuery
from snuba_sdk.timeseries import MetricsScope, Rollup, Timeseries, Metric
from snuba_sdk.column import Column
from snuba_sdk.conditions import Condition, ConditionFunction, Op


def test_quoted_mri_name() -> None:
    dsl = 'sum(`d:transactions/duration@millisecond`)'
    metric_query = MetricsQuery(query=Timeseries(
        metric=Metric(
            mri="d:transactions/duration@millisecond"),
        aggregate="sum")
    )
    result = parse_expression(dsl)
    print("final parsed expression")
    print(result)
    # assert result == metric_query


def test_unquoted_mri_name() -> None:
    dsl = 'sum(`d:transactions/duration@millisecond`)'
    metric_query = MetricsQuery(query=Timeseries(
        metric=Metric(
            mri="d:transactions/duration@millisecond"),
        aggregate="sum")
    )
    result = parse_expression(dsl)
    print("final parsed expression")
    print(result)
    # assert result == metric_query


def test_quoted_public_name() -> None:
    dsl = 'sum(`transactions.duration`)'
    metric_query = MetricsQuery(query=Timeseries(
        metric=Metric(
            public_name="transactions.duration"),
        aggregate="sum")
    )
    result = parse_expression(dsl)
    print("final parsed expression")
    print(result)
    # assert result == metric_query

    dsl = 'sum(`foo`)'
    metric_query = MetricsQuery(query=Timeseries(
        metric=Metric(
            public_name="foo"),
        aggregate="sum")
    )
    result = parse_expression(dsl)
    print("final parsed expression")
    print(result)
    # assert result == metric_query


def test_unquoted_public_name() -> None:
    dsl = 'sum(transactions.duration)'
    metric_query = MetricsQuery(query=Timeseries(
        metric=Metric(
            public_name="transactions.duration"),
        aggregate="sum")
    )
    result = parse_expression(dsl)
    print("final parsed expression")
    print(result)
    # assert result == metric_query

    dsl = 'sum(foo)'
    metric_query = MetricsQuery(query=Timeseries(
        metric=Metric(
            public_name="foo"),
        aggregate="sum")
    )
    result = parse_expression(dsl)
    print("final parsed expression")
    print(result)
    # assert result == metric_query


def test_nested_expression():
    # TODO: are these a valid dsl? If so, we need to support this by fixing nested brackets
    dsl = '(sum(foo))'
    metric_query = MetricsQuery(query=Timeseries(
        metric=Metric(
            public_name="foo"),
        aggregate="sum")
    )
    result = parse_expression(dsl)
    print("final parsed expression")
    print(result)
    # assert parse_expression(dsl) == metric_query

    dsl = 'sum((foo))'
    metric_query = MetricsQuery(query=Timeseries(
        metric=Metric(
            public_name="foo"),
        aggregate="sum")
    )
    result = parse_expression(dsl)
    print("final parsed expression")
    print(result)
    # assert parse_expression(dsl) == metric_query


def test_filter() -> None:
    dsl = 'sum(foo){bar="baz"}'
    metric_query = MetricsQuery(query=Timeseries(
        metric=Metric(
            public_name="user"),
        aggregate="sum"),
        filters=[Condition(Column("bar"), Op.EQ, "baz")]
    )
    result = parse_expression(dsl)
    print("final parsed expression")
    print(result)
    # assert parse_expression(dsl) == expr


def test_in_filter() -> None:
    dsl = 'sum(foo){bar IN ("baz", "bap")}'
    metric_query = MetricsQuery(query=Timeseries(
        metric=Metric(
            public_name="user"),
        aggregate="sum"),
        filters=[Condition(Column("bar"), Op.IN, ["baz", "bap"])]
    )
    result = parse_expression(dsl)
    print("final parsed expression")
    print(result)
    # assert parse_expression(dsl) == expr


def test_filter_inside_aggregate() -> None:
    dsl = 'sum(foo{bar="baz"})'
    metric_query = MetricsQuery(query=Timeseries(
        metric=Metric(
            public_name="user"),
        aggregate="sum",
        filters=[Condition(Column("bar"), Op.EQ, "baz")])
    )
    result = parse_expression(dsl)
    print("final parsed expression")
    print(result)
    # assert parse_expression(dsl) == expr


def test_multiple_filters() -> None:
    dsl = 'sum(user{bar="baz", foo="foz"})'
    metric_query = MetricsQuery(query=Timeseries(
        metric=Metric(
            public_name="user"),
        aggregate="sum",
        filters=[Condition(Column("bar"), Op.EQ, "baz"), Condition(Column("foo"), Op.EQ, "foz")])
    )
    result = parse_expression(dsl)
    print("final parsed expression")
    print(result)
    # assert parse_expression(dsl) == expr


def test_multi_layer_filters() -> None:
    dsl = 'sum(`d:transactions/duration@millisecond`{foo="foz", hee="haw"}){bar="baz"}'
    metric_query = MetricsQuery(query=Timeseries(
        metric=Metric(
            mri="d:transactions/duration@millisecond"),
        aggregate="sum",
        filters=[Condition(Column("foo"), Op.EQ, "foz"), Condition(Column("hee"), Op.EQ, "haw")]),
        filters=[Condition(Column("bar"), Op.EQ, "baz")]
    )
    result = parse_expression(dsl)
    print("final parsed expression")
    print(result)
    # assert parse_expression(dsl) == expr


def test_group_by() -> None:
    dsl = 'max(`d:transactions/duration@millisecond`{foo="foz"}) by transaction'
    metric_query = MetricsQuery(query=Timeseries(
        metric=Metric(
            mri="d:transactions/duration@millisecond"),
        aggregate="max",
        filters=[Condition(Column("foo"), Op.EQ, "foz")]),
        groupby=[Column("transaction")]
    )
    result = parse_expression(dsl)
    print("final parsed expression")
    print(result)
    # assert parse_expression(dsl) == expr

    dsl = 'max(`d:transactions/duration@millisecond`{foo="foz"}) by (transaction)'
    metric_query = MetricsQuery(query=Timeseries(
        metric=Metric(
            mri="d:transactions/duration@millisecond"),
        aggregate="max",
        filters=[Condition(Column("foo"), Op.EQ, "foz")]),
        groupby=[Column("transaction")]
    )
    result = parse_expression(dsl)
    print("final parsed expression")
    print(result)
    # assert parse_expression(dsl) == expr

    dsl = 'max(`d:transactions/duration@millisecond`{foo="foz"}) by (a, b)'
    metric_query = MetricsQuery(query=Timeseries(
        metric=Metric(
            mri="d:transactions/duration@millisecond"),
        aggregate="max",
        filters=[Condition(Column("foo"), Op.EQ, "foz")]),
        groupby=[Column("a"), Column("b")]
    )
    result = parse_expression(dsl)
    print("final parsed expression")
    print(result)
    # assert parse_expression(dsl) == expr

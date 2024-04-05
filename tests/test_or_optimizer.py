from snuba_sdk.column import Column
from snuba_sdk.conditions import Condition, Op
from snuba_sdk.formula import Formula
from snuba_sdk.mql.mql import parse_mql
from snuba_sdk.query_optimizers.or_optimizer import OrOptimizer
from snuba_sdk.timeseries import Timeseries


def test_timeseries() -> None:
    mql_string = '(avg(d:transactions/duration@millisecond) by (transaction)){transaction:"a" OR transaction:"b" OR transaction:"c"}'
    parsed = parse_mql(mql_string)
    assert isinstance(parsed, Timeseries)
    expected_optimized = Timeseries(
        metric=parsed.metric,
        aggregate=parsed.aggregate,
        aggregate_params=parsed.aggregate_params,
        filters=[Condition(Column("transaction"), Op.IN, ["a", "b", "c"])],
        groupby=parsed.groupby,
    )
    actual_optimized = OrOptimizer().optimize(parsed)
    assert actual_optimized == expected_optimized


def test_formula() -> None:
    mql_string = '(avg(d:transactions/duration@millisecond) / sum(d:transactions/duration@millisecond)){transaction:"a" OR transaction:"b" OR transaction:"c"}'
    parsed = parse_mql(mql_string)
    assert isinstance(parsed, Formula)
    expected_optimized = Formula(
        function_name=parsed.function_name,
        parameters=parsed.parameters,
        aggregate_params=parsed.aggregate_params,
        filters=[Condition(Column("transaction"), Op.IN, ["a", "b", "c"])],
        groupby=parsed.groupby,
    )
    actual_optimized = OrOptimizer().optimize(parsed)
    assert actual_optimized == expected_optimized


def test_unsupported() -> None:
    mql_string = '(avg(d:transactions/duration@millisecond) by (transaction)){transaction:"a" OR (transaction:"b" OR transaction:"c")}'
    parsed = parse_mql(mql_string)
    actual_optimized = OrOptimizer().optimize(parsed)
    assert actual_optimized == parsed

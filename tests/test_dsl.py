from snuba_sdk.dsl.dsl import parse_expression
from snuba_sdk.dsl.types import Function, MetricName

def test_parse_expression1() -> None:
    dsl = 'sum(`d:transactions/duration@millisecond`)'
    expr = Function("sum", [MetricName("d:transactions/duration@millisecond")])
    result = parse_expression(dsl)
    print("final parsed expression")
    print(result.__dict__)
    # assert parse_expression(dsl) == expr

def test_parse_expression2() -> None:
    dsl = 'sum(user{foo="foz", hee="haw"})'
    expr = Function("sum", [MetricName("d:transactions/duration@millisecond")])
    result = parse_expression(dsl)
    print("final parsed expression")
    print(result)
    # assert parse_expression(dsl) == expr

def test_parse_expression3() -> None:
    dsl = 'sum(`d:transactions/duration@millisecond`{foo="foz", hee="haw"}){bar="baz"}'
    expr = Function("sum", [MetricName("d:transactions/duration@millisecond")])
    result = parse_expression(dsl)
    print("final parsed expression")
    print(result)
    # assert parse_expression(dsl) == expr

def test_parse_expression4() -> None:
    dsl = 'max(`d:transactions/duration@millisecond`{status_code=500}) by transaction'
    expr = Function("sum", [MetricName("d:transactions/duration@millisecond")])
    result = parse_expression(dsl)
    print("final parsed expression")
    print(result)
    # assert parse_expression(dsl) == expr


def test_parse_expression6() -> None:
    dsl = '(count(transactions{satisfaction="satisfied"}) + count(transactions{satisfaction="tolerable"}) / 2) / count(transactions)'
    result = parse_expression(dsl)
    print("final parsed expression")
    print(result.__dict__)
    # assert parse_expression(dsl) == expr

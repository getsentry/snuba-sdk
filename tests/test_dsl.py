from snuba_sdk.dsl.dsl import parse_expression
from snuba_sdk.dsl.types import Function, MetricName

def test_parse_expression() -> None:
    dsl = 'sum(`d:transactions/duration@millisecond`{foo="foz"}){bar="baz"}'
    expr = Function("sum", [MetricName("d:transactions/duration@millisecond")])
    result = parse_expression(dsl)
    print("final parsed expression")
    print(result.__dict__)
    # assert parse_expression(dsl) == expr

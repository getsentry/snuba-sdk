"""
Contains the definition of MQL, the Metrics Query Language.
Use ``parse_expression` to parse an MQL string into an expression.
"""

from typing import Mapping

from parsimonious.exceptions import ParseError
from parsimonious.grammar import Grammar
from parsimonious.nodes import NodeVisitor
from snuba_sdk.timeseries import MetricsScope, Rollup, Timeseries, Metric

# from sentry.utils.strings import unescape_string
from snuba_sdk.query import Query
from snuba_sdk.conditions import Condition, ConditionFunction, Op
from snuba_sdk.expressions import Expression
from snuba_sdk.arithmetic import ArithmeticFunction
from snuba_sdk.function import Function
from snuba_sdk.query_visitors import InvalidQueryError
from snuba_sdk.column import Column
from snuba_sdk.dsl.types import (
    Filter,
    # Function,
    # InvalidMetricsQuery,
    # MetricName,
    # Tag,
    Variable,
)
from snuba_sdk.metrics_query import MetricsQuery

GRAMMAR = Grammar(
    r"""
expression = term (_ expr_op _ term)*
expr_op = "+" / "-"
term = coefficient (_ term_op _ coefficient)*
term_op = "*" / "/"
coefficient = number / filter
number = ~r"[0-9]+" ("." ~r"[0-9]+")?
filter = target ("{" _ condition (_ "," _ condition)* _ "}")?
condition = (variable / tag_key) _ condition_op _ tag_value
condition_op = "=" / "!=" / "~" / "!~" / "IN" / "NOT IN"
tag_key = ~"[a-zA-Z0-9_]+"
tag_value = string / string_tuple / variable
string = ~r'"([^"\\]*(?:\\.[^"\\]*)*)"'
string_tuple = "(" _ string (_ "," _ string)* _ ")"
target = variable / quoted_metric / nested_expression / function / unquoted_metric
variable = "$" ~"[a-zA-Z0-9_]+"
nested_expression = "(" _ expression _ ")"
function = function_name "(" _ expression (_ "," _ expression)* _ ")"
function_name = ~"[a-zA-Z0-9_]+"
quoted_metric = ~r'`([^`]*)`'
unquoted_metric = ~"[a-zA-Z0-9_]+"
_ = ~r"\s*"
"""
)

EXPRESSION_OPERATORS: Mapping[str, str] = {
    "+": ArithmeticFunction.PLUS.value,
    "-": ArithmeticFunction.MINUS.value,
}

TERM_OPERATORS: Mapping[str, str] = {
    "*": ArithmeticFunction.MULTIPLY.value,
    "/": ArithmeticFunction.DIVIDE.value,
}

# CONDITION_OPERATORS: Mapping[str, str] = {
#     "=": ConditionFunction.EQ.value,
#     "!=": ConditionFunction.NEQ.value,
#     "~": ConditionFunction.LIKE.value,
#     "!~": ConditionFunction.NOT_LIKE.value,
#     "IN": ConditionFunction.IN.value,
#     "NOT IN": ConditionFunction.NOT_IN.value,
# }

def parse_expression(mql: str) -> Expression:
    """
    Parse a metrics expression from a string.
    """

    try:
        tree = GRAMMAR.parse(mql.strip())
        print(tree)
    except ParseError as e:
        raise InvalidQueryError("Invalid metrics syntax") from e
    print('done parsing')
    return MqlVisitor().visit(tree)


class MqlVisitor(NodeVisitor):
    def visit_expression(self, node, children):
        expr, zero_or_more_others = children
        print('visiting expression')
        print(type(expr))
        print(zero_or_more_others)

        for _, op, _, other in zero_or_more_others:
            expr = Function(op, [expr, other])
            raise InvalidQueryError("Invalid metrics syntax")
        return expr

    def visit_expr_op(self, node, children):
        return EXPRESSION_OPERATORS[node.text]

    def visit_term(self, node, children):
        term, zero_or_more_others = children
        print('visiting term')
        print(type(term))
        print(zero_or_more_others)

        for _, op, _, other in zero_or_more_others:
            term = Function(op, [term, other])
            raise InvalidQueryError("Invalid metrics syntax")

        return term

    def visit_term_op(self, node, children):
        return TERM_OPERATORS[node.text]

    def visit_coefficient(self, node, children):
        return children[0]

    def visit_number(self, node, children):
        return float(node.text)

    def visit_filter(self, node, children):
        target, zero_or_one = children
        if isinstance(target, str):
            return target
        assert isinstance(target, MetricsQuery)
        print("visiting filter")
        print(type(target))
        print(target.__dict__)
        print(zero_or_one)
        if not zero_or_one:
            return target

        _, _, first, zero_or_more_others, _, _ = zero_or_one[0]
        conditions = [first, *(v for _, _, _, v in zero_or_more_others)]
        new_target = target.set_filters(conditions)
        print(new_target.__dict__)
        return new_target

    def visit_condition(self, node, children):
        key_or_variable, _, op, _, rhs = children
        return Condition(key_or_variable[0], op, rhs)

    def visit_condition_op(self, node, children):
        return Op(node.text)

    def visit_tag_key(self, node, children):
        return Column(node.text)

    def visit_tag_value(self, node, children):
        return children[0]

    def visit_string(self, node, children):
        return node.text[1:-1]

    def visit_string_tuple(self, node, children):
        _, _, first, zero_or_more_others, _, _ = children
        return [first, *(v for _, _, _, v in zero_or_more_others)]

    def visit_target(self, node, children):
        return children[0]

    def visit_variable(self, node, children):
        return Variable(node.text[1:])

    def visit_nested_expression(self, node, children):
        return children[2]

    def visit_function(self, node, children):
        function_name, _, _, first, zero_or_more_others, _, _ = children
        print('visiting function')
        print(function_name)
        print(first)
        print(zero_or_more_others)
        parameters = [first, *(v for _, _, _, v in zero_or_more_others)]
        print(parameters)
        result = MetricsQuery(query=Timeseries(metric=Metric(mri=first), aggregate=function_name))
        print(result.__dict__)
        return result


        # return Function(function_name, parameters)

    def visit_function_name(self, node, children):
        return node.text

    def visit_quoted_metric(self, node, children):
        return str(node.text[1:-1])

    def visit_unquoted_metric(self, node, children):
        return str(node.text)

    def visit_identifier(self, node, children):
        return node.text

    def generic_visit(self, node, children):
        """The generic visit method."""
        return children

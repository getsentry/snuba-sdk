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

PLACEHOLDER = "PLACEHOLDER"
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
target = variable / quoted_metric / nested_expression / aggregate / unquoted_metric
variable = "$" ~"[a-zA-Z0-9_]+"
nested_expression = "(" _ expression _ ")"
aggregate = aggregate_name "(" _ expression (_ "," _ expression)* _ ")"
aggregate_name = ~"[a-zA-Z0-9_]+"
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


# TODO: Need to determine whether or not target is a MRI and public name
# - match group by clause

class MqlVisitor(NodeVisitor):
    def visit(self, node):
        """Walk a parse tree, transforming it into another representation.

        Recursively descend a parse tree, dispatching to the method named after
        the rule in the :class:`~parsimonious.grammar.Grammar` that produced
        each node. If, for example, a rule was... ::

            bold = '<b>'

        ...the ``visit_bold()`` method would be called. It is your
        responsibility to subclass :class:`NodeVisitor` and implement those
        methods.

        """
        method = getattr(self, 'visit_' + node.expr_name, self.generic_visit)

        # Call that method, and show where in the tree it failed if it blows up.
        try:
            result = method(node, [self.visit(n) for n in node])
            metric_query = self.formulate_metric_query(result)
            return metric_query
        except Exception:
            raise Exception
    def formulate_metric_query(self, result):
        return result

    def visit_expression(self, node, children):
        expr, zero_or_more_others = children
        print('visited expression')
        print(expr)
        return expr


    def visit_expr_op(self, node, children):
        raise InvalidQueryError("Arithmetic function not supported")
        return EXPRESSION_OPERATORS[node.text]

    def visit_term(self, node, children):
        term, zero_or_more_others = children
        print('visited term')
        print(children)
        return term

    def visit_term_op(self, node, children):
        raise InvalidQueryError("Arithmetic function not supported")
        return TERM_OPERATORS[node.text]

    def visit_coefficient(self, node, children):
        print('visited coefficient')
        print(children[0])
        return children[0]

    def visit_number(self, node, children):
        return float(node.text)

    def visit_filter(self, node, children):
        target, zero_or_one = children
        print('visited filter')
        if not zero_or_one:
            return {"target": target}
        print(children)
        print(target)
        _, _, first, zero_or_more_others, _, _ = zero_or_one[0]
        filters = [first, *(v for _, _, _, v in zero_or_more_others)]
        print(filters)
        result = {"target": target, "filters": filters}
        print(result)
        return result

    def visit_condition(self, node, children):
        print('visited condition')
        print(children)
        lhs, _, op, _, rhs = children
        return Condition(lhs[0], op, rhs)

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
        print('visiting target')
        print(children)
        return children[0]

    def visit_variable(self, node, children):
        return Variable(node.text[1:])

    def visit_nested_expression(self, node, children):
        return children[2]

    def visit_aggregate(self, node, children):
        aggregate_name, _, _, first, zero_or_more_others, _, _ = children
        print('visited aggregate')
        print(children)
        assert isinstance(first, dict)
        first.update({"aggregate": aggregate_name})
        return first

    def visit_aggregate_name(self, node, children):
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

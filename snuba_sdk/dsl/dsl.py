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
from snuba_sdk.dsl.types import Variable
from snuba_sdk.metrics_query import MetricsQuery

ENTITY_TYPE_REGEX = r'(c|s|d|g|e)'
NAMESPACE_REGEX = r'(transactions|errors|issues|sessions|alerts|custom|spans|escalating_issues)'
MRI_NAME_REGEX = r'([a-z_]+(?:\.[a-z_]+)*)'
UNIT_REGEX = r'([\w.]*)'
GRAMMAR = Grammar(
    rf"""
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
tag_value = quoted_string / quoted_string_tuple / variable
quoted_string = ~r'"([^"\\]*(?:\\.[^"\\]*)*)"'
quoted_string_tuple = "(" _ quoted_string (_ "," _ quoted_string)* _ ")"
target = variable / nested_expression / function / metric
variable = "$" ~"[a-zA-Z0-9_]+"
nested_expression = "(" _ expression _ ")"
function = aggregate (group_by)?
aggregate = aggregate_name ("(" _ expression (_ "," _ expression)* _ ")")
aggregate_name = ~"[a-zA-Z0-9_]+"
group_by = _ "by" _ (group_by_name / group_by_name_tuple)
group_by_name = ~"[a-zA-Z0-9_]+"
group_by_name_tuple = "(" _ group_by_name (_ "," _ group_by_name)* _ ")"
metric = quoted_mri / unquoted_mri / quoted_public_name / unquoted_public_name
quoted_mri = ~r'`{ENTITY_TYPE_REGEX}:{NAMESPACE_REGEX}/{MRI_NAME_REGEX}@{UNIT_REGEX}`'
unquoted_mri = ~r'{ENTITY_TYPE_REGEX}:{NAMESPACE_REGEX}/{MRI_NAME_REGEX}@{UNIT_REGEX}'
quoted_public_name = ~r'`([a-z_]+(?:\.[a-z_]+)*)`'
unquoted_public_name = ~r'([a-z_]+(?:\.[a-z_]+)*)'
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
        print(zero_or_one)
        print(target)
        _, _, first, zero_or_more_others, *_, = zero_or_one[0]
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

    def visit_function(self, node, children):
        print('visited function')
        print(children)
        target, zero_or_one = children
        if len(zero_or_one) > 0:
            group_by = zero_or_one[0]
            if isinstance(group_by, list):
                target["groupby"] = group_by
            else:
                target["groupby"] = [group_by]
        print(target)
        return target


    def visit_group_by(self, node, children):
        print('visited group_by')
        *_, group_by = children
        print(children)
        print(group_by[0])
        return group_by[0]

    def visit_condition_op(self, node, children):
        return Op(node.text)

    def visit_tag_key(self, node, children):
        return Column(node.text)

    def visit_tag_value(self, node, children):
        return children[0]

    def visit_quoted_string(self, node, children):
        return node.text[1:-1]

    def visit_quoted_string_tuple(self, node, children):
        _, _, first, zero_or_more_others, _, _ = children
        return [first, *(v for _, _, _, v in zero_or_more_others)]

    def visit_group_by_name(self, node, children):
        print('visiting group_by_name')
        return Column(node.text)

    def visit_group_by_name_tuple(self, node, children):
        _, _, first, zero_or_more_others, _, _ = children
        print('visiting group_by_name_tuple')
        print(children)
        print(zero_or_more_others)
        print([first, *(v for _, _, _, v in zero_or_more_others)])
        return [first, *(v for _, _, _, v in zero_or_more_others)]

    def visit_target(self, node, children):
        print('visiting target')
        print(children[0])
        if isinstance(children[0], list):
            return children[0][0]
        return children[0]

    def visit_variable(self, node, children):
        return Variable(node.text[1:])

    def visit_nested_expression(self, node, children):
        return children[2]

    def visit_aggregate(self, node, children):
        print(children)
        aggregate_name, zero_or_one = children
        _, _, first, zero_or_more_others, *_ = zero_or_one
        assert isinstance(first, dict)
        first.update({"aggregate": aggregate_name})
        return first

    def visit_aggregate_name(self, node, children):
        return node.text

    def visit_quoted_mri(self, node, children):
        print("visited quoted mri")
        print({"metric_name": str(node.text[1:-1]), "metric_name_type": "mri"})
        return {"metric_name": str(node.text[1:-1]), "metric_name_type": "mri"}

    def visit_unquoted_mri(self, node, children):
        print("visited unquoted mri")
        print({"metric_name": str(node.text), "metric_name_type": "mri"})
        return {"metric_name": str(node.text), "metric_name_type": "mri"}

    def visit_quoted_public_name(self, node, children):
        print("visited quoted public_name")
        print({"metric_name": str(node.text[1:-1]), "metric_name_type": "public_name"})
        return {"metric_name": str(node.text[1:-1]), "metric_name_type": "public_name"}

    def visit_unquoted_public_name(self, node, children):
        print("visited unquoted public_name")
        print({"metric_name": str(node.text), "metric_name_type": "public_name"})
        return {"metric_name": str(node.text), "metric_name_type": "public_name"}

    def visit_identifier(self, node, children):
        return node.text

    def generic_visit(self, node, children):
        """The generic visit method."""
        return children

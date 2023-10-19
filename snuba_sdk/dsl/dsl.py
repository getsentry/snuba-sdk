"""
Contains the definition of MQL, the Metrics Query Language.
Use ``parse_expression` to parse an MQL string into an expression.
"""

from typing import Mapping, Any, List, Sequence

from parsimonious.exceptions import ParseError
from parsimonious.grammar import Grammar
from parsimonious.nodes import Node, NodeVisitor

from snuba_sdk.arithmetic import ArithmeticFunction
from snuba_sdk.column import Column, Variable
from snuba_sdk.conditions import Condition, Op
from snuba_sdk.function import Function
from snuba_sdk.metrics_query import MetricsQuery
from snuba_sdk.query_visitors import InvalidQueryError
from snuba_sdk.timeseries import Timeseries, Metric


AGGREGATE_PLACEHOLDER_NAME = "AGGREGATE_PLACEHOLDER"
ENTITY_TYPE_REGEX = r"(c|s|d|g|e)"
NAMESPACE_REGEX = (
    r"(transactions|errors|issues|sessions|alerts|custom|spans|escalating_issues)"
)
MRI_NAME_REGEX = r"([a-z_]+(?:\.[a-z_]+)*)"
UNIT_REGEX = r"([\w.]*)"
GRAMMAR = Grammar(
    rf"""
expression = term (_ expr_op _ term)*
expr_op = "+" / "-"

term = coefficient (_ term_op _ coefficient)*
term_op = "*" / "/"
coefficient = number / filter

number = ~r"[0-9]+" ("." ~r"[0-9]+")?
filter = target ("{{" _ condition (_ "," _ condition)* _ "}}")? (group_by)?

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


def parse_expression(mql: str) -> MetricsQuery:
    """
    Parse a metrics expression from a string.
    """

    try:
        tree = GRAMMAR.parse(mql.strip())
        print(tree)
    except ParseError as e:
        raise InvalidQueryError("Invalid metrics syntax") from e
    print("done parsing")
    return MqlVisitor().visit(tree)


class MqlVisitor(NodeVisitor):
    def visit(self, node: Node) -> MetricsQuery:
        """Walk a parse tree, transforming it into a MetricsQuery object.

        Recursively descend a parse tree, dispatching to the method named after
        the rule in the :class:`~parsimonious.grammar.Grammar` that produced
        each node. If, for example, a rule was... ::

            bold = '<b>'

        ...the ``visit_bold()`` method would be called.
        """
        method = getattr(self, "visit_" + node.expr_name, self.generic_visit)

        # Call that method, and show where in the tree it failed if it blows up.
        try:
            result = method(node, [self.visit(n) for n in node])
            return result
        except Exception:
            raise Exception

    def visit_expression(self, node: Node, children: Sequence[Any]) -> Any:
        expr, zero_or_more_others = children
        print("visit_expression")
        print(children)
        if isinstance(expr, Timeseries) or isinstance(expr, Function):
            return MetricsQuery(query=expr)
        print("visited expression")
        print(children)
        return expr

    def visit_expr_op(self, node: Node, children: Sequence[Any]) -> Any:
        # raise InvalidQueryError("Arithmetic function not supported")
        print("visit expr_op")
        print(EXPRESSION_OPERATORS[node.text])
        return EXPRESSION_OPERATORS[node.text]

    def collapse_into_timeseries(self, metric_query: MetricsQuery) -> Timeseries:
        metric_query_filters = metric_query.filters if metric_query.filters else []
        metric_query_groupby = metric_query.groupby if metric_query.groupby else []
        timeseries = metric_query.query
        assert timeseries is not None and isinstance(timeseries, Timeseries)

        timeseries_filters = timeseries.filters if timeseries.filters else []
        timeseries_groupby = timeseries.groupby if timeseries.groupby else []

        combined_filters = metric_query_filters + timeseries_filters
        combined_groupby = metric_query_groupby + timeseries_groupby

        combined_groupby = combined_groupby if combined_groupby else None
        combined_filters = combined_filters if combined_filters else None
        return timeseries.set_filters(combined_filters).set_groupby(combined_groupby)

    def visit_term(self, node: Node, children: Sequence[Any]) -> Any:
        term, zero_or_more_others = children
        print("visited term")
        print(children)
        print(zero_or_more_others)
        if zero_or_more_others:
            _, term_operator, _, coefficient, *_ = zero_or_more_others[0]
            # If LHS or RHS is a MetricsQuery object, then collapse its filters and groupbys into the timeseries.
            if isinstance(coefficient, MetricsQuery):
                coefficient = self.collapse_into_timeseries(coefficient)
            if isinstance(term, MetricsQuery):
                term = self.collapse_into_timeseries(term)
            function = Function(term_operator, [term, coefficient])
            return MetricsQuery(query=function)
        return term

    def visit_term_op(self, node: Node, children: Sequence[Any]) -> Any:
        # raise InvalidQueryError("Arithmetic function not supported")
        print("visit term_op")
        print(TERM_OPERATORS[node.text])
        return TERM_OPERATORS[node.text]

    def visit_coefficient(self, node: Node, children: Sequence[Any]) -> Any:
        print("visited coefficient")
        return children[0]

    def visit_number(self, node: Node, children: Sequence[Any]) -> Any:
        return float(node.text)

    def visit_filter(self, node: Node, children: Sequence[Any]) -> Any:
        target, packed_filters, packed_group_bys, *_ = children
        print("visited filter")
        if not packed_filters and not packed_group_bys:
            return target
        print(children)
        print(packed_filters)
        print(packed_group_bys)
        if packed_filters:
            (
                _,
                _,
                first,
                zero_or_more_others,
                *_,
            ) = packed_filters[0]
            filters = [first, *(v for _, _, _, v in zero_or_more_others)]
            print(filters)
            assert isinstance(target, MetricsQuery) or isinstance(target, Timeseries)
            current_filters = target.filters if target.filters else []
            filters.extend(current_filters)
            target = target.set_filters(filters)
        if packed_group_bys:
            print(packed_group_bys)
            group_by = packed_group_bys[0]
            if not isinstance(group_by, list):
                group_by = [group_by]
            if isinstance(target, Timeseries):
                target = MetricsQuery(query=target, groupby=group_by)
            if isinstance(target, MetricsQuery):
                target = target.set_groupby(group_by)
        return target

    def visit_condition(self, node: Node, children: Sequence[Any]) -> Any:
        print("visited condition")
        print(children)
        lhs, _, op, _, rhs = children
        return Condition(lhs[0], op, rhs)

    def visit_function(self, node: Node, children: Sequence[Any]) -> Any:
        print("visited function")
        print(children)
        target, packed_group_by = children
        if len(packed_group_by) > 0:
            group_by = packed_group_by[0]
            if not isinstance(group_by, list):
                group_by = [group_by]
            if isinstance(target, MetricsQuery):
                assert target.query is not None and isinstance(target.query, Timeseries)
                return target.set_query(target.query.set_groupby(group_by))
            if isinstance(target, Timeseries):
                return MetricsQuery(query=target, groupby=group_by)
        return target

    def visit_group_by(self, node: Node, children: Sequence[Any]) -> Any:
        print("visited group_by")
        *_, group_by = children
        print(children)
        print(group_by[0])
        return group_by[0]

    def visit_condition_op(self, node: Node, children: Sequence[Any]) -> Any:
        return Op(node.text)

    def visit_tag_key(self, node: Node, children: Sequence[Any]) -> Any:
        return Column(node.text)

    def visit_tag_value(self, node: Node, children: Sequence[Any]) -> Any:
        return children[0]

    def visit_quoted_string(self, node: Node, children: Sequence[Any]) -> Any:
        return str(node.text[1:-1])

    def visit_quoted_string_tuple(self, node: Node, children: Sequence[Any]) -> Any:
        _, _, first, zero_or_more_others, _, _ = children
        return [first, *(v for _, _, _, v in zero_or_more_others)]

    def visit_group_by_name(self, node: Node, children: Sequence[Any]) -> Any:
        print("visiting group_by_name")
        return Column(node.text)

    def visit_group_by_name_tuple(self, node: Node, children: Sequence[Any]) -> Any:
        _, _, first, zero_or_more_others, _, _ = children
        print("visiting group_by_name_tuple")
        print(children)
        print(zero_or_more_others)
        print([first, *(v for _, _, _, v in zero_or_more_others)])
        return [first, *(v for _, _, _, v in zero_or_more_others)]

    def visit_target(self, node: Node, children: Sequence[Any]) -> Any:
        print("visiting target")
        print(children)
        target = children[0]
        if isinstance(children[0], list):
            target = children[0][0]
        print(type(target))
        if isinstance(target, Metric):
            timeseries = Timeseries(metric=target, aggregate=AGGREGATE_PLACEHOLDER_NAME)
            return timeseries
        return target

    def visit_variable(self, node: Node, children: Sequence[Any]) -> Any:
        return Variable(node.text[1:])

    def visit_nested_expression(self, node: Node, children: Sequence[Any]) -> Any:
        return children[2]

    def visit_aggregate(self, node: Node, children: Sequence[Any]) -> Any:
        print("visted aggregate")
        print(children)
        aggregate_name, zero_or_one = children
        _, _, target, zero_or_more_others, *_ = zero_or_one
        if isinstance(target, Timeseries):
            return target.set_aggregate(aggregate_name)
        if isinstance(target, MetricsQuery):
            assert target.query is not None and isinstance(target.query, Timeseries)
            return target.set_query(target.query.set_aggregate(aggregate_name))
        return target

    def visit_aggregate_name(self, node: Node, children: Sequence[Any]) -> Any:
        return node.text

    def visit_quoted_mri(self, node: Node, children: Sequence[Any]) -> Any:
        print("visited quoted mri")
        metric = Metric(mri=str(node.text[1:-1]))
        print(metric)
        return metric

    def visit_unquoted_mri(self, node: Node, children: Sequence[Any]) -> Any:
        print("visited unquoted mri")
        metric = Metric(mri=str(node.text))
        print(metric)
        return metric

    def visit_quoted_public_name(self, node: Node, children: Sequence[Any]) -> Any:
        print("visited quoted public_name")
        metric = Metric(public_name=str(node.text[1:-1]))
        print(metric)
        return metric

    def visit_unquoted_public_name(self, node: Node, children: Sequence[Any]) -> Any:
        print("visited unquoted public_name")
        metric = Metric(public_name=str(node.text))
        print(metric)
        return metric

    def visit_identifier(self, node: Node, children: Sequence[Any]) -> Any:
        return node.text

    def generic_visit(self, node: Node, children: Sequence[Any]) -> Any:
        """The generic visit method."""
        return children

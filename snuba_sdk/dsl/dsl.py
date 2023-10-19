"""
Contains the definition of MQL, the Metrics Query Language.
Use `parse_mql()` to parse an MQL string into a MetricsQuery.
"""

from typing import Any, Mapping, Sequence

from parsimonious.exceptions import ParseError
from parsimonious.grammar import Grammar
from parsimonious.nodes import Node, NodeVisitor

from snuba_sdk.arithmetic import ArithmeticFunction
from snuba_sdk.column import Column, Variable
from snuba_sdk.conditions import Condition, Op
from snuba_sdk.function import Function
from snuba_sdk.metrics_query import MetricsQuery
from snuba_sdk.query_visitors import InvalidQueryError
from snuba_sdk.timeseries import Metric, Timeseries


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


def parse_mql(mql: str) -> MetricsQuery:
    """
    Parse a MQL string into a MetricsQuery object.
    """
    try:
        tree = GRAMMAR.parse(mql.strip())
    except ParseError as e:
        raise InvalidQueryError("Invalid metrics syntax") from e
    result = MQLlVisitor().visit(tree)
    assert isinstance(result, MetricsQuery)
    return result


class MQLlVisitor(NodeVisitor):
    def visit(self, node: Node) -> Any:
        """Walk a parse tree, transforming it into a MetricsQuery object.

        Recursively descend a parse tree, dispatching to the method named after
        the rule in the :class:`~parsimonious.grammar.Grammar` that produced
        each node. If, for example, a rule was... ::

            bold = '<b>'

        ...the ``visit_bold()`` method would be called.
        """
        method = getattr(self, "visit_" + node.expr_name, self.generic_visit)
        try:
            result = method(node, [self.visit(n) for n in node])
            return result
        except Exception:
            raise Exception

    def collapse_into_timeseries(self, metric_query: MetricsQuery) -> Timeseries:
        """
        Collpases the filters and groupbys of a MetricsQuery into the Timeseries object.
        """
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

    def visit_expression(self, node: Node, children: Sequence[Any]) -> Any:
        """
        Top level node, wraps the expression in a MetricsQuery object.
        """
        expr, zero_or_more_others = children
        if isinstance(expr, Timeseries) or isinstance(expr, Function):
            return MetricsQuery(query=expr)
        return expr

    def visit_expr_op(self, node: Node, children: Sequence[Any]) -> Any:
        raise InvalidQueryError("Arithmetic function not supported yet")
        return EXPRESSION_OPERATORS[node.text]

    def visit_term(self, node: Node, children: Sequence[Any]) -> Any:
        """
        Checks if the current node contains two term children, if so
        then merge them into a single Function with the operator. If the
        children are MetricQuery objects, then collapse them into a Timeseries first.
        """
        term, zero_or_more_others = children
        if zero_or_more_others:
            _, term_operator, _, coefficient, *_ = zero_or_more_others[0]
            if isinstance(term, MetricsQuery):
                term = self.collapse_into_timeseries(term)
            if isinstance(coefficient, MetricsQuery):
                coefficient = self.collapse_into_timeseries(coefficient)
            function = Function(term_operator, [term, coefficient])
            return MetricsQuery(query=function)
        return term

    def visit_term_op(self, node: Node, children: Sequence[Any]) -> Any:
        raise InvalidQueryError("Arithmetic function not supported yet")
        return TERM_OPERATORS[node.text]

    def visit_coefficient(self, node: Node, children: Sequence[Any]) -> Any:
        return children[0]

    def visit_number(self, node: Node, children: Sequence[Any]) -> Any:
        return float(node.text)

    def visit_filter(self, node: Node, children: Sequence[Any]) -> Any:
        """
        Given a target (which could be either a MetricsQuery or Timeseries object),
        and set its children filters and groupbys on it.
        """
        target, packed_filters, packed_groupbys, *_ = children
        if not packed_filters and not packed_groupbys:
            return target
        assert isinstance(target, MetricsQuery) or isinstance(target, Timeseries)
        if packed_filters:
            _, _, first, zero_or_more_others, *_ = packed_filters[0]
            filters = [first, *(v for _, _, _, v in zero_or_more_others)]
            current_filters = target.filters if target.filters else []
            filters.extend(current_filters)
            target = target.set_filters(filters)
        if packed_groupbys:
            group_by = packed_groupbys[0]
            if not isinstance(group_by, list):
                group_by = [group_by]
            current_groupby = target.groupby if target.groupby else []
            group_by.extend(current_groupby)
            target = target.set_groupby(group_by)
        return target

    def visit_condition(self, node: Node, children: Sequence[Any]) -> Any:
        lhs, _, op, _, rhs = children
        return Condition(lhs[0], op, rhs)

    def visit_function(self, node: Node, children: Sequence[Any]) -> Any:
        """
        Given an target (which could be either a MetricsQuery or Timeseries object),
        and set its children filters and groupbys on it.
        """
        target, packed_groupbys = children
        assert isinstance(target, MetricsQuery) or isinstance(target, Timeseries)
        if packed_groupbys:
            group_by = packed_groupbys[0]
            if not isinstance(group_by, list):
                group_by = [group_by]
            current_groupby = target.groupby if target.groupby else []
            group_by.extend(current_groupby)
            target = target.set_groupby(group_by)
        return target

    def visit_group_by(self, node: Node, children: Sequence[Any]) -> Any:
        *_, group_by = children
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
        return Column(node.text)

    def visit_group_by_name_tuple(self, node: Node, children: Sequence[Any]) -> Any:
        _, _, first, zero_or_more_others, _, _ = children
        return [first, *(v for _, _, _, v in zero_or_more_others)]

    def visit_target(self, node: Node, children: Sequence[Any]) -> Any:
        """
        Given a target (which is a Metric object), create a Timeseries object and return it.
        """
        target = children[0]
        if isinstance(children[0], list):
            target = children[0][0]
        if isinstance(target, Metric):
            # it visit the aggregate name furthur down the tree, for now just set it to a placeholder.
            timeseries = Timeseries(metric=target, aggregate=AGGREGATE_PLACEHOLDER_NAME)
            return timeseries
        return target

    def visit_variable(self, node: Node, children: Sequence[Any]) -> Any:
        return Variable(node.text[1:])

    def visit_nested_expression(self, node: Node, children: Sequence[Any]) -> Any:
        return children[2]

    def visit_aggregate(self, node: Node, children: Sequence[Any]) -> Any:
        """
        Given a target (which is either a MetricsQuery or Timeseries object),
        set the aggregate on it.
        """
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
        return Metric(mri=str(node.text[1:-1]))

    def visit_unquoted_mri(self, node: Node, children: Sequence[Any]) -> Any:
        return Metric(mri=str(node.text))

    def visit_quoted_public_name(self, node: Node, children: Sequence[Any]) -> Any:
        return Metric(public_name=str(node.text[1:-1]))

    def visit_unquoted_public_name(self, node: Node, children: Sequence[Any]) -> Any:
        return Metric(public_name=str(node.text))

    def visit_identifier(self, node: Node, children: Sequence[Any]) -> Any:
        return node.text

    def generic_visit(self, node: Node, children: Sequence[Any]) -> Any:
        """The generic visit method."""
        return children

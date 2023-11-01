"""
Contains the definition of MQL, the Metrics Query Language.
Use `parse_mql()` to parse an MQL string into a MetricsQuery.
"""

from typing import Any, List, Mapping, Sequence, Union

from parsimonious.exceptions import ParseError
from parsimonious.grammar import Grammar
from parsimonious.nodes import Node, NodeVisitor

from snuba_sdk.column import Column
from snuba_sdk.conditions import Condition, Op
from snuba_sdk.formula import ArithmeticOperator, Formula, FormulaParameterGroup
from snuba_sdk.metrics_query import MetricsQuery
from snuba_sdk.query_visitors import InvalidQueryError
from snuba_sdk.timeseries import Metric, Timeseries

AGGREGATE_PLACEHOLDER_NAME = "AGGREGATE_PLACEHOLDER"
METRIC_TYPE_REGEX = r"(c|s|d|g|e)"
NAMESPACE_REGEX = r"[a-zA-Z0-9_]+"
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
filter = target (open_brace _ condition (_ comma _ condition)* _ close_brace)? (group_by)?

condition = (variable / tag_key) _ condition_op _ tag_value
condition_op = "=" / "!=" / "~" / "!~" / "IN" / "NOT IN"
tag_key = ~"[a-zA-Z0-9_]+"
tag_value = quoted_string / quoted_string_tuple / variable

quoted_string = ~r'"([^"\\]*(?:\\.[^"\\]*)*)"'
quoted_string_tuple = open_paren _ quoted_string (_ comma _ quoted_string)* _ close_paren

target = variable / nested_expression / function / metric
variable = "$" ~"[a-zA-Z0-9_]+"
nested_expression = open_paren _ expression _ close_paren

function = aggregate (group_by)?
aggregate = aggregate_name (open_paren _ expression (_ comma _ expression)* _ close_paren)
aggregate_name = ~"[a-zA-Z0-9_]+"

group_by = _ "by" _ (group_by_name / group_by_name_tuple)
group_by_name = ~"[a-zA-Z0-9_]+"
group_by_name_tuple = open_paren _ group_by_name (_ comma _ group_by_name)* _ close_paren

metric = quoted_mri / unquoted_mri / quoted_public_name / unquoted_public_name
quoted_mri = backtick unquoted_mri backtick
unquoted_mri = ~r'{METRIC_TYPE_REGEX}:{NAMESPACE_REGEX}/{MRI_NAME_REGEX}@{UNIT_REGEX}'
quoted_public_name = backtick unquoted_public_name backtick
unquoted_public_name = ~r'([a-z_]+(?:\.[a-z_]+)*)'

open_paren = "("
close_paren = ")"
open_brace = "{{"
close_brace = "}}"
comma = ","
backtick = "`"
_ = ~r"\s*"
"""
)

EXPRESSION_OPERATORS: Mapping[str, ArithmeticOperator] = {
    "+": ArithmeticOperator.PLUS,
    "-": ArithmeticOperator.MINUS,
}

TERM_OPERATORS: Mapping[str, ArithmeticOperator] = {
    "*": ArithmeticOperator.MULTIPLY,
    "/": ArithmeticOperator.DIVIDE,
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
    assert isinstance(result, (Timeseries, Formula))
    metrics_query = MetricsQuery(query=result)
    return metrics_query


class MQLlVisitor(NodeVisitor):  # type: ignore
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
        except Exception as e:
            raise e

    def collapse_into_timeseries(self, formula: Formula) -> Formula:
        """
        Collapses the filters and groupbys of a Formula object into the Timeseries object
        using the distributive property.

        For example:
        (sum(foo){tag="value"} / sum(bar){tag="value"}){tag2="value2"}
        becomes ->
        (sum(foo){tag="value", tag2="value2"} / sum(bar){tag="value", tag2="value2"})
        """
        formula_filters = formula.filters if formula.filters else []
        formula_groupby = formula.groupby if formula.groupby else []
        new_parameters: List[FormulaParameterGroup] = []
        if formula.parameters:
            for parameter in formula.parameters:
                if isinstance(parameter, Formula):
                    new_formula = self.collapse_into_timeseries(parameter)
                    new_parameters.append(new_formula)
                if isinstance(parameter, Timeseries):
                    timeseries_filters = parameter.filters if parameter.filters else []
                    timeseries_groupby = parameter.groupby if parameter.groupby else []
                    combined_filters = formula_filters + timeseries_filters
                    combined_groupby = formula_groupby + timeseries_groupby
                    combined_filters = combined_filters if combined_filters else None
                    combined_groupby = combined_groupby if combined_groupby else None
                    new_timeseries = parameter.set_filters(
                        combined_filters
                    ).set_groupby(combined_groupby)
                    new_parameters.append(new_timeseries)
                else:
                    assert isinstance(parameter, (float, int))
                    new_parameters.append(parameter)
            formula = formula.set_parameters(new_parameters)
        return formula.set_filters(None).set_groupby(None)

    def visit_expression(
        self, node: Node, children: Sequence[Any]
    ) -> Union[Formula, Timeseries]:
        """
        Top level node, simply returns the expression.
        """
        expr, zero_or_more_others = children
        assert isinstance(expr, (Formula, Timeseries))
        return expr

    def visit_expr_op(self, node: Node, children: Sequence[Any]) -> Any:
        raise InvalidQueryError("Arithmetic function not supported yet")
        return EXPRESSION_OPERATORS[node.text]

    def visit_term(
        self, node: Node, children: Sequence[Any]
    ) -> Union[Formula, Timeseries, float, int]:
        """
        Checks if the current node contains two term children, if so
        then merge them into a single Formula with the operator. If the
        children are Formula objects, then collapse them into a Timeseries first.
        """
        term, zero_or_more_others = children
        assert isinstance(term, (Formula, Timeseries, float, int))
        if zero_or_more_others:
            _, term_operator, _, coefficient, *_ = zero_or_more_others[0]
            if isinstance(term, Formula):
                term = self.collapse_into_timeseries(term)
            if isinstance(coefficient, Formula):
                coefficient = self.collapse_into_timeseries(coefficient)
            return Formula(term_operator, [term, coefficient])
        return term

    def visit_term_op(self, node: Node, children: Sequence[Any]) -> Any:
        raise InvalidQueryError("Arithmetic function not supported yet")
        return TERM_OPERATORS[node.text]

    def visit_coefficient(
        self, node: Node, children: Sequence[Union[Timeseries, int, float]]
    ) -> Union[Timeseries, int, float]:
        return children[0]

    def visit_number(self, node: Node, children: Sequence[Any]) -> float:
        return float(node.text)

    def visit_filter(
        self, node: Node, children: Sequence[Any]
    ) -> Union[Timeseries, Formula]:
        """
        Given a Formula or Timeseries target, set its children filters and groupbys.
        """
        target, packed_filters, packed_groupbys, *_ = children
        assert isinstance(target, Formula) or isinstance(target, Timeseries)
        if not packed_filters and not packed_groupbys:
            return target
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

    def visit_condition(self, node: Node, children: Sequence[Any]) -> Condition:
        lhs, _, op, _, rhs = children
        return Condition(lhs[0], op, rhs)

    def visit_function(self, node: Node, children: Sequence[Any]) -> Timeseries:
        """
        Given an Timeseries target, set its children groupbys.
        """
        target, packed_groupbys = children
        assert isinstance(target, Timeseries)
        if packed_groupbys:
            group_by = packed_groupbys[0]
            if not isinstance(group_by, list):
                group_by = [group_by]
            current_groupby = target.groupby if target.groupby else []
            group_by.extend(current_groupby)
            target = target.set_groupby(group_by)
        return target

    def visit_group_by(
        self, node: Node, children: Sequence[Any]
    ) -> Union[Column, Sequence[Column]]:
        *_, group_by = children
        group_by_name = group_by[0]
        return group_by_name

    def visit_condition_op(self, node: Node, children: Sequence[Any]) -> Op:
        return Op(node.text)

    def visit_tag_key(self, node: Node, children: Sequence[Any]) -> Column:
        return Column(node.text)

    def visit_tag_value(
        self, node: Node, children: Sequence[Union[str, Sequence[str]]]
    ) -> str:
        tag_value = children[0]
        return tag_value

    def visit_quoted_string(self, node: Node, children: Sequence[Any]) -> str:
        return str(node.text[1:-1])

    def visit_quoted_string_tuple(
        self, node: Node, children: Sequence[Any]
    ) -> Sequence[str]:
        _, _, first, zero_or_more_others, _, _ = children
        return [first, *(v for _, _, _, v in zero_or_more_others)]

    def visit_group_by_name(self, node: Node, children: Sequence[Any]) -> Column:
        return Column(node.text)

    def visit_group_by_name_tuple(
        self, node: Node, children: Sequence[Any]
    ) -> Sequence[str]:
        _, _, first, zero_or_more_others, _, _ = children
        return [first, *(v for _, _, _, v in zero_or_more_others)]

    def visit_target(
        self, node: Node, children: Sequence[Any]
    ) -> Union[Timeseries, Formula]:
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
        assert isinstance(target, (Timeseries, Formula))
        return target

    def visit_variable(self, node: Node, children: Sequence[Any]) -> Any:
        raise InvalidQueryError("Variables are not supported yet")
        return None

    def visit_nested_expression(
        self, node: Node, children: Sequence[Any]
    ) -> Union[Timeseries, Formula]:
        assert isinstance(children[2], (Timeseries, Formula))
        return children[2]

    def visit_aggregate(self, node: Node, children: Sequence[Any]) -> Timeseries:
        """
        Given a target (which is either a Formula or Timeseries object),
        set the aggregate on it.
        """
        aggregate_name, zero_or_one = children
        _, _, target, zero_or_more_others, *_ = zero_or_one
        assert isinstance(target, Timeseries)
        return target.set_aggregate(aggregate_name)

    def visit_aggregate_name(self, node: Node, children: Sequence[Any]) -> str:
        return node.text

    def visit_quoted_mri(self, node: Node, children: Sequence[Any]) -> Metric:
        return Metric(mri=str(node.text[1:-1]))

    def visit_unquoted_mri(self, node: Node, children: Sequence[Any]) -> Metric:
        return Metric(mri=str(node.text))

    def visit_quoted_public_name(self, node: Node, children: Sequence[Any]) -> Metric:
        return Metric(public_name=str(node.text[1:-1]))

    def visit_unquoted_public_name(self, node: Node, children: Sequence[Any]) -> Metric:
        return Metric(public_name=str(node.text))

    def visit_identifier(self, node: Node, children: Sequence[Any]) -> str:
        return node.text

    def generic_visit(self, node: Node, children: Sequence[Any]) -> Any:
        """The generic visit method."""
        return children

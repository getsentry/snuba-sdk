"""
Contains the definition of MQL, the Metrics Query Language.
Use `parse_mql()` to parse an MQL string into a MetricsQuery.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence, Union, cast

from parsimonious.exceptions import ParseError
from parsimonious.grammar import Grammar
from parsimonious.nodes import Node, NodeVisitor

from snuba_sdk.column import Column
from snuba_sdk.conditions import And, BooleanCondition, BooleanOp, Condition, Op, Or
from snuba_sdk.formula import ArithmeticOperator, Formula
from snuba_sdk.timeseries import Metric, Timeseries


class InvalidMQLQueryError(Exception):
    pass


AGGREGATE_PLACEHOLDER_NAME = "AGGREGATE_PLACEHOLDER"

MQL_GRAMMAR = Grammar(
    r"""
expression = term (_ expr_op _ term)*
expr_op = "+" / "-"

term = unary (_ term_op _ unary)*
term_op = "*" / "/"

unary = unary_op? coefficient
unary_op = "-"

coefficient = number / quoted_string / filter

number = ~r"[0-9]+" ("." ~r"[0-9]+")?
filter = target (open_brace (_ filter_expr _ )? close_brace)? (group_by)?

filter_expr = filter_term (_ or _ filter_term)*
filter_term = filter_factor (_ joint_operator? _ filter_factor)*
filter_factor = (condition_op? (variable / tag_key) _ colon _ tag_value) / nested_expr
nested_expr = open_paren _ filter_expr _ close_paren
condition_op = "!"

joint_operator = comma / and

tag_key = ~r"[a-zA-Z0-9_.]+"
tag_value = quoted_suffix_wildcard_tag_value / suffix_wildcard_tag_value / quoted_string_filter / unquoted_string_filter / string_tuple / variable
suffix_wildcard_tag_value = unquoted_string wildcard
quoted_suffix_wildcard_tag_value = quote unquoted_string wildcard quote

quoted_string_filter = ~r'"([^"\\]*(?:\\.[^"\\]*)*)"'
unquoted_string_filter = ~r'[^,\[\]\"}{\(\)\s\*]+'
string_tuple = open_square_bracket _ (quoted_string / unquoted_string) (_ comma _ (quoted_string / unquoted_string))* _ close_square_bracket

quoted_string = ~r'"([^"\\]*(?:\\.[^"\\]*)*)"'
unquoted_string = ~r'[^,\[\]\"}{\(\)\s\*]+'

target = variable / nested_expression / function / metric
variable = "$" ~r"[a-zA-Z0-9_.]+"
nested_expression = open_paren _ expression _ close_paren

function = (curried_aggregate / curried_arbitrary_function / aggregate / arbitrary_function) (group_by)?
aggregate = aggregate_name (open_paren _ inner_filter _ close_paren)
aggregate_name = ~r"[a-zA-Z0-9_]+"
arbitrary_function = arbitrary_function_name (open_paren ( _ expression _ ) (_ comma _ expression)* close_paren)
arbitrary_function_name = ~r"[a-zA-Z0-9_]+"
curried_aggregate = curried_aggregate_name (open_paren _ aggregate_list? _ close_paren) (open_paren _ inner_filter _ close_paren)
curried_aggregate_name = ~r"[a-zA-Z0-9_]+"
curried_arbitrary_function = curried_arbitrary_function_name (open_paren _ aggregate_list? _ close_paren) (open_paren _ ( _ expression _ ) (_ comma _ expression)* close_paren)
curried_arbitrary_function_name = ~r"[a-zA-Z0-9_]+"

aggregate_list = param* (param_expression)
param = param_expression _ comma _
param_expression = number / quoted_string / unquoted_string

group_by = _ "by" _ (group_by_name / group_by_name_tuple)
group_by_name = ~r"[a-zA-Z0-9_.]+"
group_by_name_tuple = open_paren _ group_by_name (_ comma _ group_by_name)* _ close_paren

inner_filter = metric (open_brace (_ filter_expr _)? close_brace)? (group_by)?
metric = quoted_mri / unquoted_mri / quoted_public_name / unquoted_public_name
quoted_mri = backtick lenient_mri backtick
lenient_mri = ~r'[^:`]+:[^/`]+/[^@,`]+@[^`]+'
unquoted_mri = ~r'[^:\(\){}\[\]"`,]+:[^/\(\){}\[\]"`,]+/[^@\(\){}\[\]"`,]+@[^\(\){}\[\]"`,]+'
quoted_public_name = backtick unquoted_public_name backtick
unquoted_public_name = ~r"([a-z_]+(?:\.[a-z_]+)*)"

open_paren = "("
close_paren = ")"
open_square_bracket = "["
close_square_bracket = "]"
open_brace = "{"
close_brace = "}"
comma = ","
backtick = "`"
colon = ":"
and = "AND" / "and"
or = "OR" / "or"
quote = "\""
wildcard = "*"
_ = ~r"\s*"
"""
)

EXPRESSION_OPERATORS: Mapping[str, str] = {
    "+": ArithmeticOperator.PLUS.value,
    "-": ArithmeticOperator.MINUS.value,
}

TERM_OPERATORS: Mapping[str, str] = {
    "*": ArithmeticOperator.MULTIPLY.value,
    "/": ArithmeticOperator.DIVIDE.value,
}

UNARY_OPERATORS: Mapping[str, str] = {
    "-": "negate",
}


def parse_mql(mql: str) -> Timeseries | Formula:
    """
    Parse a MQL string into a Timeseries object.
    """
    try:
        tree = MQL_GRAMMAR.parse(mql.strip())
    except ParseError as e:
        raise InvalidMQLQueryError("Invalid metrics syntax") from e
    result = MQLVisitor().visit(tree)
    assert isinstance(result, (Timeseries, Formula))
    return result


class MQLVisitor(NodeVisitor):  # type: ignore
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

    def visit_expression(
        self, node: Node, children: Sequence[Any]
    ) -> Union[Formula, Timeseries, float, int, str]:
        """
        Top level node, simply returns the expression.
        """
        term_left, zero_or_more_others = children
        assert isinstance(term_left, (Formula, Timeseries, float, int, str))

        if zero_or_more_others:
            for zero_or_more in zero_or_more_others:
                _, expr_operator, _, term_right, *_ = zero_or_more
                term_left = Formula(expr_operator, [term_left, term_right])

        return cast(Union[Formula, Timeseries, float, int, str], term_left)

    def visit_expr_op(self, node: Node, children: Sequence[Any]) -> Any:
        return EXPRESSION_OPERATORS[node.text]

    def visit_term(
        self, node: Node, children: Sequence[Any]
    ) -> Union[Formula, Timeseries, float, int, str]:
        """
        Checks if the current node contains two term children, if so
        then merge them into a single Formula with the operator.
        """
        unary_left, zero_or_more_others = children
        assert isinstance(unary_left, (Formula, Timeseries, float, int, str))

        if zero_or_more_others:
            for zero_or_more in zero_or_more_others:
                _, term_operator, _, unary_right, *_ = zero_or_more
                unary_left = Formula(term_operator, [unary_left, unary_right])

        return cast(Union[Formula, Timeseries, float, int, str], unary_left)

    def visit_term_op(self, node: Node, children: Sequence[Any]) -> Any:
        return TERM_OPERATORS[node.text]

    def visit_unary(
        self, node: Node, children: Sequence[Any]
    ) -> Union[Formula, Timeseries, float, int, str]:
        unary_op, coefficient = children
        if unary_op:
            if isinstance(coefficient, float) or isinstance(coefficient, int):
                return -coefficient
            elif isinstance(coefficient, Formula) or isinstance(
                coefficient, Timeseries
            ):
                return Formula(function_name=unary_op[0], parameters=[coefficient])
            else:
                raise InvalidMQLQueryError(
                    f"Unary expression not supported for type {type(coefficient)}"
                )

        return cast(Union[Formula, Timeseries, float, int, str], coefficient)

    def visit_unary_op(self, node: Node, children: Sequence[Any]) -> Any:
        return UNARY_OPERATORS[node.text]

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
        target, filters, packed_groupbys, *_ = children
        packed_filters = None
        if filters:
            _, packed_filters, _ = filters[0]

        assert isinstance(target, Formula) or isinstance(target, Timeseries)
        if not packed_filters and not packed_groupbys:
            return target
        if packed_filters:
            _, filter_condition, *_ = packed_filters[0]
            current_filters = target.filters if target.filters else []
            filters = [filter_condition]
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

    def _filter(
        self, children: Sequence[Any], operator: BooleanOp
    ) -> Union[Condition, BooleanCondition]:
        first, zero_or_more_others, *_ = children
        filters: Sequence[Union[Condition, BooleanCondition]] = [
            first,
            *(v for _, _, _, v in zero_or_more_others),
        ]
        if len(filters) == 1:
            return filters[0]
        else:
            # We flatten all filters into a single condition since Snuba supports it.
            if operator == BooleanOp.AND:
                return And(conditions=filters)
            elif operator == BooleanOp.OR:
                return Or(conditions=filters)
            else:
                raise InvalidMQLQueryError(f"Invalid boolean operator {operator}")

    def visit_filter_expr(
        self, node: Node, children: Sequence[Any]
    ) -> Union[Condition, BooleanCondition]:
        return self._filter(children, BooleanOp.OR)

    def visit_filter_term(
        self, node: Node, children: Sequence[Any]
    ) -> Union[Condition, BooleanCondition]:
        return self._filter(children, BooleanOp.AND)

    def visit_filter_factor(
        self, node: Node, children: Sequence[Any]
    ) -> Union[Condition, BooleanCondition]:
        factor, *_ = children

        # A nested filter can be both a boolean condition but also a single condition, since we allow `(condition)`.
        if isinstance(factor, BooleanCondition) or isinstance(factor, Condition):
            # If we have a parenthesized expression, we just return it.
            return factor
        else:
            # condition_op, (variable or tag_key), space, colon, space, tag_value
            condition_op, lhs, _, _, _, filter_factor_value = factor
            contains_wildcard = filter_factor_value.contains_wildcard
            rhs = filter_factor_value.value
            op = Op.EQ
            if not condition_op and contains_wildcard and isinstance(rhs, str):
                op = Op.LIKE

            elif not condition_op and isinstance(rhs, list):
                op = Op.IN

            elif len(condition_op) == 1 and condition_op[0] == Op.NOT:
                if not contains_wildcard and isinstance(rhs, str):
                    op = Op.NEQ
                elif not contains_wildcard and isinstance(rhs, list):
                    op = Op.NOT_IN
                elif contains_wildcard and isinstance(rhs, str):
                    op = Op.NOT_LIKE

            return Condition(lhs[0], op, rhs)

    def visit_nested_expr(
        self, node: Node, children: Sequence[Any]
    ) -> Union[Condition, BooleanCondition]:
        _, _, filter_expr, *_ = children
        return cast(Union[Condition, BooleanCondition], filter_expr)

    def visit_function(
        self, node: Node, children: Sequence[Any]
    ) -> Union[Timeseries, Formula]:
        """
        Given an Timeseries or Formula target, set its children groupbys.
        """
        targets, packed_groupbys = children
        target = targets[0]
        assert isinstance(target, (Timeseries, Formula))
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
        group_by_name = group_by[0]
        return group_by_name

    def visit_condition_op(self, node: Node, children: Sequence[Any]) -> Op:
        return Op(node.text)

    def visit_tag_key(self, node: Node, children: Sequence[Any]) -> Column:
        return Column(node.text)

    def visit_tag_value(
        self, node: Node, children: Sequence[FilterFactor]
    ) -> FilterFactor:
        return children[0]

    def visit_quoted_suffix_wildcard_tag_value(
        self, node: Node, children: Sequence[Any]
    ) -> FilterFactor:
        _, text_before_wildcard, _, _ = children
        return FilterFactor(f"{text_before_wildcard}*", True)

    def visit_suffix_wildcard_tag_value(
        self, node: Node, children: Sequence[Any]
    ) -> FilterFactor:
        text_before_wildcard, _ = children
        return FilterFactor(f"{text_before_wildcard}*", True)

    def visit_quoted_string_filter(
        self, node: Node, children: Sequence[Any]
    ) -> FilterFactor:
        text = str(node.text[1:-1])
        match = text.replace('\\"', '"')
        return FilterFactor(match, False)

    def visit_unquoted_string_filter(
        self, node: Node, children: Sequence[Any]
    ) -> FilterFactor:
        return FilterFactor(str(node.text), False)

    def visit_unquoted_string(self, node: Node, children: Sequence[Any]) -> str:
        return str(node.text)

    def visit_test_string(self, node: Node, children: Sequence[Any]) -> str:
        return str(node.text)

    def visit_quoted_string(self, node: Node, children: Sequence[Any]) -> str:
        # The quoted string might have escaped double quotes in it. Replace
        # these with regular quotes.
        text = str(node.text[1:-1])
        match = text.replace('\\"', '"')
        return match

    def visit_string_tuple(self, node: Node, children: Sequence[Any]) -> FilterFactor:
        _, _, first, zero_or_more_others, _, _ = children
        return FilterFactor(
            [first[0], *(v[0] for _, _, _, v in zero_or_more_others)], False
        )

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
            timeseries = Timeseries(metric=target, aggregate=AGGREGATE_PLACEHOLDER_NAME)
            return timeseries
        assert isinstance(target, (Timeseries, Formula))
        return target

    def visit_variable(self, node: Node, children: Sequence[Any]) -> Any:
        raise InvalidMQLQueryError("Variables are not supported yet")

    def visit_nested_expression(
        self, node: Node, children: Sequence[Any]
    ) -> Union[Timeseries, Formula]:
        assert isinstance(children[2], (Timeseries, Formula))
        return children[2]

    def visit_aggregate(
        self, node: Node, children: Sequence[Any]
    ) -> Union[Timeseries, Formula]:
        """
        Given a target (which is either a Formula or Timeseries object),
        set the aggregate on it.
        """
        aggregate_name, zero_or_one = children
        _, _, target, zero_or_more_others, *_ = zero_or_one
        assert isinstance(target, Timeseries)
        if target.aggregate == AGGREGATE_PLACEHOLDER_NAME:
            return target.set_aggregate(aggregate_name)
        else:
            # The parameter inside this aggregate already has an aggregate set.
            # Therefore, this needs to be treated as an arbitrary function.
            # e.g. `sum(count(mri))` -> Formula(sum, (Timeseries(count),)
            return Formula(function_name=aggregate_name, parameters=[target])

    def visit_curried_aggregate(
        self, node: Node, children: Sequence[Any]
    ) -> Timeseries:
        """
        Given a target (which is either a Formula or Timeseries object),
        set the aggregate and aggregate params on it.
        """
        aggregate_name, agg_params, zero_or_one = children
        _, _, target, _, *_ = zero_or_one
        _, _, agg_param_list, *_ = agg_params
        aggregate_params = agg_param_list[0] if agg_param_list else []
        assert isinstance(target, Timeseries)
        return target.set_aggregate(aggregate_name, aggregate_params)

    def visit_arbitrary_function(self, node: Node, children: Sequence[Any]) -> Formula:
        """
        Returns a Fomula with the arbitrary function name and parameters.
        apdex(count(mri), 300) -> Formula(apdex, (Timeseries(count), 300))
        """
        arbitrary_function_name, zero_or_one = children
        _, expr, params, *_ = zero_or_one
        _, target, _ = expr
        arbitrary_function_params = [param[-1] for param in params]
        parameters = [target, *arbitrary_function_params]
        if (
            isinstance(target, Timeseries)
            and target.aggregate == AGGREGATE_PLACEHOLDER_NAME
        ):
            raise InvalidMQLQueryError(
                "Cannot use arbitrary functions on a Timeseries without an aggregate"
            )
        return Formula(function_name=arbitrary_function_name, parameters=parameters)

    def visit_curried_arbitrary_function(
        self, node: Node, children: Sequence[Any]
    ) -> Union[Timeseries, Formula]:
        """
        Returns a Fomula with the arbitrary curried function name and parameters.
        topK(10)(sum(mri)) -> Formula(topK, (Timeseries(sum), 10))
        """
        curried_arbitrary_function_name, agg_params, zero_or_one = children
        _, _, agg_param_list, *_ = agg_params
        aggregate_params = agg_param_list[0] if agg_param_list else []
        _, _, expr, params, *_ = zero_or_one
        _, target, _ = expr
        curried_arbitrary_function_params = [param[-1] for param in params]
        if (
            isinstance(target, Timeseries)
            and target.aggregate == AGGREGATE_PLACEHOLDER_NAME
        ):
            return target.set_aggregate(
                curried_arbitrary_function_name, aggregate_params
            )
        return Formula(
            function_name=curried_arbitrary_function_name,
            aggregate_params=aggregate_params,
            parameters=[target, *curried_arbitrary_function_params],
        )

    def visit_inner_filter(self, node: Node, children: Sequence[Any]) -> Timeseries:
        """
        Given a metric, set its children filters and groupbys, then return a Timeseries.
        """
        metric, filters, packed_groupbys, *_ = children
        packed_filters = None
        if filters:
            _, packed_filters, _ = filters[0]

        metric = metric[0]
        assert isinstance(metric, Metric)
        timeseries = Timeseries(metric=metric, aggregate=AGGREGATE_PLACEHOLDER_NAME)
        if not packed_filters and not packed_groupbys:
            return timeseries
        if packed_filters:
            _, filter_condition, *_ = packed_filters[0]
            timeseries = timeseries.set_filters([filter_condition])
        if packed_groupbys:
            group_by = packed_groupbys[0]
            if not isinstance(group_by, list):
                group_by = [group_by]
            timeseries = timeseries.set_groupby(group_by)
        return timeseries

    def visit_param(self, node: Node, children: Sequence[Any]) -> str | int | float:
        """
        Discard the comma and return the aggregate param for the curried aggregate function.
        """
        param, *_ = children
        assert isinstance(param, (str, int, float))
        return param

    def visit_param_expression(
        self, node: Node, children: Sequence[Any]
    ) -> str | int | float:
        """
        Return the aggregate param for the curried aggregate function.
        """
        (param,) = children
        assert isinstance(param, (str, int, float))
        return param

    def visit_aggregate_list(
        self, node: Node, children: Sequence[Any]
    ) -> list[str | int | float]:
        agg_params, param = children
        if param is not None:
            agg_params.append(param)
        assert isinstance(agg_params, list)
        return agg_params

    def visit_aggregate_name(self, node: Node, children: Sequence[Any]) -> str:
        return node.text

    def visit_curried_aggregate_name(self, node: Node, children: Sequence[Any]) -> str:
        return node.text

    def visit_arbitrary_function_name(self, node: Node, children: Sequence[Any]) -> str:
        return node.text

    def visit_curried_arbitrary_function_name(
        self, node: Node, children: Sequence[Any]
    ) -> str:
        return node.text

    def visit_quoted_mri(self, node: Node, children: Sequence[Any]) -> Metric:
        return Metric(mri=str(node.text[1:-1]))

    def visit_lenient_mri(self, node: Node, children: Sequence[Any]) -> Metric:
        return Metric(mri=str(node.text))

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


@dataclass
class FilterFactor(object):
    value: str | Sequence[str] | Condition | BooleanCondition
    contains_wildcard: bool

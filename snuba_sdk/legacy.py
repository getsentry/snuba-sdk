from __future__ import annotations

from datetime import datetime
from functools import partial
from typing import Any, Mapping, Optional, Sequence, Union

from snuba_sdk.column import Column
from snuba_sdk.conditions import OPERATOR_TO_FUNCTION, Condition, Op, Or
from snuba_sdk.entity import Entity, get_required_time_column
from snuba_sdk.function import Function
from snuba_sdk.orderby import Direction, LimitBy, OrderBy
from snuba_sdk.query import Query

CONDITION_OPERATORS = set(op.value for op in Op)


def is_condition(cond_or_list: Sequence[Any]) -> bool:
    """
    Checks whether a legacy expression is a condition or not.

    :param cond_or_list: A sequence of legacy values.
    :type cond_or_list: Sequence[Any]

    """

    return (
        len(cond_or_list) == 3
        and (
            isinstance(cond_or_list[1], str)
            and cond_or_list[1].upper() in CONDITION_OPERATORS
        )
        and isinstance(cond_or_list[0], (str, tuple, list))
    )


def parse_datetime(date_str: str) -> datetime:
    """
    Tries to convert a string to a datetime using one of the different date formats.

    :param date_str: A possible datetime string.
    :type date_str: str

    :raises ValueError: If the string is not a valid datetime.

    """

    # Python 3.6 doesn't handle tz in the form "+00:00" correctly (the colon is too much apparently)
    # so until we upgrade we need this hack.
    if "+" in date_str:
        first, tz = date_str.split("+", 1)
        tz = tz.replace(":", "")
        date_str = f"{first}+{tz}"

    date_styles = ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S")
    for styles in date_styles:
        try:
            return datetime.strptime(date_str, styles)
        except ValueError:
            continue

    raise ValueError(f"{date_str} is not a recognized datetime")


def parse_string(value: str) -> str:
    return value


def parse_scalar(value: Any, only_strings: Optional[bool] = False) -> Any:
    """
    Convert a scalar value into the expected value for the SDK.

    :param value: A value to be converted for the SDK.
    :type value: Any

    """
    if isinstance(value, (list, tuple, set)):
        return tuple(map(partial(parse_scalar, only_strings=only_strings), value))

    if isinstance(value, str) and not only_strings:
        try:
            if not only_strings:
                date_scalar = parse_datetime(value)
                return date_scalar
        except ValueError:
            return parse_string(value)
    elif isinstance(value, str):
        return parse_string(value)

    return value


def parse_exp(value: Any) -> Any:
    """
    Takes a legacy expression and converts it to an equivalent SDK Expression.

    :param value: A legacy expression.
    :type value: Any

    """
    if isinstance(value, str):
        # Legacy sends raw strings in single quotes, so strip enclosing quotes only
        if not value:
            return value
        elif value.startswith("'") and value.endswith("'"):
            value = value[1:-1]
            return parse_string(value)

        return Column(value)
    elif not isinstance(value, list):
        return parse_scalar(value)

    if is_condition(value):
        return parse_condition_to_function(value)

    alias = value[2] if len(value) > 2 else None
    if alias and alias.startswith("`") and alias.endswith("`"):
        alias = alias[1:-1]

    if value[0].endswith("()") and not value[1]:
        return Function(value[0].strip("()"), [], alias)
    if not value[0].endswith(")") and not value[1]:
        # ["count", None, "count"]
        return Function(value[0], [], alias)

    children = None
    if isinstance(value[1], list):
        children = list(map(parse_exp, value[1]))
    elif value[1]:
        children = [parse_exp(value[1])]

    return Function(value[0], children, alias)


def parse_extension_condition(
    col: str, values: Any, always_in: bool = False
) -> Optional[Condition]:
    """
    Create an SDK condition using the values passed as extensions in the
    legacy API.

    :param col: The column that the automatic condition applies too.
    :type col: str
    :param values: The RHS values of the condition. Could be a single scalar
        or a sequence of values.
    :type values: Any
    :param always_in: Some conditions always use an IN condition, even if there is a single value.
    :type always_in: bool

    """

    column = Column(col)
    if isinstance(values, int):
        if always_in:
            values = (values,)
        else:
            return Condition(column, Op.EQ, values)

    if isinstance(values, (list, tuple)):
        rhs: Sequence[Any] = tuple(map(parse_scalar, values))
        return Condition(column, Op.IN, rhs)

    return None


def _parse_condition_parts(cond: Sequence[Any]) -> tuple[Any, Op, Any]:
    """
    Parse the parts of a legacy condition and return them.

    :param cond: A legacy condition array.
    :type cond: Sequence[Any]
    """
    rhs = None
    lhs = parse_exp(cond[0])
    if cond[1] not in ["IS NULL", "IS NOT NULL"]:
        only_strings = False
        if isinstance(lhs, Column) and (
            lhs.subscriptable == "tags" or lhs.name == "release"
        ):
            only_strings = True
        elif (
            isinstance(lhs, Function)
            and lhs.function == "ifNull"
            and lhs.parameters
            and len(lhs.parameters) > 1
        ):
            first = lhs.parameters[0]
            if isinstance(first, Column) and (
                first.subscriptable == "tags" or first.name == "release"
            ):
                only_strings = True

        rhs = parse_scalar(cond[2], only_strings=only_strings)

    return (lhs, Op(cond[1]), rhs)


def parse_condition(cond: Sequence[Any]) -> Condition:
    """
    Convert a legacy condition into an SDK condition.

    :param cond: A legacy condition array.
    :type cond: Sequence[Any]

    """
    lhs, op, rhs = _parse_condition_parts(cond)
    return Condition(lhs, op, rhs)


def parse_condition_to_function(cond: Sequence[Any]) -> Function:
    lhs, op, rhs = _parse_condition_parts(cond)
    return Function(OPERATOR_TO_FUNCTION[op].value, (lhs, rhs))


def json_to_snql(body: Mapping[str, Any], entity: str) -> Query:
    """
    This will output a Query object that matches the Legacy query body that was passed in.
    The entity is necessary since the SnQL API requires an explicit entity. This doesn't
    support subquery or joins.

    :param body: The legacy API body.
    :type body: Mapping[str, Any]
    :param entity: The name of the entity being queried.
    :type entity: str

    :raises InvalidExpressionError, InvalidQueryError: If the legacy body is invalid, the SDK will
        raise an exception.

    """

    dataset = body.get("dataset") or entity
    sample = body.get("sample")
    if sample is not None:
        sample = float(sample)
    query = Query(dataset, Entity(entity, None, sample))

    selected_columns = []
    for a in body.get("aggregations", []):
        selected_columns.append(parse_exp(list(a)))

    selected = []
    for s in body.get("selected_columns", []):
        if isinstance(s, tuple):
            selected.append(list(s))
        else:
            selected.append(s)

    selected_columns.extend(list(map(parse_exp, selected)))

    arrayjoin = body.get("arrayjoin")
    if arrayjoin:
        query = query.set_array_join([Column(arrayjoin)])

    query = query.set_select(selected_columns)

    groupby = body.get("groupby", [])
    if groupby and not isinstance(groupby, list):
        groupby = [groupby]

    parsed_groupby = []
    for g in groupby:
        if isinstance(g, tuple):
            g = list(g)
        parsed_groupby.append(parse_exp(g))
    query = query.set_groupby(parsed_groupby)

    conditions: list[Union[Or, Condition]] = []
    if body.get("organization"):
        org_cond = parse_extension_condition("org_id", body["organization"])
        if org_cond:
            conditions.append(org_cond)

    assert isinstance(query.match, Entity)
    time_column = get_required_time_column(query.match.name)
    if time_column:
        time_cols = (("from_date", Op.GTE), ("to_date", Op.LT))
        for col, op in time_cols:
            date_val = body.get(col)
            if date_val:
                conditions.append(
                    Condition(Column(time_column), op, parse_datetime(date_val))
                )

    if body.get("project"):
        proj_cond = parse_extension_condition("project_id", body["project"], True)
        if proj_cond:
            conditions.append(proj_cond)

    for cond in body.get("conditions", []):
        if not is_condition(cond):
            or_conditions = []
            for or_cond in cond:
                or_conditions.append(parse_condition(or_cond))

            if len(or_conditions) > 1:
                conditions.append(Or(or_conditions))
            else:
                conditions.extend(or_conditions)
        else:
            conditions.append(parse_condition(cond))

    query = query.set_where(conditions)

    having: list[Union[Or, Condition]] = []
    for cond in body.get("having", []):
        if not is_condition(cond):
            or_conditions = []
            for or_cond in cond:
                or_conditions.append(parse_condition(or_cond))

            having.append(Or(or_conditions))
        else:
            having.append(parse_condition(cond))

    query = query.set_having(having)

    order_by = body.get("orderby")
    if order_by:
        if not isinstance(order_by, list):
            order_by = [order_by]

        order_bys = []
        for o in order_by:
            direction = Direction.ASC
            if isinstance(o, list):
                first = o[0]
                if isinstance(first, str) and first.startswith("-"):
                    o[0] = first.lstrip("-")
                    direction = Direction.DESC
                part = parse_exp(o)
            elif isinstance(o, str):
                if o.startswith("-"):
                    direction = Direction.DESC
                    part = parse_exp(o.lstrip("-"))
                else:
                    part = parse_exp(o)

            order_bys.append(OrderBy(part, direction))

        query = query.set_orderby(order_bys)

    limitby = body.get("limitby")
    if limitby:
        limit, name = limitby
        query = query.set_limitby(LimitBy([Column(name)], int(limit)))

    extras = (
        "limit",
        "offset",
        "granularity",
        "totals",
        "consistent",
        "turbo",
        "debug",
        "dry_run",
        "parent_api",
    )
    for extra in extras:
        if body.get(extra) is not None:
            query = getattr(query, f"set_{extra}")(body.get(extra))

    query.set_legacy(True)
    return query

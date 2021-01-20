from datetime import datetime
from typing import Any, List, Mapping, Sequence, Union

from snuba_sdk.conditions import Condition, Op
from snuba_sdk.entity import Entity
from snuba_sdk.expressions import (
    Column,
    Direction,
    Function,
    LimitBy,
    OrderBy,
)
from snuba_sdk.query import Query
from snuba_sdk.query_visitors import InvalidQuery


def parse_datetime(date_str: str) -> datetime:
    return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%f")


def json_to_snql(body: Mapping[str, Any], entity: str) -> Query:
    def to_exp(value: Union[str, List[Any]]) -> Union[Column, Function]:
        if not isinstance(value, list):
            return Column(value)

        children = []
        if isinstance(value[1], list):
            children = list(map(to_exp, value[1]))
        elif value[1]:
            children = [to_exp(value[1])]

        alias = value[2] if len(value) > 2 else None
        return Function(value[0], children, alias)

    def to_scalar(value: Any) -> Any:
        if isinstance(value, list):
            return list(map(to_scalar, value))
        elif isinstance(value, tuple):
            return tuple(map(to_scalar, value))

        if isinstance(value, str):
            try:
                date_scalar = parse_datetime(value)
                return date_scalar
            except ValueError:
                return value

        return value

    dataset = body.get("dataset", "")
    query = Query(dataset, Entity(entity))

    selected_columns = list(map(to_exp, body.get("selected_columns", [])))
    for a in body.get("aggregations", []):
        if a[0].endswith("()") and not a[1]:
            selected_columns.append(Function(a[0].strip("()"), [], a[2]))
        else:
            if "(" in a[0] or ")" in a[0]:
                raise InvalidQuery(f"SnQL does not support infix expressions: '{a[0]}'")
            agg = to_exp(a)
            selected_columns.append(agg)

    arrayjoin = body.get("arrayjoin")
    if arrayjoin:
        selected_columns.append(Function("arrayJoin", [Column(arrayjoin)], arrayjoin))

    query = query.set_select(selected_columns)

    groupby = body.get("groupby", [])
    if groupby and not isinstance(groupby, list):
        groupby = [groupby]

    query = query.set_groupby(list(map(to_exp, groupby)))

    conditions = []
    for cond in body.get("conditions", []):
        if len(cond) != 3 or not isinstance(cond[1], str):
            raise InvalidQuery("OR conditions not supported yet")

        conditions.append(Condition(to_exp(cond[0]), Op(cond[1]), to_scalar(cond[2])))

    extra_conditions = ["project", "organization"]
    for cond in extra_conditions:
        column = Column(f"{cond}_id")
        values = body.get(cond)
        if isinstance(values, int):
            conditions.append(Condition(column, Op.EQ, values))
        elif isinstance(values, list):
            rhs: Sequence[Any] = list(map(to_scalar, values))
            conditions.append(Condition(column, Op.IN, rhs))
        elif isinstance(values, tuple):
            rhs = tuple(map(to_scalar, values))
            conditions.append(Condition(column, Op.IN, rhs))

    date_conds = [("from_date", Op.GT), ("to_date", Op.LTE)]
    for cond, op in date_conds:
        date_str = body.get(cond, "")
        if date_str:
            # HACK: This is to get sessions working quickly.
            # The time column should depend on the entity.
            conditions.append(
                Condition(Column("started"), op, parse_datetime(date_str))
            )

    query = query.set_where(conditions)

    having = []
    for cond in body.get("having", []):
        if len(cond) != 3 or not isinstance(cond[1], str):
            raise InvalidQuery("OR conditions not supported yet")

        having.append(Condition(to_exp(cond[0]), Op(cond[1]), to_scalar(cond[2])))

    query = query.set_having(having)

    order_by = body.get("orderby")
    if order_by:
        if not isinstance(order_by, list):
            order_by = [order_by]

        order_bys = []
        for o in order_by:
            direction = Direction.DESC if o.startswith("-") else Direction.ASC
            order_bys.append(OrderBy(Column(o.lstrip("-")), direction))

        query = query.set_orderby(order_bys)

    limitby = body.get("limitby")
    if limitby:
        limit, name = limitby
        query = query.set_limitby(LimitBy(Column(name), int(limit)))

    extras = ("limit", "offset", "granularity", "totals")
    for extra in extras:
        if body.get(extra) is not None:
            query = getattr(query, f"set_{extra}")(body.get(extra))

    # TODO: Sample clause
    return query

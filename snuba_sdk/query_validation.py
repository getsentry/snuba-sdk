from __future__ import annotations

from typing import List, Optional, Sequence, Set, Tuple, Union

# Import the modules due to sphinx autodoc problems
# https://github.com/agronholm/sphinx-autodoc-typehints#dealing-with-circular-imports
from snuba_sdk import query as main
from snuba_sdk import query_visitors as qvisitors
from snuba_sdk.column import Column
from snuba_sdk.conditions import (
    BooleanCondition,
    Condition,
    Op,
    get_top_level_conditions,
)
from snuba_sdk.entity import Entity
from snuba_sdk.expressions import Expression
from snuba_sdk.function import CurriedFunction
from snuba_sdk.relationships import Join
from snuba_sdk.schema import Column as ColumnModel


class InvalidMatchError(Exception):
    pass


def validate_match(
    query: main.Query, column_finder: qvisitors.ExpressionSearcher
) -> None:
    """
    Perform validation related to the match clause of the query. Currently that takes two forms:
    validate that the columns referenced in the query are valid columns based on the match clause,
    and ensure that the query has the appropriate conditions based on the entities required columns.

    :param query: The query to be validated.
    :type query: main.Query
    :param column_finder: An ExpressionSearcher used to find all the columns in the query.
    :type column_finder: ExpressionSearcher
    """
    all_columns = column_finder.visit(query)
    if isinstance(query.match, main.Query):
        validate_subquery(query.match, all_columns)
        validate_match(query.match, column_finder)
    elif isinstance(query.match, Join):
        validate_join(query.match, all_columns)
    else:
        validate_entity(query.match, all_columns)

    validate_required_columns(query)


def validate_subquery(match: main.Query, all_columns: Set[Expression]) -> None:
    """
    Validate that the outer query is only referencing columns in the inner query.

    :param match: The inner query of the query being validated.
    :type match: main.Query
    :param all_columns: All the columns referenced in the outer query.
    :type all_columns: Set[Expression]
    :raises InvalidQueryError
    """
    inner_match = set()
    assert match.select is not None
    for s in match.select:
        if isinstance(s, CurriedFunction):
            inner_match.add(s.alias)
        elif isinstance(s, Column):
            inner_match.add(s.name)

    for c in all_columns:
        if isinstance(c, Column) and c.name not in inner_match:
            raise InvalidMatchError(
                f"outer query is referencing column '{c.name}' that does not exist in subquery"
            )


def validate_join(match: Join, all_columns: Set[Expression]) -> None:
    """
    Validate that all the columns in the query are referencing an entity
    in the match, and that there is no alias shadowing.

    :param match: The Join for this query
    :type match: Join
    :param all_columns: The set of all columns referenced in the query
    :type all_columns: Set[Expression]
    :raises InvalidMatchError
    """
    entity_aliases = {alias: entity for alias, entity in match.get_alias_mappings()}
    for c in all_columns:
        assert isinstance(c, Column)
        if c.entity is None:
            raise InvalidMatchError(f"column '{c.name}' must have a qualifying entity")
        elif c.entity.alias not in entity_aliases:
            raise InvalidMatchError(
                f"column '{c.name}' has unknown entity alias {c.entity.alias}"
            )
        elif entity_aliases[c.entity.alias] != c.entity.name:
            raise InvalidMatchError(
                f"column '{c.name}' has incorrect alias for entity {c.entity}: '{c.entity.alias}'"
            )


def validate_entity(match: Entity, all_columns: Set[Expression]) -> None:
    """
    Perform the checks to validate the match entity:

    Ensure that all the columns referenced in the query are in the data model for that entity.

    :param match: The Entity of the query.
    :type match: Entity
    :param all_columns: All the columns referenced in the query.
    :type all_columns: Set[Expression]
    """
    for column in all_columns:
        assert isinstance(column, Column)
        column.validate_data_model(match)


class RequiredColumnError(Exception):
    pass


def _check_entity_has_required_columns(
    entity: Entity, conditions: Optional[Sequence[Union[BooleanCondition, Condition]]]
) -> None:
    """
    For a given entity, flatten the AND conditions to find just the top level ones and
    make sure there is a correct condition for each required column and the required
    time column. Conditions must be on a particular op(s). This doesn't handle ORs
    completely, since it's possible to combine ORs in a way to have valid conditions.

    :param entity: The entity to check for required columns
    :type entity: Entity
    :param conditions: The conditions of the query
    :type conditions: Optional[Sequence[Union[BooleanCondition, Condition]]]
    :raises RequiredColumnError
    """
    if not entity.data_model:
        return

    if not conditions:
        raise RequiredColumnError("where clause is missing required columns")

    schema = entity.data_model
    top_level = get_top_level_conditions(conditions)

    required_matches: List[Tuple[Set[Op], ColumnModel]] = [
        *[({Op.EQ, Op.IN}, req_column) for req_column in schema.required_columns],
    ]
    not_matched = set(range(len(required_matches)))
    for i, match in enumerate(required_matches):
        if i not in not_matched:
            continue

        ops, col_to_match = match
        if _find_matching_condition(ops, col_to_match, top_level, entity.alias):
            not_matched.remove(i)

    if not_matched:
        entity_alias = f"{entity.alias}." if entity.alias else ""
        missing = sorted(
            [f"'{entity_alias}{required_matches[i][1].name}'" for i in not_matched]
        )
        s = "s" if len(missing) > 1 else ""
        raise RequiredColumnError(
            f"where clause is missing required condition{s} on column{s} {', '.join(missing)}"
        )

    ops = {Op.GTE, Op.LT}
    missing_ops = set()
    for op in ops:
        if not _find_matching_condition(
            {op}, schema.required_time_column, top_level, entity.alias
        ):
            missing_ops.add(op)

    if missing_ops:
        s = "s" if len(missing_ops) > 1 else ""
        entity_alias = f"{entity.alias}." if entity.alias else ""
        exc_value = [f"{op.value}" for op in missing_ops]
        raise RequiredColumnError(
            f"where clause is missing required {', '.join(exc_value)} condition{s} on column '{entity_alias}{schema.required_time_column.name}'"
        )


def _find_matching_condition(
    ops: Set[Op],
    col_to_match: ColumnModel,
    top_level_conditions: Sequence[BooleanCondition | Condition],
    entity_alias: Optional[str],
) -> bool:
    for cond in top_level_conditions:
        if (
            isinstance(cond, Condition)
            and isinstance(cond.lhs, Column)
            and (cond.lhs.entity is None or cond.lhs.entity.alias == entity_alias)
            and cond.lhs.name == col_to_match.name
            and cond.op in ops
        ):
            return True

    return False


def validate_required_columns(query: main.Query) -> None:
    """
    Ensure that all the required columns for the entities referenced in the query
    are in the conditions of the query.
    Time column checks require a >= condition and a < condition.
    Required columns require either a = condition or an IN condition.
    This is a very naive check at the moment. It does not check that the condition has
    the correct type and it assumes required columns are top level conditions. It
    also will not work correctly if a subscriptable column is used as a required column.

    :param query: The Query to check.
    :type query: main.Query
    """
    if isinstance(query.match, main.Query):
        validate_required_columns(query.match)
    elif isinstance(query.match, Entity):
        _check_entity_has_required_columns(query.match, query.where)
    elif isinstance(query.match, Join):
        for entity in query.match.get_entities():
            _check_entity_has_required_columns(entity, query.where)

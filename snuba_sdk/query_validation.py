from __future__ import annotations

from typing import Set

# Import the modules due to sphinx autodoc problems
# https://github.com/agronholm/sphinx-autodoc-typehints#dealing-with-circular-imports
from snuba_sdk import query as main
from snuba_sdk import query_visitors as qvisitors
from snuba_sdk.column import Column
from snuba_sdk.entity import Entity
from snuba_sdk.expressions import Expression
from snuba_sdk.function import CurriedFunction
from snuba_sdk.relationships import Join


class InvalidMatchError(Exception):
    pass


def validate_match(
    query: main.Query, column_finder: qvisitors.ExpressionSearcher
) -> None:
    """
    Perform validation related to the match clause of the query. Currently that takes two forms:
    validate that the columns referenced in the query are valid columns based on the match clause.

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

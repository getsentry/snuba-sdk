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
    :param column_finder: An ExpressionSearcher used to find all the columns in the query.
    """
    all_columns = column_finder.visit(query)
    if isinstance(query.match, main.Query):
        _validate_subquery(query.match, all_columns)
        validate_match(query.match, column_finder)
    elif isinstance(query.match, Join):
        _validate_join(query.match, all_columns)
    else:
        _validate_entity(query.match, all_columns)


def _validate_subquery(match: main.Query, all_columns: Set[Expression]) -> None:
    """
    Validate that the outer query is only referencing columns in the inner query.

    :param match: The inner query of the query being validated.
    :param all_columns: All the columns referenced in the outer query.
    :raises InvalidQueryError
    """
    inner_column_names = set()
    assert match.select is not None
    for s in match.select:
        # If the inner query has a function in its SELECT, then it is possible for the
        # outer query to reference that function using the alias.
        if isinstance(s, CurriedFunction):
            inner_column_names.add(s.alias)
        elif isinstance(s, Column):
            inner_column_names.add(s.name)

    for c in all_columns:
        if isinstance(c, Column) and c.name not in inner_column_names:
            raise InvalidMatchError(
                f"outer query is referencing column '{c.name}' that does not exist in subquery"
            )


def _validate_join(match: Join, all_columns: Set[Expression]) -> None:
    """
    Validate that all the columns in the query are referencing an entity
    in the match, and that there is no alias shadowing.

    :param match: The Join for this query
    :param all_columns: The set of all columns referenced in the query
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


def _validate_entity(match: Entity, all_columns: Set[Expression]) -> None:
    """
    Perform the checks to validate the match entity:

    Ensure that all the columns referenced in the query are in the data model for that entity.

    :param match: The Entity of the query.
    :param all_columns: All the columns referenced in the query.
    """
    for column in all_columns:
        assert isinstance(column, Column)
        column.validate_data_model(match)

from dataclasses import dataclass
from typing import Any, MutableMapping, Sequence, Set, Tuple

from snuba_sdk.entity import Entity
from snuba_sdk.expressions import Expression, InvalidExpression


@dataclass(frozen=True)
class Relationship(Expression):
    """
    A representation of a relationship between two Entities. The relationship
    name should be defined in the data model of the LHS and entity in Snuba.
    Both Entities must have a valid alias, which will be used to qualify the
    columns in the SnQL query.

    :param lhs: The Entity that owns the relationship.
    :type name: Entity
    :param name: The name of the relationship on the LHS Entity.
    :type name: str
    :param rhs: The Entity connected to the LHS using the relationship.
    :type name: Entity

    :raises InvalidExpression: If the incorrect types are used or if either
        of the Entities does not have an alias.

    """

    lhs: Entity
    name: str
    rhs: Entity

    def validate(self) -> None:
        def valid_entity(e: Any) -> None:
            if not isinstance(e, Entity):
                raise InvalidExpression(f"'{e}' must be an Entity")
            elif e.alias is None:
                raise InvalidExpression(f"{e} must have a valid alias")

        valid_entity(self.lhs)
        valid_entity(self.rhs)

        if not isinstance(self.name, str) or not self.name:
            raise InvalidExpression(f"'{self.name}' is not a valid relationship name")


@dataclass(frozen=True)
class Join(Expression):
    """
    A collection of relationships that is used in the MATCH section of
    the SnQL query. Must contain at least one Relationship, and will make
    sure that Entity aliases are not used by different Entities.

    :param relationships: The relationships in the join.
    :type name: Sequence[Relationship]

    :raises InvalidExpression: If two different Entities are using
        the same alias, this will be raised.

    """

    relationships: Sequence[Relationship]

    def get_alias_mappings(self) -> Set[Tuple[str, str]]:
        aliases = set()
        for rel in self.relationships:
            if rel.lhs.alias is not None:
                aliases.add((rel.lhs.alias, rel.lhs.name))
            if rel.rhs.alias is not None:
                aliases.add((rel.rhs.alias, rel.rhs.name))

        return aliases

    def validate(self) -> None:
        if not isinstance(self.relationships, (list, tuple)) or not self.relationships:
            raise InvalidExpression("Join must have at least one Relationship")
        elif not all(isinstance(x, Relationship) for x in self.relationships):
            raise InvalidExpression("Join expects a list of Relationship objects")

        seen: MutableMapping[str, str] = {}
        for alias, entity in self.get_alias_mappings():
            if alias in seen and seen[alias] != entity:
                entities = sorted([entity, seen[alias]])
                raise InvalidExpression(
                    f"alias '{alias}' is duplicated for entities {', '.join(entities)}"
                )
            seen[alias] = entity

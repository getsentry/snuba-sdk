from dataclasses import dataclass
from typing import Any, MutableMapping, Sequence, Tuple

from snuba_sdk.entity import Entity
from snuba_sdk.expressions import Expression, InvalidExpression


@dataclass(frozen=True)
class Relationship(Expression):
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
    relationships: Sequence[Relationship]

    def get_alias_mappings(self) -> Sequence[Tuple[str, str]]:
        aliases = []
        for rel in self.relationships:
            if rel.lhs.alias is not None:
                aliases.append((rel.lhs.alias, rel.lhs.name))
            if rel.rhs.alias is not None:
                aliases.append((rel.rhs.alias, rel.rhs.name))

        return aliases

    def validate(self) -> None:
        if not isinstance(self.relationships, (list, tuple)) or not self.relationships:
            raise InvalidExpression("Join must have at least one Relationship")
        elif not all(isinstance(x, Relationship) for x in self.relationships):
            raise InvalidExpression("Join expects a list of Relationship objects")

        seen: MutableMapping[str, str] = {}
        for alias, entity in self.get_alias_mappings():
            if alias in seen and seen[alias] != entity:
                raise InvalidExpression(
                    f"alias '{alias}' is duplicated for entities {entity}, {seen[alias]}"
                )
            seen[alias] = entity

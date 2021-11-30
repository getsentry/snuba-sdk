import re
from dataclasses import dataclass, field
from typing import Optional

from snuba_sdk.entity import Entity
from snuba_sdk.expressions import Expression, InvalidExpressionError


class InvalidColumnError(InvalidExpressionError):
    pass


column_name_re = re.compile(r"^[a-zA-Z_](\w|\.|:)*(\[([^\[\]]*)\])?$")


@dataclass(frozen=True)
class Column(Expression):
    """
    A representation of a single column in the database. Columns are
    expected to be alpha-numeric, with '.', '_`, and `:` allowed as well.
    If the column is subscriptable then you can specify the column in the
    form `subscriptable[key]`. The `subscriptable` attribute will contain the outer
    column and `key` will contain the inner key.

    :param name: The column name.
    :type name: str
    :param entity: The entity for that column
    :type name: Optional[Entity]

    :raises InvalidColumnError: If the column name is not a string or has an
        invalid format.

    """

    name: str
    entity: Optional[Entity] = None
    subscriptable: Optional[str] = field(init=False, default=None)
    key: Optional[str] = field(init=False, default=None)

    def validate(self) -> None:
        if not isinstance(self.name, str):
            raise InvalidColumnError(f"column '{self.name}' must be a string")
        if not column_name_re.match(self.name):
            raise InvalidColumnError(
                f"column '{self.name}' is empty or contains invalid characters"
            )

        if self.entity is not None:
            if not isinstance(self.entity, Entity):
                raise InvalidColumnError(f"column '{self.name}' expects an Entity")
            if not self.entity.alias:
                raise InvalidColumnError(
                    f"column '{self.name}' expects an Entity with an alias"
                )

            self.validate_data_model(self.entity)

        # If this is a subscriptable set these values to help with debugging etc.
        # Because this is frozen we can't set the value directly.
        if "[" in self.name:
            subscriptable, key = self.name.split("[", 1)
            key = key.strip("]")
            super().__setattr__("subscriptable", subscriptable)
            super().__setattr__("key", key)

    def validate_data_model(self, entity: Entity) -> None:
        if entity.data_model is None:
            return

        to_check = self.subscriptable if self.subscriptable else self.name
        if not entity.data_model.contains(to_check):
            raise InvalidColumnError(
                f"entity '{entity.name}' does not support the column '{self.name}'"
            )

import re
from dataclasses import dataclass, field
from typing import Optional

from snuba_sdk.entity import Entity
from snuba_sdk.expressions import Expression, InvalidExpression


class InvalidColumn(InvalidExpression):
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

    :raises InvalidColumn: If the column name is not a string or has an
        invalid format.

    """

    name: str
    entity: Optional[Entity] = None
    subscriptable: Optional[str] = field(init=False, default=None)
    key: Optional[str] = field(init=False, default=None)

    def validate(self) -> None:
        if not isinstance(self.name, str):
            raise InvalidColumn(f"column '{self.name}' must be a string")
            self.name = str(self.name)
        if not column_name_re.match(self.name):
            raise InvalidColumn(
                f"column '{self.name}' is empty or contains invalid characters"
            )
        if self.entity is not None:
            if not isinstance(self.entity, Entity):
                raise InvalidColumn(f"column {self.name} expects an Entity")
            if not self.entity.alias:
                raise InvalidColumn(
                    f"column {self.name} expects an Entity with an alias"
                )

        # If this is a subscriptable set these values to help with debugging etc.
        # Because this is frozen we can't set the value directly.
        if "[" in self.name:
            subscriptable, key = self.name.split("[", 1)
            key = key.strip("]")
            super().__setattr__("subscriptable", subscriptable)
            super().__setattr__("key", key)

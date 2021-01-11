from __future__ import annotations

import re
from dataclasses import dataclass

from snuba_sdk.expressions import Expression


entity_name_re = re.compile(r"[a-zA-Z_]+")


class InvalidEntity(Exception):
    pass


@dataclass(frozen=True)
class Entity(Expression):
    name: str

    def validate(self) -> None:
        # TODO: There should be a whitelist of entity names at some point
        if not entity_name_re.match(self.name):
            raise InvalidEntity(f"{self.name} is not a valid entity name")

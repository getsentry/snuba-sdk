import re
from dataclasses import dataclass
from typing import Optional, Union

from snuba_sdk.expressions import Expression


entity_name_re = re.compile(r"^[a-zA-Z_]+$")


class InvalidEntity(Exception):
    pass


@dataclass(frozen=True)
class Entity(Expression):
    name: str
    sample: Optional[Union[int, float]] = None

    def validate(self) -> None:
        # TODO: There should be a whitelist of entity names at some point
        if not isinstance(self.name, str) or not entity_name_re.match(self.name):
            raise InvalidEntity(f"{self.name} is not a valid entity name")

        if self.sample is not None:
            if not isinstance(self.sample, (int, float)):
                raise InvalidEntity(
                    "sample must be a float between 0 and 1 or an integer greater than 1"
                )
            elif isinstance(self.sample, float):
                if self.sample < 0.0 or self.sample > 1.0:
                    raise InvalidEntity(
                        "float samples must be between 0.0 and 1.0 (%age of rows)"
                    )
            elif isinstance(self.sample, int):
                if self.sample < 1:
                    raise InvalidEntity("int samples must be at least 1 (# of rows)")

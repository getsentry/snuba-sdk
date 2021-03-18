import re
from dataclasses import dataclass
from typing import Optional

from snuba_sdk.expressions import Expression


entity_name_re = re.compile(r"^[a-zA-Z_]+$")


class InvalidEntity(Exception):
    pass


@dataclass(frozen=True)
class Entity(Expression):
    name: str
    alias: Optional[str] = None
    sample: Optional[float] = None

    def validate(self) -> None:
        # TODO: There should be a whitelist of entity names at some point
        if not isinstance(self.name, str) or not entity_name_re.match(self.name):
            raise InvalidEntity(f"'{self.name}' is not a valid entity name")

        if self.sample is not None:
            if not isinstance(self.sample, float):
                raise InvalidEntity("sample must be a float")
            elif self.sample <= 0.0:
                raise InvalidEntity("samples must be greater than 0.0")

        if self.alias is not None:
            if not isinstance(self.alias, str) or not self.alias:
                raise InvalidEntity(f"'{self.alias}' is not a valid alias")


# TODO: This should be handled by the users of the SDK, not the SDK itself.
ENTITY_TIME_COLUMNS = {
    "discover": "timestamp",
    "errors": "timestamp",
    "events": "timestamp",
    "discover_events": "timestamp",
    "outcomes": "timestamp",
    "outcomes_raw": "timestamp",
    "sessions": "started",
    "transactions": "finish_ts",
    "discover_transactions": "finish_ts",
    "discover_events": "timestamp",
    "spans": "finish_ts",
}


def get_required_time_column(entity_name: str) -> Optional[str]:
    return ENTITY_TIME_COLUMNS.get(entity_name)

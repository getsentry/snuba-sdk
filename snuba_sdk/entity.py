import re
from dataclasses import dataclass, field
from typing import Optional

from snuba_sdk.expressions import Expression
from snuba_sdk.schema import DataModel

entity_name_re = re.compile(r"^[a-zA-Z_]+$")


class InvalidEntityError(Exception):
    pass


@dataclass(frozen=True, repr=False)
class Entity(Expression):
    name: str
    alias: Optional[str] = None
    sample: Optional[float] = None
    data_model: Optional[DataModel] = field(hash=False, default=None)

    def validate(self) -> None:
        # TODO: There should be a whitelist of entity names at some point
        if not isinstance(self.name, str) or not entity_name_re.match(self.name):
            raise InvalidEntityError(f"'{self.name}' is not a valid entity name")

        if self.sample is not None:
            if not isinstance(self.sample, float):
                raise InvalidEntityError("sample must be a float")
            elif self.sample <= 0.0:
                raise InvalidEntityError("samples must be greater than 0.0")

        if self.alias is not None:
            if not isinstance(self.alias, str) or not self.alias:
                raise InvalidEntityError(f"'{self.alias}' is not a valid alias")

        if self.data_model is not None:
            if not isinstance(self.data_model, DataModel):
                raise InvalidEntityError("data_model must be an instance of DataModel")

    def __repr__(self) -> str:
        alias = f", alias='{self.alias}'" if self.alias is not None else ""
        sample = f", sample={self.sample}" if self.sample is not None else ""
        return f"Entity('{self.name}'{alias}{sample})"


# TODO: This should be handled by the users of the SDK, not the SDK itself.
ENTITY_TIME_COLUMNS = {
    "discover": "timestamp",
    "discover_events": "timestamp",
    "discover_transactions": "finish_ts",
    "errors": "timestamp",
    "events": "timestamp",
    "outcomes": "timestamp",
    "outcomes_raw": "timestamp",
    "replays": "timestamp",
    "search_issues": "timestamp",
    "sessions": "started",
    "spans": "finish_ts",
    "transactions": "finish_ts",
}


def get_required_time_column(entity_name: str) -> Optional[str]:
    return ENTITY_TIME_COLUMNS.get(entity_name)

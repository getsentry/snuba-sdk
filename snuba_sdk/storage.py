import re
from dataclasses import dataclass, field
from typing import Optional

from snuba_sdk.expressions import Expression
from snuba_sdk.schema import DataModel

storage_name_re = re.compile(r"^[a-zA-Z_]+$")


class InvalidStorageError(Exception):
    pass


@dataclass(frozen=True, repr=False)
class Storage(Expression):
    name: str
    sample: Optional[float] = None
    data_model: Optional[DataModel] = field(hash=False, default=None)

    def validate(self) -> None:
        if not isinstance(self.name, str) or not storage_name_re.match(self.name):
            raise InvalidStorageError(f"'{self.name}' is not a valid storage name")

        if self.sample is not None:
            if not isinstance(self.sample, float):
                raise InvalidStorageError("sample must be a float")
            elif self.sample <= 0.0:
                raise InvalidStorageError("samples must be greater than 0.0")

        if self.data_model is not None:
            if not isinstance(self.data_model, DataModel):
                raise InvalidStorageError("data_model must be an instance of DataModel")

    def __repr__(self) -> str:
        sample = f", sample={self.sample}" if self.sample is not None else ""
        return f"STORAGE('{self.name}'{sample})"

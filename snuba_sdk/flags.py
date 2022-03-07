from __future__ import annotations

import re
from dataclasses import dataclass

from snuba_sdk.expressions import Expression, InvalidExpressionError


@dataclass(frozen=True)
class BooleanFlag(Expression):
    value: bool = False
    name: str = ""

    def validate(self) -> None:
        if not isinstance(self.value, bool):
            raise InvalidExpressionError(f"{self.name} must be a boolean")

    def __bool__(self) -> bool:
        return self.value

    def __str__(self) -> str:
        return str(self.value)


@dataclass(frozen=True)
class Totals(BooleanFlag):
    name: str = "totals"


@dataclass(frozen=True)
class Consistent(BooleanFlag):
    name: str = "consistent"


@dataclass(frozen=True)
class Turbo(BooleanFlag):
    name: str = "turbo"


@dataclass(frozen=True)
class Debug(BooleanFlag):
    name: str = "debug"


@dataclass(frozen=True)
class DryRun(BooleanFlag):
    name: str = "dry_run"


@dataclass(frozen=True)
class Legacy(BooleanFlag):
    name: str = "legacy"


FLAG_RE = re.compile(r"^[a-zA-Z0-9_\.\+\*\/:\-\[\]]*$")


@dataclass(frozen=True)
class StringFlag(Expression):
    value: str = ""
    name: str = ""

    def validate(self) -> None:
        if not isinstance(self.value, str) or len(self.value) == 0:
            raise InvalidExpressionError(f"{self.name} must be a non-empty string")
        elif not FLAG_RE.match(self.value):
            raise InvalidExpressionError(f"{self.name} contains invalid characters")

    def __str__(self) -> str:
        return str(self.value)


@dataclass(frozen=True)
class ParentAPI(StringFlag):
    name: str = "parent_api"

    def validate(self) -> None:
        if not isinstance(self.value, str) or self.value == "":
            raise InvalidExpressionError(f"{self.name} must be a non-empty string")


@dataclass(frozen=True)
class Team(StringFlag):
    name: str = "team"


@dataclass(frozen=True)
class Feature(StringFlag):
    name: str = "feature"

from dataclasses import dataclass

from snuba_sdk import Expression


@dataclass(frozen=True)
class Entity(Expression):
    name: str

    def validate(self) -> None:
        # TODO: Validate something about this
        return

    def translate(self) -> str:
        return f"{self}"

    def __repr__(self) -> str:
        return f"{self.name}"

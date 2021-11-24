from dataclasses import dataclass, field
from typing import Iterator, Sequence, Set


@dataclass(frozen=True)
class Column:
    name: str


@dataclass(frozen=True, init=False)
class ColumnSet:
    column_names: Set[str]
    columns: Set[Column] = field(init=False, default_factory=set)

    def __init__(self, columns: Sequence[Column]) -> None:
        super().__setattr__("column_names", set(c.name for c in columns))
        super().__setattr__("columns", set(columns))

    def contains(self, column_name: str) -> bool:
        return column_name in self.column_names

    def __iter__(self) -> Iterator[Column]:
        for col in self.columns:
            yield col


@dataclass(frozen=True, init=False)
class EntityModel:
    column_set: ColumnSet

    def __init__(
        self,
        columns: Sequence[Column],
    ) -> None:
        super().__setattr__("column_set", ColumnSet(columns))

    def contains(self, column_name: str) -> bool:
        return self.column_set.contains(column_name)

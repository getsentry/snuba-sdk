from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterator, Sequence


@dataclass(frozen=True)
class Column:
    name: str
    required: bool = False


@dataclass(frozen=True, init=False)
class ColumnSet:
    column_names: set[str]
    columns: set[Column] = field(init=False, default_factory=set)

    def __init__(self, columns: Sequence[Column]) -> None:
        super().__setattr__("column_names", set(c.name for c in columns))
        super().__setattr__("columns", set(columns))
        super().__setattr__(
            "required_columns", set([r for r in self.columns if r.required])
        )

    def contains(self, column_name: str) -> bool:
        return column_name in self.column_names

    def __iter__(self) -> Iterator[Column]:
        for col in self.columns:
            yield col


@dataclass(frozen=True, init=False)
class EntityModel:
    column_set: ColumnSet
    required_time_column: Column
    required_columns: set[Column] = field(init=False, default_factory=set)

    def __init__(
        self,
        columns: Sequence[Column],
        required_time_column: Column,
    ) -> None:
        super().__setattr__("column_set", ColumnSet(columns))
        super().__setattr__("required_time_column", required_time_column)
        super().__setattr__("required_columns", set([r for r in columns if r.required]))

    def contains(self, column_name: str) -> bool:
        return self.column_set.contains(column_name)

from __future__ import annotations

from dataclasses import dataclass


class InvalidMQLContextError(Exception):
    pass


@dataclass
class MQLContext:
    """
    The MQL string alone is not enough to fully describe a query.
    This class contains all of the additional information needed to
    execute a metrics query in snuba.

    It should be noted that this class is used as an intermediary encoding
    class for data in the the MetricsQuery class that can't be encoded into
    MQL. As such it shouldn't be used directly by users of the SDK.

    This also means that the validation here is quite loose, since this object
    should be created exclusively from a valid MetricsQuery object.
    """

    start: str
    end: str
    rollup: dict[str, str | int | None]
    scope: dict[str, str | list[int]]
    indexer_mappings: dict[str, str | int]
    limit: int | None = None
    offset: int | None = None
    extrapolate: bool | None = None

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        # Simple assert that all the expected fields are present
        fields = ["start", "end", "rollup", "scope", "indexer_mappings"]
        for field in fields:
            if getattr(self, field) is None:
                raise InvalidMQLContextError(f"MQLContext.{field} is required")

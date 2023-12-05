from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import datetime
from typing import Any, Mapping, Sequence

from snuba_sdk.expressions import Limit, Offset
from snuba_sdk.mql_context_visitors import MQLContextPrinter, Validator
from snuba_sdk.timeseries import MetricsScope, Rollup


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
    should be created from a MetricsQuery object, which has already been validated.
    """

    entity: str
    start: datetime
    end: datetime
    rollup: Rollup
    scope: MetricsScope
    indexer_mappings: dict[str, Any]
    limit: Limit | None = None
    offset: Offset | None = None

    def validate(self) -> None:
        fields = ["entity", "start", "end", "rollup", "scope"]
        for field in fields:
            if getattr(self, field) is None:
                raise InvalidMQLContextError(f"{field} is required for a MQL context")

        if not isinstance(indexer_mapping, dict):
            raise InvalidMQLContextError("indexer_mapping must be a dictionary")

    def serialize(self) -> Mapping[str, Any]:
        self.validate()
        result = MQL_CONTEXT_PRINTER.visit(self)
        return result


MQL_CONTEXT_PRINTER = MQLContextPrinter()
VALIDATOR = Validator()

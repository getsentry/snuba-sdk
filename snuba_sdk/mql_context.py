from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import datetime
from typing import Any, Sequence

from snuba_sdk.expressions import Limit, Offset
from snuba_sdk.mql_context_visitors import MQLContextPrinter, Validator
from snuba_sdk.timeseries import MetricsScope, Rollup


@dataclass
class MQLContext:
    """ """

    entity: str | None = None
    start: datetime | None = None
    end: datetime | None = None
    rollup: Rollup | None = None
    scope: MetricsScope | None = None
    limit: Limit | None = None
    offset: Offset | None = None
    indexer_mappings: dict[str, Any] | None = None

    def get_fields(self) -> Sequence[str]:
        self_fields = fields(self)  # Verified the order in the Python source
        return tuple(f.name for f in self_fields)

    def validate(self) -> None:
        Validator().visit(self)

    def serialize(self) -> dict[str, Any]:
        self.validate()
        result = MQL_CONTEXT_PRINTER.visit(self)
        return result


MQL_CONTEXT_PRINTER = MQLContextPrinter()

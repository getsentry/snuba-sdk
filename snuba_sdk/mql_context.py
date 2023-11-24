from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import datetime
from typing import Any, Mapping, Sequence

from snuba_sdk.expressions import Limit, Offset
from snuba_sdk.mql_context_visitors import MQLContextPrinter, Validator
from snuba_sdk.timeseries import MetricsScope, Rollup


@dataclass
class MQLContext:
    """
    The MQL string alone is not enough to fully describe a query.
    This class contains all of the additional information needed to
    execute a metrics query in snuba.
    """

    entity: str | None = None
    start: datetime | None = None
    end: datetime | None = None
    rollup: Rollup | None = None
    scope: MetricsScope | None = None
    limit: Limit | None = None
    offset: Offset | None = None
    indexer_mappings: dict[str, Any] | None = None

    def get_fields(self) -> Sequence[str]:
        self_fields = fields(self)
        return tuple(f.name for f in self_fields)

    def validate(self) -> None:
        # For now, we cannot validate entity because it unknown when
        # we converting MetricsQuery to MQL. In that specific case, we
        # need to set the entity on the requesst after serialization.

        # In the future, we should be able to remove entity from this class
        # entirely when we join entities together.
        VALIDATOR.visit(self)

    def serialize(self) -> Mapping[str, Any]:
        self.validate()
        result = MQL_CONTEXT_PRINTER.visit(self)
        return result


MQL_CONTEXT_PRINTER = MQLContextPrinter()
VALIDATOR = Validator()

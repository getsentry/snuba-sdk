from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field, fields
from typing import Any, Mapping

from snuba_sdk.query import BaseQuery


class InvalidRequestError(Exception):
    pass


class InvalidFlagError(Exception):
    pass


@dataclass
class Flags:
    totals: bool | None = None
    consistent: bool | None = None
    turbo: bool | None = None
    debug: bool | None = None
    dry_run: bool | None = None
    legacy: bool | None = None

    def validate(self) -> None:
        flags_fields = fields(self)
        for ff in flags_fields:
            flag = getattr(self, ff.name)
            if flag is not None and not isinstance(flag, bool):
                raise InvalidFlagError(f"{ff.name} must be a boolean")

    def to_dict(self) -> dict[str, bool]:
        self.validate()
        values = asdict(self)
        return {f: v for f, v in values.items() if v is not None}


FLAG_RE = re.compile(r"^[a-zA-Z0-9_\.\+\*\/:\-\[\]]*$")


@dataclass
class Request:
    dataset: str
    app_id: str
    query: BaseQuery
    flags: Flags = field(default_factory=Flags)
    parent_api: str = "<unknown>"
    tenant_ids: dict[str, str | int] = field(default_factory=dict)
    mql_context: dict[str, Mapping[str, Any]] | None = None

    def validate(self) -> None:
        if not self.dataset or not isinstance(self.dataset, str):
            raise InvalidRequestError("Request must have a valid dataset")
        elif not FLAG_RE.match(self.dataset):
            raise InvalidRequestError(f"'{self.dataset}' is not a valid dataset")

        if not self.app_id or not isinstance(self.app_id, str):
            raise InvalidRequestError("Request must have a valid app_id")
        if not FLAG_RE.match(self.app_id):
            raise InvalidRequestError(f"'{self.app_id}' is not a valid app_id")

        if not self.parent_api or not isinstance(self.parent_api, str):
            raise InvalidRequestError(f"`{self.parent_api}` is not a valid parent_api")

        if not isinstance(self.tenant_ids, dict):
            raise InvalidRequestError("Request must have a `tenant_ids` dictionary")

        self.query.validate()
        if self.flags is not None:
            self.flags.validate()

    def to_dict(self) -> dict[str, str | bool | dict[str, str | int]]:
        self.validate()
        flags = self.flags.to_dict() if self.flags is not None else {}

        # TODO: Uncomment when we fully support MQL snuba endpoint.
        # if isinstance(self.query, MetricsQuery):
        #     serialized_mql = self.query.serialize_to_mql()
        #     mql_context = serialized_mql["mql_context"]
        #     if self.mql_context:
        #         if "indexer_mappings" in self.mql_context:
        #             mql_context["indexer_mappings"] = self.mql_context["indexer_mappings"]
        #         if "entity" in self.mql_context:
        #             mql_context["entity"] = self.mql_context["entity"]
        #         self.mql_context = mql_context
        #     return {
        #         **flags,
        #         "query": serialized_mql["mql"],
        #         "mql_context": mql_context,
        #         "dataset": self.dataset,
        #         "app_id": self.app_id,
        #         "tenant_ids": self.tenant_ids,
        #         "parent_api": self.parent_api,
        #     }
        return {
            **flags,
            "query": self.query.serialize(),
            "dataset": self.dataset,
            "app_id": self.app_id,
            "tenant_ids": self.tenant_ids,
            "parent_api": self.parent_api,
        }

    def serialize(self) -> str:
        return json.dumps(self.to_dict())

    def __str__(self) -> str:
        return self.serialize()

    def print(self) -> str:
        self.validate()
        output = self.to_dict()
        return json.dumps(output, sort_keys=True, indent=4 * " ")

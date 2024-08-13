from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field, fields
from typing import Mapping

from snuba_sdk.delete_query import DeleteQuery
from snuba_sdk.metrics_query import MetricsQuery
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
    tenant_ids: Mapping[str, str | int] = field(default_factory=dict)

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

    def to_dict(self) -> dict[str, object]:
        self.validate()
        flags = self.flags.to_dict() if self.flags is not None else {}

        mql_context = None
        if isinstance(self.query, MetricsQuery):
            serialized_mql = self.query.serialize()
            assert isinstance(serialized_mql, dict)  # mypy
            mql_context = serialized_mql["mql_context"]
            query = str(serialized_mql["mql"])
        elif isinstance(self.query, DeleteQuery):
            """
            for a DeleteQuery, the query is not a snql/mql string,
            it is a dict
            """
            return {
                **flags,
                "query": self.query.serialize(),
                "app_id": self.app_id,
                "tenant_ids": self.tenant_ids,
                "parent_api": self.parent_api,
            }
        else:
            query = str(self.query.serialize())

        ret: dict[str, object] = {
            **flags,
            "query": query,
            "dataset": self.dataset,
            "app_id": self.app_id,
            "tenant_ids": self.tenant_ids,
            "parent_api": self.parent_api,
        }
        if mql_context is not None:
            ret["mql_context"] = mql_context
        return ret

    def serialize(self) -> str:
        return json.dumps(self.to_dict())

    def serialize_mql(self) -> str:
        # NOTE: This function is temporary, just to help with a cutover in the Sentry codebase.
        # It will be removed in a future version.
        return json.dumps(self.to_dict())

    def __str__(self) -> str:
        return self.serialize()

    def print(self) -> str:
        self.validate()
        output = self.to_dict()
        return json.dumps(output, sort_keys=True, indent=4 * " ")

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Union

from snuba_sdk.query import BaseQuery


class InvalidDeleteQueryError(Exception):
    pass


@dataclass(frozen=True)
class DeleteQuery(BaseQuery):
    """
    This represents a snuba delete query.
    Inputs:
        storage - the storage to delete from
        columnConditions - a mapping from column-name to a list of column values
            that defines the delete conditions. ex:
            {
                "id": [1, 2, 3]
                "status": ["failed"]
            }
            represents
            DELETE FROM ... WHERE id in (1,2,3) AND status='failed'
    Deletes all rows in the given storage, that satisfy the conditions
    defined in 'columnConditions'.
    """

    storage_name: str
    column_conditions: Dict[str, List[Union[str, int]]]

    def validate(self) -> None:
        if "project_id" not in self.column_conditions:
            raise InvalidDeleteQueryError(
                "missing required column condition on 'project_id'"
            )
        elif len(self.column_conditions["project_id"]) == 0:
            raise InvalidDeleteQueryError("column condition on 'project_id' is empty")

    def serialize(self) -> Union[str, Dict[str, Any]]:
        # the body of the request
        self.validate()
        return json.dumps({"columns": self.column_conditions})

    def print(self) -> str:
        return repr(self)

    def __repr__(self) -> str:
        return f"DeleteQuery(storage_name={repr(self.storage_name)}, columnsConditions={repr(self.column_conditions)})"

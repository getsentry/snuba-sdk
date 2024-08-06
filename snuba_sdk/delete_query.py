import json
from dataclasses import dataclass
from typing import Any, Dict, List, Union

from snuba_sdk.query import BaseQuery


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
    column_conditions: Dict[str, List[Any]]

    def validate(self) -> None:
        """
        i dont think we need to do any input validation at the sdk
        level bc inputs will be validated at the endpoint and the proper
        response code returned.
        """
        return

    def serialize(self) -> Union[str, Dict[str, Any]]:
        # the body of the request
        self.validate()
        return json.dumps({"columns": self.column_conditions})

    def print(self) -> str:
        return repr(self)

    def __repr__(self) -> str:
        return f"DeleteQuery(storage_name={repr(self.storage_name)}, columnsConditions={repr(self.column_conditions)})"

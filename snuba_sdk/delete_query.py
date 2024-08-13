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
        if self.column_conditions == {}:
            raise InvalidDeleteQueryError("column conditions cannot be empty")

        for col, values in self.column_conditions.items():
            if len(values) == 0:
                raise InvalidDeleteQueryError(
                    f"column condition '{col}' cannot be empty"
                )

    def serialize(self) -> Union[str, Dict[str, Any]]:
        # the body of the request
        self.validate()
        return {"columns": self.column_conditions}

    def print(self) -> str:
        return repr(self)

    def __repr__(self) -> str:
        return f"DeleteQuery(storage_name={repr(self.storage_name)}, columnsConditions={repr(self.column_conditions)})"

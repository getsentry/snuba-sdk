from __future__ import annotations

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Generic, List, Optional, Set, TypeVar, Union

from snuba_sdk.clickhouse import is_aggregation_function


TVisited = TypeVar("TVisited")


class Visitor(ABC, Generic[TVisited]):
    @abstractmethod
    def visit_column(self, column: Column) -> TVisited:
        raise NotImplementedError

    @abstractmethod
    def visit_function(self, func: Function) -> TVisited:
        raise NotImplementedError

    @abstractmethod
    def visit_int_literal(
        self, literal: int, minn: Optional[int], maxn: Optional[int], name: str
    ) -> TVisited:
        raise NotImplementedError

    @abstractmethod
    def visit_entity(self, entity: Entity) -> TVisited:
        raise NotImplementedError

    @abstractmethod
    def visit_condition(self, cond: Condition) -> TVisited:
        raise NotImplementedError


class Expression(ABC):
    def __post_init__(self) -> None:
        self.accept(Validation())

    @abstractmethod
    def accept(self, visitor: Visitor[TVisited]) -> TVisited:
        raise NotImplementedError


# For type hinting
ScalarType = Union[None, bool, str, bytes, float, int, date, datetime]
# For type checking
Scalar: Set[type] = {type(None), bool, str, bytes, float, int, date, datetime}


@dataclass(frozen=True)
class Limit(Expression):
    limit: int

    def accept(self, visitor: Visitor[TVisited]) -> TVisited:
        return visitor.visit_int_literal(self.limit, 0, 10000, "limit")


@dataclass(frozen=True)
class Offset(Expression):
    offset: int

    def accept(self, visitor: Visitor[TVisited]) -> TVisited:
        return visitor.visit_int_literal(self.offset, 0, 10000, "offset")


@dataclass(frozen=True)
class Granularity(Expression):
    granularity: int

    def accept(self, visitor: Visitor[TVisited]) -> TVisited:
        return visitor.visit_int_literal(self.granularity, 1, None, "granularity")


@dataclass(frozen=True)
class Column(Expression):
    name: str

    def accept(self, visitor: Visitor[TVisited]) -> TVisited:
        return visitor.visit_column(self)


@dataclass(frozen=True)
class Function(Expression):
    function: str
    parameters: List[Union[ScalarType, Column, Function]]
    alias: Optional[str] = None

    def is_aggregate(self) -> bool:
        return is_aggregation_function(self.function)

    def accept(self, visitor: Visitor[TVisited]) -> TVisited:
        return visitor.visit_function(self)


class Op(Enum):
    GT = ">"
    LT = "<"
    GTE = ">="
    LTE = "<="
    EQ = "="
    NEQ = "!="
    IN = "IN"
    NOT_IN = "NOT IN"
    LIKE = "LIKE"
    NOT_LIKE = "NOT LIKE"


@dataclass(frozen=True)
class Condition(Expression):
    lhs: Union[Column, Function]
    op: Op
    rhs: Union[Column, Function, ScalarType]

    def accept(self, visitor: Visitor[TVisited]) -> TVisited:
        return visitor.visit_condition(self)


@dataclass(frozen=True)
class Entity(Expression):
    name: str
    name_validator: re.Pattern[str] = field(
        init=False, repr=False, compare=False, default=re.compile(r"[a-zA-Z_]+")
    )

    def accept(self, visitor: Visitor[TVisited]) -> TVisited:
        return visitor.visit_entity(self)


class InvalidExpression(Exception):
    pass


class InvalidEntity(Exception):
    pass


class Validation(Visitor[None]):
    entity_name_re = re.compile(r"[a-zA-Z_]+")
    column_name_re = re.compile(r"[a-zA-Z_][a-zA-Z0-9_\.]*")

    def visit_column(self, column: Column) -> None:
        if not isinstance(column.name, str):
            raise InvalidExpression(f"column '{column.name}' must be a string")
            column.name = str(column.name)
        if not self.column_name_re.match(column.name):
            raise InvalidExpression(
                f"column '{column.name}' is empty or contains invalid characters"
            )

    def visit_function(self, func: Function) -> None:
        if not isinstance(func.function, str):
            raise InvalidExpression(f"function '{func.function}' must be a string")
        if func.function == "":
            # TODO: Have a whitelist of valid functions to check, maybe even with more
            # specific parameter type checking
            raise InvalidExpression("function cannot be empty")
        if not self.column_name_re.match(func.function):
            raise InvalidExpression(
                f"function '{func.function}' contains invalid characters"
            )

        if func.alias is not None:
            if not isinstance(func.alias, str) or func.alias == "":
                raise InvalidExpression(
                    f"alias '{func.alias}' of function {func.function} must be None or a non-empty string"
                )
            if not self.column_name_re.match(func.alias):
                raise InvalidExpression(
                    f"alias '{func.alias}' of function {func.function} contains invalid characters"
                )

        for param in func.parameters:
            if isinstance(param, (Column, Function, *Scalar)):
                continue
            else:
                assert not isinstance(param, bytes)  # mypy
                raise InvalidExpression(
                    f"parameter '{param}' of function {func.function} is an invalid type"
                )

    def visit_int_literal(
        self, literal: int, minn: Optional[int], maxn: Optional[int], name: str
    ) -> None:
        if not isinstance(literal, int):
            raise InvalidExpression(f"{name} '{literal}' must be an integer")
        if minn is not None and literal < minn:
            raise InvalidExpression(f"{name} '{literal}' must be at least {minn:,}")
        elif maxn is not None and literal > maxn:
            raise InvalidExpression(f"{name} '{literal}' is capped at {maxn:,}")

    def visit_entity(self, entity: Entity) -> None:
        # TODO: There should be a whitelist of entity names at some point
        if not self.entity_name_re.match(entity.name):
            raise InvalidEntity(f"{entity.name} is not a valid entity name")

    def visit_condition(self, cond: Condition) -> None:
        if not isinstance(cond.lhs, (Column, Function)):
            raise InvalidExpression(
                f"invalid condition: LHS of a condition must be a Column or Function, not {type(cond.lhs)}"
            )
        if not isinstance(cond.rhs, (Column, Function, *Scalar)):
            raise InvalidExpression(
                f"invalid condition: RHS of a condition must be a Column, Function or Scalar not {type(cond.rhs)}"
            )
        if not isinstance(cond.op, Op):
            raise InvalidExpression(
                "invalid condition: operator of a condition must be an Op"
            )

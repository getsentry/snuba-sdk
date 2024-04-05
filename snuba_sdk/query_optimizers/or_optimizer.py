from dataclasses import replace
from typing import Union

from snuba_sdk.conditions import BooleanCondition, BooleanOp, Condition, Op
from snuba_sdk.formula import Formula
from snuba_sdk.timeseries import Timeseries


class OrOptimizer:
    def optimize(self, query: Union[Formula, Timeseries]) -> Union[Formula, Timeseries]:
        if query.filters is None:
            return query

        optimized = False
        new_filters = []
        for cond in query.filters:
            res = self._optimize_condition(cond)
            if res is not None:
                optimized = True
                new_filters.append(res)
            else:
                new_filters.append(cond)

        if optimized:
            return replace(query, filters=new_filters)
        return query

    def _optimize_condition(
        self, cond: Union[BooleanCondition, Condition]
    ) -> Union[BooleanCondition, Condition, None]:
        """
        Given a condition, returns the optimized version, or None if it can't be optimized
        """
        if not isinstance(cond, BooleanCondition) or cond.op != BooleanOp.OR:
            return None
        shared_lhs = None
        rhsides = []
        for curr in cond.conditions:
            if (
                not isinstance(curr, Condition)
                or curr.op != Op.EQ
                or (shared_lhs and curr.lhs != shared_lhs)
            ):
                # can't be optimized
                return None
            if not shared_lhs:
                shared_lhs = curr.lhs
            rhsides += [curr.rhs]
        assert shared_lhs
        assert rhsides
        return Condition(shared_lhs, Op.IN, rhsides)  # type: ignore

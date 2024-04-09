from typing import Union

from snuba_sdk.conditions import BooleanCondition, BooleanOp, Condition, Op
from snuba_sdk.conditions import ConditionGroup


class OrOptimizer:
    def optimize(self, condition_group: ConditionGroup) -> ConditionGroup | None:
        """
        Given a condition group, returns a new condition group with optimized or-conditions,
        or None if the conditions can't be optimized.

        Specifically, any conditions that have the form:
        BooleanCondition(OR, [tag=val1,tag=val2, tag=val3, ...])
        become
        Condition(tag, IN, [tag=val1,tag=val2, tag=val3, ...])
        """
        optimized = False
        new_filters = []
        for cond in condition_group:
            res = self._optimize_condition(cond)

            if res is not None:
                optimized = True
                new_filters.append(res)
            else:
                new_filters.append(cond)

        if optimized:
            return new_filters
        return None

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

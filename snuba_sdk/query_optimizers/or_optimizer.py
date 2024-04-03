from snuba_sdk.conditions import (
    BooleanCondition,
    BooleanOp,
    Condition,
    Op,
)
from snuba_sdk.formula import Formula
from snuba_sdk.timeseries import Timeseries


class OrOptimizer:
    def optimize(self, query: Formula | Timeseries) -> Formula | Timeseries:
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

        if not optimized:
            return query
        elif isinstance(query, Timeseries):
            return Timeseries(
                metric=query.metric,
                aggregate=query.aggregate,
                aggregate_params=query.aggregate_params,
                filters=new_filters,
                groupby=query.groupby,
            )
        else:
            return Formula(
                function_name=query.function_name,
                parameters=query.parameters,
                aggregate_params=query.aggregate_params,
                filters=new_filters,
                groupby=query.groupby,
            )

    def _optimize_condition(
        self, cond: BooleanCondition | Condition
    ) -> BooleanCondition | Condition | None:
        """
        Given a condition, returns the optimized version, or None if it can't be optimized
        """
        if cond.op != BooleanOp.OR:
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

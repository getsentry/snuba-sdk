import numbers
import re
from typing import Any, List, Optional

# This is supposed to enumerate the functions snuba supports (with their
# validator) so we can keep control of the functions snuba
# exposes.
#
# At this point it is just listing some of them used during query
# processing, so we can keep the list in one place only.

# Please keep them sorted alphabetically in two groups:
# Standard and Snuba specific.
_AGGREGATION_FUNCTIONS_BASE = {
    # Base
    "count",
    "min",
    "max",
    "sum",
    "avg",
    "any",
    "stddevPop",
    "stddevSamp",
    "varPop",
    "varSamp",
    "covarPop",
    "covarSamp",
    # Snuba Specific
    "anyHeavy",
    "anyLast",
    "argMin",
    "argMax",
    "avgWeighted",
    "topK",
    "topKWeighted",
    "groupArray",
    "groupUniqArray",
    "groupArrayInsertAt",
    "groupArrayMovingAvg",
    "groupArrayMovingSum",
    "groupBitAnd",
    "groupBitOr",
    "groupBitXor",
    "groupBitmap",
    "groupBitmapAnd",
    "groupBitmapOr",
    "groupBitmapXor",
    "sumWithOverflow",
    "sumMap",
    "minMap",
    "maxMap",
    "skewSamp",
    "skewPop",
    "kurtSamp",
    "kurtPop",
    "uniq",
    "uniqExact",
    "uniqCombined",
    "uniqCombined64",
    "uniqHLL12",
    "quantile",
    "quantiles",
    "quantileExact",
    "quantileExactLow",
    "quantileExactHigh",
    "quantileExactWeighted",
    "quantileTiming",
    "quantileTimingWeighted",
    "quantileDeterministic",
    "quantileTDigest",
    "quantileTDigestWeighted",
    "simpleLinearRegression",
    "stochasticLinearRegression",
    "stochasticLogisticRegression",
    "categoricalInformationValue",
    # Parametric
    "histogram",
    "sequenceMatch",
    "sequenceCount",
    "windowFunnel",
    "retention",
    "uniqUpTo",
    "sumMapFiltered",
    # Sentry
    "apdex",
    "failure_rate",
}

_AGGREGATION_SUFFIXES = {
    "",
    "If",
    "Array",
    "SampleState",
    "State",
    "Merge",
    "MergeState",
    "ForEach",
    "OrDefault",
    "OrNull",
    "Resample",
}

AGGREGATION_FUNCTIONS = {
    f"{f_name}{suffix}"
    for f_name in _AGGREGATION_FUNCTIONS_BASE
    for suffix in _AGGREGATION_SUFFIXES
}

FUNCTION_NAME = re.compile(r"([a-zA-Z_]+)\(")


def is_aggregation_function(func_name: str) -> bool:
    # Special case for legacy functions
    if "(" in func_name:
        matches = FUNCTION_NAME.findall(func_name)
        return any(func in AGGREGATION_FUNCTIONS for func in matches)

    return func_name in AGGREGATION_FUNCTIONS


def check_array_type(pot_array: List[Any]) -> bool:
    """
    Check if a list follows the Snuba array typing rules.
    - An array must contain all the same data type, or NULL
    - An array can nest arrays, but those arrays must all hold the same data type
    """

    def find_base(value: Any) -> Optional[str]:
        if value is None:
            return None
        elif isinstance(value, numbers.Number):
            return "num"
        elif not isinstance(value, list):
            return str(type(value))

        to_check = None
        for v in value:
            if v is not None:
                to_check = v
                break

        if to_check is None:
            return None

        return f"list({find_base(to_check)})"

    # Find the first non-null type
    base_type = None
    for elem in pot_array:
        base_type = find_base(elem)
        if base_type is not None:
            break

    if base_type is None:
        return True

    for elem in pot_array:
        elem_type = find_base(elem)
        if elem_type is not None and elem_type != base_type:
            return False
        elif isinstance(elem, list) and not check_array_type(elem):
            return False

    return True

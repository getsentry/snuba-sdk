from __future__ import annotations

import re
from typing import Optional

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


def is_aggregation_function(func_name: str, aliases: Optional[set[str]] = None) -> bool:
    # Special case for legacy functions
    if "(" in func_name:
        matches = FUNCTION_NAME.findall(func_name)
        if any(func in AGGREGATION_FUNCTIONS for func in matches):
            return True

        if aliases is not None and any(alias in func_name for alias in aliases):
            return True

    return func_name in AGGREGATION_FUNCTIONS

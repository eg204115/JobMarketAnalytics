"""
Pandas UDF for parsing free-text salary strings (Jooble) into a structured
(min, max, currency) result. This is deliberately the ONLY UDF in the Silver
layer — every other transformation uses native Spark SQL functions, because
regex-based salary parsing is genuinely inexpressible with built-ins, while
things like string trimming/lowering are not (see bronze_to_silver.py, which
uses F.trim/F.lower/F.regexp_replace natively instead of a UDF).

We use a Pandas UDF (vectorized, operates on a pandas.Series per batch)
rather than a row-at-a-time UDF, because Pandas UDFs avoid per-row Python
serialization overhead — significant at scale, and best practice whenever
Python-side logic can't be avoided.
"""

from __future__ import annotations

import re

import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

SALARY_RESULT_SCHEMA = StructType([
    StructField("parsed_salary_min", DoubleType(), nullable=True),
    StructField("parsed_salary_max", DoubleType(), nullable=True),
    StructField("parsed_currency", StringType(), nullable=True),
])

# Matches patterns like "$90,000 - $120,000", "Rs. 250,000", "£45k - £60k"
_CURRENCY_SYMBOLS = {"$": "USD", "£": "GBP", "€": "EUR", "Rs.": "LKR", "Rs": "LKR"}
_RANGE_PATTERN = re.compile(
    r"(?P<symbol>\$|£|€|Rs\.?)\s?(?P<low>[\d,]+)(?:k)?"
    r"(?:\s*-\s*(?:\$|£|€|Rs\.?)?\s?(?P<high>[\d,]+)(?:k)?)?",
    re.IGNORECASE,
)


def _parse_single_salary_text(text: str | None) -> tuple[float | None, float | None, str | None]:
    """Pure-Python parsing logic, unit-testable independent of Spark."""
    if not text:
        return None, None, None

    match = _RANGE_PATTERN.search(text)
    if not match:
        return None, None, None

    symbol = match.group("symbol")
    currency = _CURRENCY_SYMBOLS.get(symbol, None)

    def to_number(raw: str | None) -> float | None:
        if raw is None:
            return None
        value = float(raw.replace(",", ""))
        # Handle "60k" style shorthand — detected via the (?:k)? group,
        # but re-checked here since the group doesn't capture the 'k' itself.
        if "k" in text.lower() and value < 1000:
            value *= 1000
        return value

    low = to_number(match.group("low"))
    high = to_number(match.group("high")) if match.group("high") else low

    return low, high, currency


@pandas_udf(SALARY_RESULT_SCHEMA)
def parse_salary_text_udf(text_series: pd.Series) -> pd.DataFrame:
    """
    Vectorized entry point Spark calls per-batch. Delegates to the pure-Python
    function above so the parsing logic itself can be unit tested without
    spinning up Spark at all (see test_salary_parser.py).
    """
    parsed = text_series.apply(_parse_single_salary_text)
    return pd.DataFrame(
        parsed.tolist(),
        columns=["parsed_salary_min", "parsed_salary_max", "parsed_currency"],
    )
from __future__ import annotations

import re

from src.tools.sql_executor import execute_sql


def score_root_cause(predicted: str, expected: str, threshold: float = 0.5) -> bool:
    """Score whether the predicted root cause matches the expected one.

    Uses keyword overlap scoring: extract meaningful terms from both strings
    and check if overlap exceeds a threshold.

    Args:
        predicted: The agent's root cause description.
        expected: The expected root cause from the golden set.
        threshold: Minimum overlap ratio to consider a match.

    Returns:
        True if the predicted root cause matches the expected one.
    """
    if not predicted or not expected:
        return False

    pred_terms = _extract_terms(predicted)
    exp_terms = _extract_terms(expected)

    if not exp_terms:
        return False

    overlap = pred_terms & exp_terms
    ratio = len(overlap) / len(exp_terms)

    return ratio >= threshold


def score_fix(
    fixed_sql: str,
    verification_query: str,
    engine: str,
    expected_min_rows: int | None = None,
) -> bool:
    """Score whether the proposed fix actually works.

    Runs the fixed SQL against the test database and verifies with
    a verification query.

    Args:
        fixed_sql: The agent's proposed fixed SQL.
        verification_query: Query to verify the fix worked.
        engine: Database engine ('sqlite' or 'duckdb').
        expected_min_rows: Minimum expected row count (optional).

    Returns:
        True if the fix executes without error and verification passes.
    """
    # Step 1: Execute the fixed SQL
    result = execute_sql(fixed_sql, engine)
    if result.startswith("SQL Error") or result.startswith("Error"):
        return False

    # Step 2: Run verification query
    if not verification_query:
        return True  # No verification query — fix executed without error

    verify_result = execute_sql(verification_query, engine)
    if verify_result.startswith("SQL Error") or verify_result.startswith("Error"):
        return False

    # Step 3: Check row count if expected
    count = _extract_count(verify_result)
    if count is not None:
        if count == 0:
            return False
        if expected_min_rows is not None and count < expected_min_rows:
            return False

    return True


def _extract_terms(text: str) -> set[str]:
    """Extract meaningful keywords from a text string."""
    # Lowercase and split on non-alphanumeric
    words = re.findall(r"[a-z0-9_]+", text.lower())

    # Filter out common stop words and very short words
    stop_words = {
        "the", "a", "an", "is", "are", "was", "were", "be", "been", "being",
        "have", "has", "had", "do", "does", "did", "will", "would", "could",
        "should", "may", "might", "can", "shall", "to", "of", "in", "for",
        "on", "with", "at", "by", "from", "as", "into", "but", "or", "and",
        "not", "no", "it", "its", "that", "this", "which", "what", "who",
        "how", "when", "where", "why", "all", "each", "both", "few", "more",
    }

    return {w for w in words if w not in stop_words and len(w) > 1}


def _extract_count(result: str) -> int | None:
    """Extract a numeric count from a query result string."""
    # Look for a number in the result
    lines = result.strip().split("\n")
    for line in lines:
        # Skip header and separator lines
        if "---" in line or "cnt" in line.lower() or "count" in line.lower():
            continue
        # Find a standalone number
        match = re.search(r"\b(\d+)\b", line)
        if match:
            return int(match.group(1))
    return None

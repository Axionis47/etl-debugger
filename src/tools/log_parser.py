from __future__ import annotations

import re


def parse_logs(log_content: str, pattern: str = "") -> str:
    """Search ETL error logs for relevant error messages.

    Args:
        log_content: The full text of the error log.
        pattern: Optional regex pattern to filter log lines. If empty,
                 returns all ERROR and WARN lines.

    Returns:
        Matching log lines, or summary if too many matches.
    """
    if not log_content.strip():
        return "No log content provided."

    lines = log_content.strip().split("\n")

    if pattern:
        try:
            compiled = re.compile(pattern, re.IGNORECASE)
            matches = [line for line in lines if compiled.search(line)]
        except re.error as e:
            return f"Invalid regex pattern: {e}"
    else:
        # Default: show ERROR and WARN lines
        matches = [
            line for line in lines
            if any(level in line for level in ("ERROR", "WARN", "FATAL"))
        ]

    if not matches:
        return "No matching log lines found."

    if len(matches) > 20:
        shown = matches[:20]
        return "\n".join(shown) + f"\n... ({len(matches) - 20} more lines)"

    return "\n".join(matches)

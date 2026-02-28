from __future__ import annotations

import sqlite3

import duckdb


# Module-level database paths, set by the agent before running
_db_paths: dict[str, str] = {}


def set_db_path(engine: str, path: str) -> None:
    """Register a database path for an engine."""
    _db_paths[engine] = path


def get_db_path(engine: str) -> str | None:
    """Get the registered database path for an engine."""
    return _db_paths.get(engine)


def execute_sql(query: str, engine: str = "duckdb") -> str:
    """Execute a SQL query against a database and return results as a formatted table.

    Args:
        query: The SQL query to execute.
        engine: Database engine - either 'sqlite' or 'duckdb'.

    Returns:
        Query results as a formatted string table, or error message if query fails.
    """
    db_path = _db_paths.get(engine)
    if not db_path:
        return f"Error: No database configured for engine '{engine}'. Available: {list(_db_paths.keys())}"

    try:
        if engine == "duckdb":
            return _execute_duckdb(query, db_path)
        elif engine == "sqlite":
            return _execute_sqlite(query, db_path)
        else:
            return f"Error: Unknown engine '{engine}'. Use 'sqlite' or 'duckdb'."
    except Exception as e:
        return f"SQL Error ({engine}): {e}"


def _execute_duckdb(query: str, db_path: str) -> str:
    """Execute SQL against DuckDB."""
    con = duckdb.connect(db_path)
    try:
        result = con.execute(query)
        if result.description is None:
            rows_affected = con.execute("SELECT changes()").fetchone()
            count = rows_affected[0] if rows_affected else 0
            return f"Query executed successfully. Rows affected: {count}"

        columns = [desc[0] for desc in result.description]
        rows = result.fetchmany(50)

        return _format_table(columns, rows, total_hint=len(rows))
    finally:
        con.close()


def _execute_sqlite(query: str, db_path: str) -> str:
    """Execute SQL against SQLite."""
    con = sqlite3.connect(db_path)
    try:
        cursor = con.execute(query)

        if cursor.description is None:
            con.commit()
            return f"Query executed successfully. Rows affected: {cursor.rowcount}"

        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchmany(50)

        return _format_table(columns, rows, total_hint=len(rows))
    finally:
        con.close()


def _format_table(columns: list[str], rows: list[tuple], total_hint: int = 0) -> str:
    """Format query results as a readable text table."""
    if not rows:
        return f"Columns: {', '.join(columns)}\n(0 rows returned)"

    str_rows = [[str(v) for v in row] for row in rows]
    widths = [max(len(c), *(len(r[i]) for r in str_rows)) for i, c in enumerate(columns)]

    header = " | ".join(c.ljust(w) for c, w in zip(columns, widths))
    separator = "-+-".join("-" * w for w in widths)
    data_lines = [" | ".join(v.ljust(w) for v, w in zip(row, widths)) for row in str_rows]

    parts = [header, separator, *data_lines]
    if total_hint >= 50:
        parts.append(f"... (showing first 50 rows)")

    return "\n".join(parts)

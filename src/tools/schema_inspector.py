from __future__ import annotations

import sqlite3

import duckdb

from src.tools.sql_executor import get_db_path


def inspect_schema(table_name: str, engine: str = "duckdb") -> str:
    """Inspect the schema of a database table. Returns column names, data types, and row count.

    Args:
        table_name: Name of the table to inspect.
        engine: Database engine - either 'sqlite' or 'duckdb'.

    Returns:
        Table schema as formatted text with columns, types, and row count.
    """
    db_path = get_db_path(engine)
    if not db_path:
        return f"Error: No database configured for engine '{engine}'."

    try:
        if engine == "duckdb":
            return _inspect_duckdb(table_name, db_path)
        elif engine == "sqlite":
            return _inspect_sqlite(table_name, db_path)
        else:
            return f"Error: Unknown engine '{engine}'."
    except Exception as e:
        return f"Schema Error ({engine}): {e}"


def _inspect_duckdb(table_name: str, db_path: str) -> str:
    """Inspect schema in DuckDB."""
    con = duckdb.connect(db_path)
    try:
        cols = con.execute(f"DESCRIBE {table_name}").fetchall()
        count_result = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        row_count = count_result[0] if count_result else 0

        lines = [f"Table: {table_name}", f"Row count: {row_count}", ""]
        lines.append(f"{'Column':<20} {'Type':<15} {'Nullable':<10}")
        lines.append(f"{'-'*20} {'-'*15} {'-'*10}")
        for col in cols:
            name, dtype, nullable = col[0], col[1], col[2]
            null_str = "YES" if nullable == "YES" else "NO"
            lines.append(f"{name:<20} {dtype:<15} {null_str:<10}")

        return "\n".join(lines)
    finally:
        con.close()


def _inspect_sqlite(table_name: str, db_path: str) -> str:
    """Inspect schema in SQLite."""
    con = sqlite3.connect(db_path)
    try:
        cursor = con.execute(f"PRAGMA table_info({table_name})")
        cols = cursor.fetchall()

        if not cols:
            tables = con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
            table_list = ", ".join(t[0] for t in tables)
            return f"Table '{table_name}' not found. Available tables: {table_list}"

        count_result = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        row_count = count_result[0] if count_result else 0

        lines = [f"Table: {table_name}", f"Row count: {row_count}", ""]
        lines.append(f"{'Column':<20} {'Type':<15} {'Nullable':<10}")
        lines.append(f"{'-'*20} {'-'*15} {'-'*10}")
        for col in cols:
            name, dtype, notnull = col[1], col[2], col[3]
            null_str = "NO" if notnull else "YES"
            lines.append(f"{name:<20} {dtype:<15} {null_str:<10}")

        return "\n".join(lines)
    finally:
        con.close()


def compare_schemas(source_table: str, dest_table: str, engine: str = "duckdb") -> str:
    """Compare schemas of two tables side by side. Shows column name matches, type mismatches, and sample values for mismatched columns.

    Args:
        source_table: Name of the source table.
        dest_table: Name of the destination table.
        engine: Database engine - either 'sqlite' or 'duckdb'.

    Returns:
        Side-by-side schema comparison with type match status and sample values for mismatches.
    """
    db_path = get_db_path(engine)
    if not db_path:
        return f"Error: No database configured for engine '{engine}'."

    try:
        if engine == "duckdb":
            return _compare_duckdb(source_table, dest_table, db_path)
        elif engine == "sqlite":
            return _compare_sqlite(source_table, dest_table, db_path)
        else:
            return f"Error: Unknown engine '{engine}'."
    except Exception as e:
        return f"Compare Error ({engine}): {e}"


def _compare_duckdb(source_table: str, dest_table: str, db_path: str) -> str:
    """Compare schemas in DuckDB."""
    con = duckdb.connect(db_path)
    try:
        src_cols = {row[0]: row[1] for row in con.execute(f"DESCRIBE {source_table}").fetchall()}
        dst_cols = {row[0]: row[1] for row in con.execute(f"DESCRIBE {dest_table}").fetchall()}

        lines = [f"Schema Comparison: {source_table} vs {dest_table}", ""]
        lines.append(f"{'Source Column':<25} {'Dest Column':<25} {'Status'}")
        lines.append(f"{'-'*25} {'-'*25} {'-'*20}")

        mismatched_cols: list[tuple[str, str, str]] = []

        for dcol, dtype in dst_cols.items():
            if dcol in src_cols:
                stype = src_cols[dcol]
                if stype.upper() == dtype.upper():
                    lines.append(f"{dcol + ' (' + stype + ')':<25} {dcol + ' (' + dtype + ')':<25} OK")
                else:
                    lines.append(f"{dcol + ' (' + stype + ')':<25} {dcol + ' (' + dtype + ')':<25} TYPE MISMATCH")
                    mismatched_cols.append((dcol, stype, dtype))
            else:
                lines.append(f"{'(missing)':<25} {dcol + ' (' + dtype + ')':<25} MISSING IN SOURCE")

        for scol, stype in src_cols.items():
            if scol not in dst_cols:
                lines.append(f"{scol + ' (' + stype + ')':<25} {'(not in dest)':<25} EXTRA IN SOURCE")

        if mismatched_cols:
            lines.append("")
            lines.append("Sample values for mismatched columns:")
            for col, stype, dtype in mismatched_cols:
                try:
                    samples = con.execute(f"SELECT DISTINCT {col} FROM {source_table} LIMIT 10").fetchall()
                    vals = [str(row[0]) for row in samples]
                    lines.append(f"  {col}: {', '.join(vals)}")
                except Exception:
                    pass

        return "\n".join(lines)
    finally:
        con.close()


def _compare_sqlite(source_table: str, dest_table: str, db_path: str) -> str:
    """Compare schemas in SQLite."""
    con = sqlite3.connect(db_path)
    try:
        src_info = con.execute(f"PRAGMA table_info({source_table})").fetchall()
        dst_info = con.execute(f"PRAGMA table_info({dest_table})").fetchall()

        src_cols = {row[1]: row[2] for row in src_info}
        dst_cols = {row[1]: row[2] for row in dst_info}

        lines = [f"Schema Comparison: {source_table} vs {dest_table}", ""]
        lines.append(f"{'Source Column':<25} {'Dest Column':<25} {'Status'}")
        lines.append(f"{'-'*25} {'-'*25} {'-'*20}")

        for dcol, dtype in dst_cols.items():
            if dcol in src_cols:
                stype = src_cols[dcol]
                if stype.upper() == dtype.upper():
                    lines.append(f"{dcol + ' (' + stype + ')':<25} {dcol + ' (' + dtype + ')':<25} OK")
                else:
                    lines.append(f"{dcol + ' (' + stype + ')':<25} {dcol + ' (' + dtype + ')':<25} TYPE MISMATCH")
            else:
                lines.append(f"{'(missing)':<25} {dcol + ' (' + dtype + ')':<25} MISSING IN SOURCE")

        for scol, stype in src_cols.items():
            if scol not in dst_cols:
                lines.append(f"{scol + ' (' + stype + ')':<25} {'(not in dest)':<25} EXTRA IN SOURCE")

        return "\n".join(lines)
    finally:
        con.close()


def sample_values(table_name: str, column_name: str, engine: str = "duckdb") -> str:
    """Get distinct sample values from a specific column. Useful for inspecting join keys or data quality issues.

    Args:
        table_name: Name of the table.
        column_name: Name of the column to sample.
        engine: Database engine - either 'sqlite' or 'duckdb'.

    Returns:
        Up to 10 distinct sample values from the column.
    """
    db_path = get_db_path(engine)
    if not db_path:
        return f"Error: No database configured for engine '{engine}'."

    try:
        if engine == "duckdb":
            con = duckdb.connect(db_path)
        else:
            con = sqlite3.connect(db_path)

        try:
            rows = con.execute(
                f"SELECT DISTINCT {column_name} FROM {table_name} LIMIT 10"
            ).fetchall()

            if not rows:
                return f"Column '{column_name}' in table '{table_name}': (no data)"

            values = [str(row[0]) for row in rows]
            dtype_info = ""
            try:
                dtype_row = con.execute(
                    f"SELECT typeof({column_name}) FROM {table_name} LIMIT 1"
                ).fetchone()
                if dtype_row:
                    dtype_info = f" (type: {dtype_row[0]})"
            except Exception:
                pass

            return f"Column '{column_name}' in '{table_name}'{dtype_info}:\nDistinct values: {', '.join(values)}"
        finally:
            con.close()
    except Exception as e:
        return f"Sample Error ({engine}): {e}"

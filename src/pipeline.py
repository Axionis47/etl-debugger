from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

import duckdb
import yaml

from src.models import PipelineConfig


def load_pipeline(path: str | Path) -> PipelineConfig:
    """Load a pipeline configuration from a YAML file."""
    path = Path(path)
    with open(path) as f:
        data = yaml.safe_load(f)
    return PipelineConfig(**data)


def load_error_log(pipeline_path: str | Path) -> str:
    """Load the error log from the same directory as the pipeline config."""
    log_path = Path(pipeline_path).parent / "error.log"
    if not log_path.exists():
        return ""
    return log_path.read_text()


def setup_test_db(pipeline: PipelineConfig, case_dir: str | Path) -> str:
    """Set up a temporary database with test data from CSV files.

    Returns the path to the database file.
    """
    case_dir = Path(case_dir)
    engine = pipeline.destination.engine
    db_path = str(case_dir / f"test.{engine}")

    if engine == "duckdb":
        _setup_duckdb(pipeline, case_dir, db_path)
    else:
        _setup_sqlite(pipeline, case_dir, db_path)

    return db_path


def _setup_duckdb(pipeline: PipelineConfig, case_dir: Path, db_path: str) -> None:
    """Set up a DuckDB database with test data."""
    con = duckdb.connect(db_path)

    # Load CSV files as tables
    for csv_file in case_dir.glob("*.csv"):
        table_name = csv_file.stem
        con.execute(
            f"CREATE OR REPLACE TABLE {table_name} AS "
            f"SELECT * FROM read_csv_auto('{csv_file}')"
        )

    # Create the destination table if schema SQL exists
    schema_file = case_dir / "schema.sql"
    if schema_file.exists():
        for stmt in schema_file.read_text().split(";"):
            stmt = stmt.strip()
            if stmt:
                con.execute(stmt)
    else:
        # Create destination table based on pipeline config
        dest_table = pipeline.destination.table
        _create_dest_table_duckdb(con, dest_table, case_dir)

    con.close()


def _setup_sqlite(pipeline: PipelineConfig, case_dir: Path, db_path: str) -> None:
    """Set up a SQLite database with test data."""
    con = sqlite3.connect(db_path)
    cursor = con.cursor()

    # Load CSV files as tables
    for csv_file in case_dir.glob("*.csv"):
        table_name = csv_file.stem
        with open(csv_file) as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None:
                continue
            cols = reader.fieldnames
            col_defs = ", ".join(f"{c} TEXT" for c in cols)
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({col_defs})")
            placeholders = ", ".join("?" for _ in cols)
            for row in reader:
                values = [row[c] for c in cols]
                cursor.execute(f"INSERT INTO {table_name} VALUES ({placeholders})", values)

    # Create destination table
    schema_file = case_dir / "schema.sql"
    if schema_file.exists():
        for stmt in schema_file.read_text().split(";"):
            stmt = stmt.strip()
            if stmt:
                cursor.execute(stmt)
    else:
        dest_table = pipeline.destination.table
        _create_dest_table_sqlite(cursor, dest_table, case_dir)

    con.commit()
    con.close()


def _create_dest_table_duckdb(con: duckdb.DuckDBPyConnection, table_name: str, case_dir: Path) -> None:
    """Create a destination table in DuckDB from schema.sql or infer from CSV."""
    schema_file = case_dir / "dest_schema.sql"
    if schema_file.exists():
        con.execute(schema_file.read_text())


def _create_dest_table_sqlite(cursor: sqlite3.Cursor, table_name: str, case_dir: Path) -> None:
    """Create a destination table in SQLite from schema.sql."""
    schema_file = case_dir / "dest_schema.sql"
    if schema_file.exists():
        cursor.execute(schema_file.read_text())
